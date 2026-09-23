"""Worker version governance (ADR 27): is this build fit to run jobs?

The point these tests protect: protocol compatibility is NOT sufficient. A worker can speak
the queue contract correctly and still carry bugs that produce plausible-looking wrong
results, so the coordinator judges the reported build version too — including a block-list,
because bugs are not monotonic.
"""

from __future__ import annotations

import json as _json
from pathlib import Path as _Path

import pytest as _pytest
from clusterbuck.api import create_app as _create_app
from clusterbuck.versions import (
    PROTOCOL_VERSION,
    VersionPolicy,
    assess,
    parse_version,
)
from fastapi.testclient import TestClient

OK = "ok"
STALE = "stale"
QUAR = "quarantine"


def _assess(policy, version="1.0.0", protocol=PROTOCOL_VERSION):
    return assess(policy, agent_version=version, protocol_version=protocol)


# --- version parsing ---

def test_parse_version_forms():
    assert parse_version("1.4.2") == (1, 4, 2)
    assert parse_version("0.7") == (0, 7)
    assert parse_version("1.2.3-rc1") == (1, 2, 3)        # pre-release stripped
    assert parse_version("1.2.3+abc123") == (1, 2, 3)      # build metadata stripped
    assert parse_version("") is None
    assert parse_version(None) is None
    assert parse_version("not-a-version") is None


# --- no policy: a fleet works before the operator has opinions ---

def test_unconfigured_policy_accepts_everything():
    assert _assess(VersionPolicy(), version="0.0.1").status == OK
    assert _assess(VersionPolicy(), version=None).status == OK


# --- current: behind is usable but flagged ---

def test_behind_current_is_stale_not_quarantined():
    p = VersionPolicy(current="1.2.0")
    f = _assess(p, version="1.1.9")
    assert f.status == STALE
    assert "behind the current release 1.2.0" in f.reason
    assert f.current_version == "1.2.0"          # the worker is told what to move to
    assert _assess(p, version="1.2.0").status == OK
    assert _assess(p, version="1.3.0").status == OK   # ahead is fine (a canary node)


def test_padded_comparison():
    # 1.4 must sort below 1.4.1, not be treated as incomparable.
    p = VersionPolicy(current="1.4.1")
    assert _assess(p, version="1.4").status == STALE
    assert _assess(p, version="1.4.1").status == OK


# --- minimum: below the floor is unfit ---

def test_below_minimum_is_quarantined():
    p = VersionPolicy(current="2.0.0", minimum="1.5.0")
    f = _assess(p, version="1.4.9")
    assert f.status == QUAR
    assert "below the supported floor 1.5.0" in f.reason
    assert _assess(p, version="1.5.0").status == STALE   # meets floor, behind current


# --- the block-list: bugs are NOT monotonic ---

def test_specific_bad_release_is_blocked_while_neighbours_pass():
    """The case a floor cannot express: 1.4.2 is broken, 1.4.1 and 1.4.3 are fine."""
    p = VersionPolicy(current="1.4.3", minimum="1.0.0", blocked=frozenset({"1.4.2"}))
    assert _assess(p, version="1.4.2").status == QUAR
    assert "explicitly blocked as known-bad" in _assess(p, version="1.4.2").reason
    assert _assess(p, version="1.4.1").status == STALE   # older, but not broken
    assert _assess(p, version="1.4.3").status == OK


def test_block_list_beats_being_current():
    # A release can be both the newest AND known-bad — blocking must win.
    p = VersionPolicy(current="1.4.2", blocked=frozenset({"1.4.2"}))
    assert _assess(p, version="1.4.2").status == QUAR


# --- protocol skew outranks build version ---

def test_protocol_skew_quarantines_regardless_of_build():
    # Even a brand-new build is unfit if it speaks an older queue contract.
    p = VersionPolicy(current="1.0.0")
    f = _assess(p, version="9.9.9", protocol=PROTOCOL_VERSION - 1)
    assert f.status == QUAR
    assert "queue-contract protocol" in f.reason


def test_newer_protocol_is_not_penalised():
    # A worker ahead of the coordinator is the coordinator's problem to catch up to, not a
    # reason to stop the worker.
    assert _assess(VersionPolicy(), protocol=PROTOCOL_VERSION + 1).status == OK


# --- unverifiable versions ---

def test_unknown_version_is_quarantined_only_when_a_floor_exists():
    # No floor declared: flag it, keep serving.
    assert _assess(VersionPolicy(current="1.0.0"), version=None).status == STALE
    # Floor declared: it cannot be SHOWN to meet the floor, so refuse rather than assume.
    f = _assess(VersionPolicy(current="1.0.0", minimum="1.0.0"), version=None)
    assert f.status == QUAR
    assert "cannot be shown to meet" in f.reason


def test_garbage_version_is_handled_not_crashed():
    f = _assess(VersionPolicy(current="1.0.0"), version="banana")
    assert f.status == STALE and "banana" in f.reason


# --- the wire shape the worker consumes ---

def test_to_wire_shape():
    w = _assess(VersionPolicy(current="1.0.0"), version="0.9.0").to_wire()
    assert set(w) == {"status", "reason", "current_version"}
    assert w["status"] == "stale" and w["current_version"] == "1.0.0"


# --- through the real heartbeat endpoint ---

def _enrolled(client, ram_gb=64.0):
    token = client.post("/nodes/tokens").json()["join_token"]
    return client.post("/nodes/enroll", json={
        "join_token": token, "hostname": "h", "os": "linux", "arch": "arm64",
        "hw": {"ram_gb": ram_gb, "accelerator": "cpu", "disk_free_gb": 100},
        "profile": "shared"}).json()


def test_heartbeat_returns_fitness_and_persists_the_version(client):
    node = _enrolled(client)
    hdr = {"x-cbk-node-key": node["node_key"]}
    r = client.post(f"/nodes/{node['node_id']}/heartbeat",
                    json={"mode": "active", "agent_version": "0.7.0",
                          "protocol_version": PROTOCOL_VERSION}, headers=hdr)
    assert r.status_code == 200
    # With no policy configured the verdict is ok — but it is still REPORTED, so the worker
    # always knows where it stands.
    assert r.json()["fitness"]["status"] == "ok"

    # And the version is now visible to the operator, which it previously never was.
    me = next(n for n in client.get("/nodes").json()["nodes"]
              if n["node_id"] == node["node_id"])
    assert me["agent_version"] == "0.7.0"
    assert me["protocol_version"] == PROTOCOL_VERSION
    assert me["fitness"] == "ok"


def test_quarantined_worker_is_told_to_stop_and_gets_no_install(client, monkeypatch):
    """A blocked build must be quarantined AND denied model-management actions."""
    from dataclasses import replace

    from clusterbuck import api as api_mod

    # Settings is a frozen dataclass, so swap the module's reference to a modified copy.
    # Block exactly the version this node will report.
    monkeypatch.setattr(api_mod, "settings",
                        replace(api_mod.settings, worker_blocked_versions="0.6.6"))
    node = _enrolled(client)
    hdr = {"x-cbk-node-key": node["node_key"]}

    r = client.post(f"/nodes/{node['node_id']}/heartbeat",
                    json={"mode": "away", "installed": ["llama3.2:3b"],
                          "agent_version": "0.6.6",
                          "protocol_version": PROTOCOL_VERSION}, headers=hdr)
    body = r.json()
    assert body["fitness"]["status"] == "quarantine"
    assert "known-bad" in body["fitness"]["reason"]
    # An unfit build is never handed an install, even if a proposal was approved for it.
    assert body["action"] is None

    me = next(n for n in client.get("/nodes").json()["nodes"]
              if n["node_id"] == node["node_id"])
    assert me["fitness"] == "quarantine"


# --- serving the signed artifact (ADR 38) ----------------------------------------------


@_pytest.fixture()
def release_client(tmp_path, redis_url):
    """A coordinator with a release channel configured and one real artifact on disk."""
    rel = tmp_path / "releases"
    rel.mkdir()
    (rel / "cbk-0.9.0.pyz").write_bytes(b"PK\x03\x04 pretend zipapp")
    (rel / "secret.txt").write_bytes(b"not part of any release")
    (rel / "release.json").write_text(_json.dumps({
        "version": "0.9.0", "channel": "stable", "protocol_version": 1,
        "artifacts": {"py3-none-any": {
            "url": "http://coordinator:8018/releases/cbk-0.9.0.pyz", "sha256": "0" * 64}},
    }))
    app = _create_app(redis_url=redis_url, db_path=str(tmp_path / "r.db"),
                      start_scheduler=False, update_release=str(rel / "release.json"))
    with TestClient(app) as c:
        yield c


def test_a_released_artifact_is_served_without_a_credential(release_client):
    """The worker fetching this has a node key and must never be given the operator key.
    The signature and digest are the security boundary — an attacker who can serve this
    file still cannot make a worker install it."""
    r = release_client.get("/releases/cbk-0.9.0.pyz")
    assert r.status_code == 200
    assert r.content.startswith(b"PK")


def test_a_file_beside_the_release_is_not_served(release_client):
    """Only what the manifest names. The release directory is not a web root."""
    assert release_client.get("/releases/secret.txt").status_code == 404


def test_the_manifest_itself_is_not_served(release_client):
    assert release_client.get("/releases/release.json").status_code == 404


@_pytest.mark.parametrize("attempt", [
    "..%2f..%2fetc%2fpasswd", "....//etc/passwd", "%2e%2e%2fsecret.txt",
])
def test_traversal_cannot_escape_the_release_directory(release_client, attempt):
    """`filename` is matched against an allowlist and never joined as a caller-controlled
    path, so there is nothing for `..` to traverse."""
    assert release_client.get(f"/releases/{attempt}").status_code == 404


def test_no_release_channel_means_no_route(client):
    assert client.get("/releases/anything.pyz").status_code == 404


def test_a_manifest_entry_missing_on_disk_says_so(release_client, tmp_path):
    rel = _Path(release_client.app.state.update_release).parent
    (rel / "cbk-0.9.0.pyz").unlink()
    r = release_client.get("/releases/cbk-0.9.0.pyz")
    assert r.status_code == 404 and "missing on disk" in r.json()["detail"]


# --- the coordinator must not OFFER a downgrade ------------------------------------


def test_version_gt_compares_numerically_not_lexically():
    """The trap the string form walks into: "0.9.0" sorts ABOVE "0.10.0" as text, so a
    lexical gate would treat every release after .9 in a 0.x line as a downgrade."""
    from clusterbuck.versions import parse_version as pv
    from clusterbuck.versions import version_gt

    assert version_gt(pv("0.10.0"), pv("0.9.0"))
    assert not version_gt(pv("0.9.0"), pv("0.10.0"))
    assert not version_gt(pv("0.19.0"), pv("0.19.0")), "equal is not newer"
    assert version_gt(pv("1.0.1"), pv("1.0")), "padded: 1.0 == 1.0.0 < 1.0.1"


def test_a_channel_left_behind_does_not_offer_a_node_a_downgrade(
    tmp_path, redis_url, monkeypatch
):
    """`_build_update_for` checked EQUALITY, so a channel naming an older build offered
    every node a downgrade — and the worker, also checking equality, installed it.

    This is the accidental half of the hazard rather than the malicious one: a release
    touches five settings and nothing reconciles them, so a channel left behind is an
    ordinary mistake. This fleet's did exactly that, offering 0.10.0 to a node on 0.18.0
    with auto-update on. The worker refuses a non-newer manifest too — that is the real
    control, since it also covers a manifest replayed without passing through here — but
    the coordinator should not be the thing that gets it wrong.
    """
    from clusterbuck.api import _build_update_for
    from clusterbuck.signing import generate_keypair, private_pem

    key_file = tmp_path / "signing.pem"
    key_file.write_text(private_pem(generate_keypair()))
    release = tmp_path / "release.json"

    class _App:
        class state:
            update_signing_key = str(key_file)
            update_release = str(release)

    class _Node:
        node_id, os, arch = "node-x", "darwin", "arm64"
        agent_version = "0.18.0"

    def _publish(version: str) -> None:
        release.write_text(_json.dumps({
            "version": version, "channel": "stable", "protocol_version": 1,
            "artifacts": {"py3-none-any": {
                "url": f"http://c:8018/releases/cbk-{version}.pyz", "sha256": "a" * 64}},
        }))

    _publish("0.10.0")
    assert _build_update_for(_App, _Node(), "python") is None, \
        "a channel behind the node must offer nothing"

    _publish("0.18.0")
    assert _build_update_for(_App, _Node(), "python") is None, "equal offers nothing"

    _publish("0.19.0")
    offered = _build_update_for(_App, _Node(), "python")
    assert offered is not None and offered["version"] == "0.19.0", "newer is still offered"
