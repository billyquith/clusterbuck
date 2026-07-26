"""Worker version governance (ADR 27): is this build fit to run jobs?

The point these tests protect: protocol compatibility is NOT sufficient. A worker can speak
the queue contract correctly and still carry bugs that produce plausible-looking wrong
results, so the coordinator judges the reported build version too — including a block-list,
because bugs are not monotonic.
"""

from __future__ import annotations

from clusterbuck.versions import (
    PROTOCOL_VERSION,
    VersionPolicy,
    assess,
    parse_version,
)

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
