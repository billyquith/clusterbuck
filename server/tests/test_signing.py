"""Release-manifest signing (M4c): round-trip, the committed cross-language fixture, and
the /updates/manifest endpoint. Cross-language verification (C# consuming these DER
signatures) is proven by the worker's UpdateVerifierTests against the same fixture."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from cryptography.hazmat.primitives.serialization import load_pem_public_key
from fastapi.testclient import TestClient

from clusterbuck import signing
from clusterbuck.api import create_app

CONTRACT = Path(__file__).resolve().parents[2] / "contract"


def test_sign_verify_roundtrip():
    priv = signing.generate_keypair()
    m = signing.build_manifest(priv, version="1.0.0", rid="linux-x64",
                               url="https://x/y", sha256="a" * 64, channel="stable",
                               protocol_version=1)
    pub = priv.public_key()
    assert signing.verify_manifest(pub, m)
    assert not signing.verify_manifest(pub, dict(m, sha256="0" * 64))     # tampered artifact
    assert not signing.verify_manifest(pub, dict(m, version="9.9.9"))     # tampered version
    # url and protocol_version are inside the signed payload: without that, an on-path
    # attacker could redirect the fetch host, or stall every worker via skew gating.
    assert not signing.verify_manifest(pub, dict(m, url="https://evil.invalid/cbk"))
    assert not signing.verify_manifest(pub, dict(m, protocol_version=99))
    assert not signing.verify_manifest(pub, dict(m, channel="canary"))


def test_committed_fixture_verifies():
    m = json.loads((CONTRACT / "examples" / "update-manifest.valid.json").read_text(encoding="utf-8"))
    pub = load_pem_public_key((CONTRACT / "examples" / "update-signing.pub.pem").read_bytes())
    assert signing.verify_manifest(pub, m)


def test_update_endpoint_signs(tmp_path, redis_url):
    priv = signing.generate_keypair()
    keyp = tmp_path / "sign.key.pem"
    keyp.write_text(signing.private_pem(priv))
    sha = hashlib.sha256(b"artifact").hexdigest()
    release = tmp_path / "release.json"
    release.write_text(json.dumps({
        "version": "2.0.0", "channel": "stable", "protocol_version": 1,
        "artifacts": {"osx-arm64": {"url": "https://x/cbk", "sha256": sha}},
    }))

    app = create_app(redis_url=redis_url, db_path=str(tmp_path / "t.db"),
                     start_scheduler=False, update_signing_key=str(keyp),
                     update_release=str(release))
    with TestClient(app) as c:
        m = c.get("/updates/manifest", params={"rid": "osx-arm64"})
        assert m.status_code == 200, m.text
        manifest = m.json()
        assert manifest["version"] == "2.0.0" and manifest["sha256"] == sha
        assert signing.verify_manifest(priv.public_key(), manifest)  # server signed it validly

        assert c.get("/updates/manifest", params={"rid": "win-x64"}).status_code == 404


def test_update_endpoint_404_when_unconfigured(client):
    # The conftest client configures no signing key ⇒ no update channel.
    assert client.get("/updates/manifest", params={"rid": "osx-arm64"}).status_code == 404


def test_artifact_key_is_chosen_by_flavour_and_fails_closed():
    """Flavour, not platform, names the artifact — and an unknown flavour names nothing."""
    from clusterbuck.api import PY_ARTIFACT, artifact_key_for

    # One Python artifact covers every platform — that is what collapses the build matrix.
    assert artifact_key_for("python", "darwin", "arm64") == PY_ARTIFACT
    assert artifact_key_for("python", "windows", "x64") == PY_ARTIFACT
    assert artifact_key_for("python", "linux", "arm64") == PY_ARTIFACT
    # Absent flavour ⇒ python, the only implementation there is. This used to read as
    # a retired flavour and hand such a node a platform-specific id — see the test below
    # for why that mattered; a worker old enough to omit the field is a Python one.
    assert artifact_key_for(None, "linux", "x64") == PY_ARTIFACT
    # Fails closed: no artifact named for a flavour we do not know, so no update is offered.
    assert artifact_key_for("rust", "linux", "x64") is None
    assert artifact_key_for("pyhton", "darwin", "arm64") is None   # typo, not a coin flip


def _flavour_heartbeat(tmp_path, redis_url, monkeypatch, flavour, artifacts):
    """Enrol an auto-updating darwin/arm64 node, beat once, return the offered manifest.

    Declares a current version so the node is judged `stale`: without a version policy it is
    assessed `ok`, the coordinator never consults the release at all, and a flavour test
    would pass whether or not the artifact selection is correct.
    """
    from dataclasses import replace

    from clusterbuck import api as api_mod
    monkeypatch.setattr(api_mod, "settings",
                        replace(api_mod.settings, worker_current_version="2.0.0"))

    priv = signing.generate_keypair()
    keyp = tmp_path / "sign.key.pem"
    keyp.write_text(signing.private_pem(priv))
    release = tmp_path / "release.json"
    release.write_text(json.dumps({
        "version": "2.0.0", "channel": "stable", "protocol_version": 1,
        "artifacts": artifacts,
    }))
    app = create_app(redis_url=redis_url, db_path=str(tmp_path / f"{flavour}.db"),
                     start_scheduler=False, update_signing_key=str(keyp),
                     update_release=str(release))
    with TestClient(app) as c:
        token = c.post("/nodes/tokens").json()["join_token"]
        enr = c.post("/nodes/enroll", json={
            "join_token": token, "hostname": "node-x", "os": "darwin", "arch": "arm64",
            "hw": {"ram_gb": 64, "accelerator": "metal", "disk_free_gb": 512},
            "profile": "shared",
        }).json()
        # Opt in to self-update; without it the coordinator never offers one (ADR 13).
        app.state.store.set_node_flags(enr["node_id"], auto_update=True)
        hb = c.post(f"/nodes/{enr['node_id']}/heartbeat",
                    json={"mode": "away", "agent_version": "0.1.0",
                          "agent_flavour": flavour, "protocol_version": 1},
                    headers={"x-cbk-node-key": enr["node_key"]})
        assert hb.status_code == 200, hb.text
        return hb.json()


def test_a_worker_is_never_offered_an_artifact_it_cannot_execute(tmp_path, redis_url, monkeypatch):
    """The hazard this field exists to close.

    A Python worker on darwin/arm64 used to be handed the osx-arm64 single-file binary,
    because the artifact was keyed off os+arch alone. It would verify a perfectly valid
    signature, download ~73 MB, and install a managed executable over its own entrypoint.
    A release with no Python artifact must offer that worker nothing at all.
    """
    sha = hashlib.sha256(b"not-a-zipapp").hexdigest()
    # A release that names only a platform-specific key no current flavour maps to.
    unrunnable_only = {"osx-arm64": {"url": "https://x/cbk", "sha256": sha}}

    body = _flavour_heartbeat(tmp_path, redis_url, monkeypatch, "python", unrunnable_only)
    assert body["update"] is None, "a Python worker was offered an artifact it cannot run"
    assert body["fitness"]["status"] == "stale"   # still told to update, just not handed one

    # The same release does offer it to the worker it was actually built for.
    # No flavour maps to a platform-specific key any more, so such a release is offered to
    # nobody — including a node reporting an unrecognised flavour. Fail-closed, by design.
    assert _flavour_heartbeat(
        tmp_path, redis_url, monkeypatch, "rust", unrunnable_only)["update"] is None


def test_python_worker_gets_the_platform_independent_artifact(tmp_path, redis_url, monkeypatch):
    sha = hashlib.sha256(b"cbk.pyz").hexdigest()
    body = _flavour_heartbeat(tmp_path, redis_url, monkeypatch, "python", {
        "py3-none-any": {"url": "https://x/cbk.pyz", "sha256": sha},
    })
    assert body["update"] is not None
    assert body["update"]["rid"] == "py3-none-any" and body["update"]["sha256"] == sha
