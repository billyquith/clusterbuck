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
    m = json.loads((CONTRACT / "examples" / "update-manifest.valid.json").read_text())
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
