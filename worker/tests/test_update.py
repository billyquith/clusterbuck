"""Signed self-update (ADR 13): signature interop, and the applier's order of operations.

The order is the security boundary, so each step is asserted independently rather than only
through a happy path.
"""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

import httpx
import pytest

from cbk_worker import update as upd
from cbk_worker.config import AGENT_VERSION, PROTOCOL_VERSION
from cbk_worker.models import UpdateManifest

CONTRACT = Path(__file__).resolve().parents[2] / "contract"
EXAMPLES = CONTRACT / "examples"


# --- verification -----------------------------------------------------------------------


def test_committed_fixture_verifies():
    """The fixture the SERVER signed, verified here (DER / SEC1 / RFC 3279 format)."""
    m = UpdateManifest.from_wire(
        json.loads((EXAMPLES / "update-manifest.valid.json").read_text(encoding="utf-8")))
    pem = (EXAMPLES / "update-signing.pub.pem").read_text(encoding="utf-8")
    assert upd.verify(m, pem)


def test_signing_payload_covers_url_and_protocol_version():
    """Signing only version/rid/sha256/channel would let an attacker redirect the fetch host
    or stall the whole fleet with an inflated protocol_version."""
    m = UpdateManifest(version="1.0.0", rid="linux-x64", url="https://x/cbk.pyz",
                       sha256="a" * 64, channel="stable", protocol_version=1)
    payload = upd.signing_payload(m).decode()
    assert payload.split("\n") == ["1.0.0", "linux-x64", "a" * 64, "stable",
                                   "https://x/cbk.pyz", "1"]


@pytest.mark.parametrize("field,value", [
    ("version", "9.9.9"),
    ("rid", "win-x64"),
    ("sha256", "b" * 64),
    ("channel", "canary"),
    ("url", "https://evil.invalid/cbk.pyz"),
    ("protocol_version", 99),
])
def test_tampering_with_any_signed_field_fails_verification(field, value):
    from dataclasses import replace

    m = UpdateManifest.from_wire(
        json.loads((EXAMPLES / "update-manifest.valid.json").read_text(encoding="utf-8")))
    pem = (EXAMPLES / "update-signing.pub.pem").read_text(encoding="utf-8")
    assert upd.verify(m, pem)                             # baseline
    assert not upd.verify(replace(m, **{field: value}), pem)


def test_malformed_signature_is_rejected_not_raised():
    m = UpdateManifest(version="1.0.0", rid="x", url="u", sha256="a" * 64,
                       channel="stable", signature="not base64 !!", protocol_version=1)
    pub = (EXAMPLES / "update-signing.pub.pem").read_text(encoding="utf-8")
    assert upd.verify(m, pub) is False


def test_skew_gate():
    assert not upd.should_pause_for_skew(UpdateManifest(protocol_version=PROTOCOL_VERSION))
    assert upd.should_pause_for_skew(UpdateManifest(protocol_version=PROTOCOL_VERSION + 1))
    assert not upd.should_pause_for_skew(UpdateManifest(protocol_version=None))


# --- applying ---------------------------------------------------------------------------


def _signed(tmp_path, body: bytes, version="9.9.9", url="https://x/cbk.pyz"):
    """A manifest genuinely signed by a throwaway key, plus that key's public PEM."""
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec

    key = ec.generate_private_key(ec.SECP256R1())
    m = UpdateManifest(version=version, rid="py3-none-any", url=url,
                       sha256=hashlib.sha256(body).hexdigest(), channel="stable",
                       protocol_version=1)
    sig = key.sign(upd.signing_payload(m), ec.ECDSA(hashes.SHA256()))
    from dataclasses import replace
    m = replace(m, signature=base64.b64encode(sig).decode())
    pem = key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    return m, pem


def _artifact(tmp_path, name="cbk.pyz", content=b"OLD"):
    p = tmp_path / name
    p.write_bytes(content)
    return p


async def test_refuses_without_a_pinned_key(tmp_path, monkeypatch):
    monkeypatch.setenv("CBK_AGENT_PATH", str(_artifact(tmp_path)))
    m, _ = _signed(tmp_path, b"NEW")
    async with httpx.AsyncClient() as c:
        result = await upd.UpdateApplier(c, None, log=lambda _: None).apply(m)
    assert result.outcome is upd.Outcome.REFUSED
    assert "signed-or-nothing" in result.detail


async def test_skips_when_not_running_from_a_packaged_artifact(tmp_path, monkeypatch):
    monkeypatch.delenv("CBK_AGENT_PATH", raising=False)
    m, pem = _signed(tmp_path, b"NEW")
    async with httpx.AsyncClient() as c:
        result = await upd.UpdateApplier(c, pem, log=lambda _: None).apply(m)
    # Running from the test venv, not a .pyz: replacing a developer's tree is never intended.
    assert result.outcome is upd.Outcome.SKIPPED
    assert "nothing to replace" in result.detail


async def test_skips_when_already_on_that_version(tmp_path, monkeypatch):
    monkeypatch.setenv("CBK_AGENT_PATH", str(_artifact(tmp_path)))
    m, pem = _signed(tmp_path, b"NEW", version=AGENT_VERSION)
    async with httpx.AsyncClient() as c:
        result = await upd.UpdateApplier(c, pem, log=lambda _: None).apply(m)
    assert result.outcome is upd.Outcome.SKIPPED and AGENT_VERSION in result.detail


async def test_a_missing_verifier_is_reported_as_such_not_as_a_bad_signature(
        tmp_path, monkeypatch):
    """Both refuse, but they need opposite responses from an operator.

    Regression: the zipapp does not vendor `cryptography`, so under a system interpreter
    without it every update was reported "signature invalid for 0.9.9 — refusing". That reads
    as tampering and sends you looking for a key problem, when the fix is one pip install.
    """
    monkeypatch.setenv("CBK_AGENT_PATH", str(_artifact(tmp_path)))
    m, pem = _signed(tmp_path, b"NEW")
    monkeypatch.setattr(upd, "verifier_available", lambda: False)

    fetched: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        fetched.append(str(request.url))
        return httpx.Response(200, content=b"NEW")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        result = await upd.UpdateApplier(c, pem, log=lambda _: None).apply(m)

    assert result.outcome is upd.Outcome.REFUSED       # still fails closed
    assert "cryptography" in result.detail and "pip install" in result.detail
    assert "signature invalid" not in result.detail
    assert fetched == [], "downloaded despite being unable to verify"


def test_verifier_is_available_in_the_dev_environment():
    """Guards the test above from becoming vacuous: if `cryptography` were missing here, the
    real verification tests would all be exercising the no-verifier path instead."""
    assert upd.verifier_available() is True


async def test_bad_signature_is_refused_before_any_fetch(tmp_path, monkeypatch):
    """The signature covers the url, so an invalid one must be rejected with NO network
    call — otherwise a tampered manifest still reaches an attacker-chosen host."""
    monkeypatch.setenv("CBK_AGENT_PATH", str(_artifact(tmp_path)))
    m, pem = _signed(tmp_path, b"NEW", url="https://legit/cbk.pyz")
    from dataclasses import replace
    tampered = replace(m, url="https://evil.invalid/cbk.pyz")

    fetched: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        fetched.append(str(request.url))
        return httpx.Response(200, content=b"NEW")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        result = await upd.UpdateApplier(c, pem, log=lambda _: None).apply(tampered)
    assert result.outcome is upd.Outcome.REFUSED and "signature invalid" in result.detail
    assert fetched == [], f"fetched despite an invalid signature: {fetched}"


async def test_digest_mismatch_is_refused_and_leaves_the_old_artifact(tmp_path, monkeypatch):
    target = _artifact(tmp_path, content=b"OLD")
    monkeypatch.setenv("CBK_AGENT_PATH", str(target))
    m, pem = _signed(tmp_path, b"NEW")          # manifest promises sha256 of b"NEW"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"TAMPERED-IN-FLIGHT")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        result = await upd.UpdateApplier(c, pem, log=lambda _: None).apply(m)
    assert result.outcome is upd.Outcome.REFUSED and "digest mismatch" in result.detail
    assert target.read_bytes() == b"OLD", "a bad download replaced the running artifact"
    assert not (tmp_path / "cbk.pyz.new").exists(), "staged file left behind"


async def test_successful_update_swaps_and_retains_the_previous(tmp_path, monkeypatch):
    target = _artifact(tmp_path, content=b"OLD")
    monkeypatch.setenv("CBK_AGENT_PATH", str(target))
    m, pem = _signed(tmp_path, b"NEW")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"NEW")

    execs: list[list[str]] = []
    monkeypatch.setattr(upd.os, "execv", lambda exe, argv: execs.append(argv))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        result = await upd.UpdateApplier(c, pem, log=lambda _: None).apply(m)

    assert result.outcome is upd.Outcome.APPLIED, result.detail
    assert target.read_bytes() == b"NEW"
    # The outgoing binary is retained, which is what makes rollback possible at all.
    assert (tmp_path / "cbk.prev.pyz").read_bytes() == b"OLD"
    assert execs, "did not re-exec into the new artifact"


async def test_rollback_restores_the_retained_artifact(tmp_path, monkeypatch):
    target = _artifact(tmp_path, content=b"BAD-RELEASE")
    (tmp_path / "cbk.prev.pyz").write_bytes(b"KNOWN-GOOD")
    monkeypatch.setenv("CBK_AGENT_PATH", str(target))

    assert upd.rollback(log=lambda _: None) is True
    assert target.read_bytes() == b"KNOWN-GOOD"
    assert (tmp_path / "cbk.pyz.bad").read_bytes() == b"BAD-RELEASE"


async def test_rollback_is_a_no_op_without_a_retained_artifact(tmp_path, monkeypatch):
    monkeypatch.setenv("CBK_AGENT_PATH", str(_artifact(tmp_path)))
    assert upd.rollback(log=lambda _: None) is False


# --- Windows: refuse rather than fail ambiguously ---------------------------------------

async def test_self_update_refuses_on_windows(monkeypatch, tmp_path):
    """The swap-then-re-exec sequence is unsound on Windows, in two different ways.

    `shutil.move` over the running `.pyz` hits WinError 32 because zipimport holds the
    handle; and if it somehow landed, `os.execv` there spawns a child rather than
    replacing the image, so Task Scheduler's `RestartCount 999` would leave two workers
    on one Redis consumer name — exactly what `_reexec` claims execv prevents.

    A refusal is strictly better than either. A silent failure reads as "this node won't
    update"; the duplicate-consumer case reads as a queue bug somewhere else entirely.
    """
    import httpx

    from cbk_worker.update import Outcome, UpdateApplier, UpdateManifest

    monkeypatch.setattr("cbk_worker.update.os.name", "nt")
    async with httpx.AsyncClient() as client:
        applier = UpdateApplier(client, public_key_pem="-----BEGIN PUBLIC KEY-----")
        m = UpdateManifest(version="9.9.9", url="https://example/cbk.pyz",
                           sha256="0" * 64, signature="x", protocol_version=1)
        got = await applier.apply(m)

    assert got.outcome is Outcome.REFUSED
    assert "Windows" in got.detail and "9.9.9" in got.detail
    assert "consumer name" in got.detail, "the refusal must say WHY, not just decline"


async def test_the_windows_refusal_comes_before_any_download(monkeypatch, tmp_path):
    """It must not fetch a multi-GB artifact only to refuse to install it."""
    import httpx

    from cbk_worker.update import Outcome, UpdateApplier, UpdateManifest

    called = False

    class _Boom(httpx.AsyncClient):
        def stream(self, *a, **k):
            nonlocal called
            called = True
            raise AssertionError("downloaded before checking the platform")

    monkeypatch.setattr("cbk_worker.update.os.name", "nt")
    async with _Boom() as client:
        applier = UpdateApplier(client, public_key_pem="-----BEGIN PUBLIC KEY-----")
        got = await applier.apply(UpdateManifest(
            version="1.0.0", url="https://example/cbk.pyz", sha256="0" * 64,
            signature="x", protocol_version=1))

    assert got.outcome is Outcome.REFUSED
    assert called is False
