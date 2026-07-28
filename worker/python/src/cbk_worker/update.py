"""Signed worker self-update (protocols.md §7, ADR 13).

An update channel is remote-code-execution by design, so the order of operations here is the
security boundary and is deliberately strict — identical to the .NET worker's UpdateApplier:

  1. **Refuse without a pinned public key.** No key ⇒ no self-update, ever. Signed or
     nothing; there is no "trust this once" path.
  2. **Verify the signature before fetching.** The signature covers the url, so a redirected
     download is rejected before any network call to the attacker's host.
  3. **Verify the digest after fetching**, before the bytes are allowed near the install path.
     A signature over a manifest says nothing about what actually arrived.
  4. **Retain the previous artifact** as `<name>.prev`, so a bad release can be rolled back.
  5. **Swap, then re-exec.** The replaced process starts the new artifact and steps aside.

Interop note (the trap that cost the .NET side a debugging session): Python `cryptography`
emits and expects DER (SEC1/RFC 3279) ECDSA signatures. .NET's VerifyData defaults to
IEEE-P1363 (raw r‖s) and returns false — not an error — on a DER blob. This side is the
native DER end of that contract; the .NET worker passes
DSASignatureFormat.Rfc3279DerSequence to meet it.

Deliberately NOT implemented: canary rings and automatic crash-loop rollback. The retained
previous artifact makes rollback *possible* (and `rollback()` performs it), but deciding a
release is crash-looping needs multi-node observation this cannot honestly verify on one
machine — see ADR 13.
"""

from __future__ import annotations

import enum
import hashlib
import os
import shutil
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx

from .config import AGENT_VERSION, PROTOCOL_VERSION
from .models import UpdateManifest


def signing_payload(m: UpdateManifest) -> bytes:
    """Must match server/clusterbuck/signing.py::signing_payload exactly.

    url and protocol_version are inside the signed payload: signing only the first four
    fields would let an attacker redirect the fetch host, or stall the whole fleet via an
    inflated protocol_version.
    """
    return "\n".join([
        m.version, m.rid, m.sha256, m.channel, m.url,
        str(m.protocol_version if m.protocol_version is not None else 1),
    ]).encode()


def verify(m: UpdateManifest, public_key_pem: str) -> bool:
    """True only if `m` carries a valid signature from the pinned key.

    A missing `cryptography` returns False rather than raising: no verifier means no
    verification means no update, which is the same fail-closed answer as a bad signature.
    """
    import base64

    try:
        from cryptography.exceptions import InvalidSignature
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.hazmat.primitives.serialization import load_pem_public_key
    except ImportError:
        return False

    try:
        signature = base64.b64decode(m.signature, validate=True)
    except Exception:
        return False        # malformed base64 is simply an invalid signature

    try:
        key = load_pem_public_key(public_key_pem.encode())
        key.verify(signature, signing_payload(m), ec.ECDSA(hashes.SHA256()))
        return True
    except (InvalidSignature, ValueError, TypeError):
        return False


def should_pause_for_skew(m: UpdateManifest) -> bool:
    """Skew gate (protocols.md §7): a worker too far behind the release's protocol pauses
    pulling until it has updated, rather than speaking a stale queue contract."""
    return m.protocol_version is not None and PROTOCOL_VERSION < m.protocol_version


def current_artifact() -> Path | None:
    """The file this agent runs from, or None when there is nothing meaningful to replace.

    Only a packaged artifact qualifies. Running from a source checkout or an editable venv
    returns None — the same posture as the .NET worker refusing to self-update when it is
    running under the `dotnet` host rather than as a published binary, because replacing a
    developer's working tree from the network is never the intent.
    """
    override = os.environ.get("CBK_AGENT_PATH")
    if override:
        p = Path(override)
        return p if p.is_file() else None

    # A zipapp: this module lives *inside* the archive, so one of its parents IS the file.
    for parent in Path(__file__).resolve().parents:
        if parent.suffix == ".pyz" and parent.is_file():
            return parent

    argv0 = Path(sys.argv[0]).resolve() if sys.argv and sys.argv[0] else None
    if argv0 is not None and argv0.is_file() and argv0.suffix == ".pyz":
        return argv0
    return None


class Outcome(enum.Enum):
    APPLIED = "applied"
    SKIPPED = "skipped"
    REFUSED = "refused"
    FAILED = "failed"


@dataclass(frozen=True)
class UpdateResult:
    outcome: Outcome
    detail: str


class UpdateApplier:
    def __init__(self, client: httpx.AsyncClient, public_key_pem: str | None,
                 log: Callable[[str], None] | None = None) -> None:
        self._client = client
        self._public_key_pem = public_key_pem
        self._log = log or print

    async def apply(self, m: UpdateManifest) -> UpdateResult:
        """Verify and apply a manifest. On success the process re-execs and does not return."""
        if not self._public_key_pem:
            return UpdateResult(Outcome.REFUSED,
                                "no pinned public key (CBK_UPDATE_PUBKEY): self-update is "
                                "signed-or-nothing")

        target = current_artifact()
        if target is None:
            return UpdateResult(Outcome.SKIPPED,
                                "not running from a packaged artifact — nothing to replace")

        if m.version == AGENT_VERSION:
            return UpdateResult(Outcome.SKIPPED, f"already on {m.version}")

        # (2) Signature first: it covers the url, so this rejects a redirected download
        # before any request is made to an attacker-chosen host.
        if not verify(m, self._public_key_pem):
            return UpdateResult(Outcome.REFUSED,
                                f"signature invalid for {m.version} ({m.rid}) — refusing")

        if should_pause_for_skew(m):
            self._log(f"note: {m.version} requires protocol {m.protocol_version}, "
                      f"this agent speaks {PROTOCOL_VERSION}")

        staged = target.with_name(target.name + ".new")
        previous = target.with_name(target.stem + ".prev" + target.suffix)

        try:
            self._log(f"fetching {m.version} ({m.rid}) from {m.url}")
            digest = hashlib.sha256()
            with staged.open("wb") as fh:
                async with self._client.stream("GET", m.url) as resp:
                    resp.raise_for_status()
                    async for chunk in resp.aiter_bytes():
                        digest.update(chunk)
                        fh.write(chunk)

            # (3) The manifest's signature says nothing about what actually arrived.
            actual = digest.hexdigest()
            if actual.lower() != m.sha256.lower():
                staged.unlink(missing_ok=True)
                return UpdateResult(Outcome.REFUSED,
                                    f"digest mismatch: expected {m.sha256}, got {actual}")

            if os.name != "nt":
                staged.chmod(0o755)

            # (4) Retain the outgoing artifact so a bad release can be reverted.
            previous.unlink(missing_ok=True)
            shutil.move(str(target), str(previous))
            shutil.move(str(staged), str(target))
            self._log(f"installed {m.version}; previous artifact retained at {previous}")

            # (5) Hand over to the new artifact and step aside.
            self._reexec(target)
            return UpdateResult(Outcome.APPLIED, f"applied {m.version}, re-executing")
        except Exception as e:
            staged.unlink(missing_ok=True)
            # If the swap half-completed, put the old artifact back rather than leaving none.
            try:
                if not target.exists() and previous.exists():
                    shutil.move(str(previous), str(target))
            except OSError:
                pass
            return UpdateResult(Outcome.FAILED, f"update to {m.version} failed: {e}")

    def _reexec(self, target: Path) -> None:
        self._log("re-executing as the new version; this process image is being replaced")
        sys.stdout.flush()
        sys.stderr.flush()
        # execv replaces this process rather than forking a child: no window in which two
        # workers hold the same consumer name, and no orphan if the parent dies first.
        os.execv(sys.executable, [sys.executable, str(target), *sys.argv[1:]])


def rollback(log: Callable[[str], None] | None = None) -> bool:
    """Restore the retained previous artifact. The manual half of rollback."""
    say = log or print
    target = current_artifact()
    if target is None:
        return False
    previous = target.with_name(target.stem + ".prev" + target.suffix)
    if not previous.is_file():
        return False
    scrap = target.with_name(target.name + ".bad")
    scrap.unlink(missing_ok=True)
    shutil.move(str(target), str(scrap))
    shutil.move(str(previous), str(target))
    say(f"rolled back to the retained artifact; bad build kept at {scrap}")
    return True
