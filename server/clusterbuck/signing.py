"""Release-manifest signing (protocols.md §7, ADR 13): the self-update channel is
remote-code-execution by design, so it ships signed-or-nothing.

ECDSA P-256 over SHA-256. The signature covers a drift-free payload — the newline-joined
`version, rid, sha256, channel` — rather than re-serialized JSON, so key order / whitespace
can't break verification. The artifact is pinned by `sha256` inside that payload, so a
valid signature vouches for a specific binary.

The private key is operator-held and NEVER enters git (it's RCE if leaked). The worker
pins the corresponding public key. `cryptography` emits DER (SEC1/RFC 3279) signatures;
the C# verifier must be told to expect that format (see UpdateVerifier).
"""

from __future__ import annotations

import base64

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec


def signing_payload(*, version: str, rid: str, sha256: str, channel: str,
                    url: str, protocol_version: int) -> bytes:
    """The exact bytes a signature covers.

    `url` and `protocol_version` are inside the payload deliberately. Signing only
    version/rid/sha256/channel left two gaps: an on-path attacker could rewrite `url` to
    choose which host a worker fetches from (the digest still catches a swapped binary, but
    the fetch itself goes wherever they say), and could inflate `protocol_version` to make
    every worker pause pulling — a fleet-wide stall that bypasses the signed channel.
    """
    return "\n".join([version, rid, sha256, channel, url, str(protocol_version)]).encode()


def generate_keypair() -> ec.EllipticCurvePrivateKey:
    return ec.generate_private_key(ec.SECP256R1())


def public_pem(private_key: ec.EllipticCurvePrivateKey) -> str:
    return private_key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()


def private_pem(private_key: ec.EllipticCurvePrivateKey) -> str:
    """PKCS8 PEM for the operator to store the signing key (outside the repo)."""
    return private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


def load_private_pem(pem: str | bytes) -> ec.EllipticCurvePrivateKey:
    if isinstance(pem, str):
        pem = pem.encode()
    key = serialization.load_pem_private_key(pem, password=None)
    assert isinstance(key, ec.EllipticCurvePrivateKey)
    return key


def sign_manifest(private_key: ec.EllipticCurvePrivateKey, *, version: str, rid: str,
                  sha256: str, channel: str, url: str, protocol_version: int) -> str:
    """Return the base64 DER ECDSA signature over the manifest's signing payload."""
    sig = private_key.sign(
        signing_payload(version=version, rid=rid, sha256=sha256, channel=channel,
                        url=url, protocol_version=protocol_version),
        ec.ECDSA(hashes.SHA256()),
    )
    return base64.b64encode(sig).decode()


def verify_manifest(public_key: ec.EllipticCurvePublicKey, manifest: dict) -> bool:
    try:
        public_key.verify(
            base64.b64decode(manifest["signature"]),
            signing_payload(
                version=manifest["version"], rid=manifest["rid"],
                sha256=manifest["sha256"], channel=manifest["channel"],
                url=manifest["url"], protocol_version=int(manifest["protocol_version"]),
            ),
            ec.ECDSA(hashes.SHA256()),
        )
        return True
    except (InvalidSignature, KeyError, ValueError):
        return False


def build_manifest(private_key: ec.EllipticCurvePrivateKey, *, version: str, rid: str,
                   url: str, sha256: str, channel: str,
                   protocol_version: int) -> dict:
    """Assemble a signed release manifest (protocols.md §7)."""
    return {
        "version": version, "rid": rid, "url": url, "sha256": sha256,
        "channel": channel, "protocol_version": protocol_version,
        "signature": sign_manifest(private_key, version=version, rid=rid,
                                    sha256=sha256, channel=channel, url=url,
                                    protocol_version=protocol_version),
    }


# --- bootstrap artifact ---------------------------------------------------------------
#
# The update channel was signed from the start; the BOOTSTRAP that installs the first copy
# was not. `join.py` fetched `cbk.pyz` over plain HTTP and checked only that it was
# non-empty, then installed it as a service — so an on-path attacker on the LAN (ARP, DNS
# or mDNS spoofing of the coordinator's name) had root-installed code execution on every
# joining machine, against a coordinator whose patch channel is rigorously signed.
#
# Same primitive, separate payload. A manifest signature must not be replayable as an
# artifact signature or vice versa, so the two are domain-separated by a prefix rather
# than sharing `signing_payload`.
_BOOTSTRAP_DOMAIN = "cbk-bootstrap-artifact-v1"


def bootstrap_payload(*, sha256: str, version: str) -> bytes:
    """The exact bytes an artifact signature covers: the digest, bound to a version."""
    return "\n".join([_BOOTSTRAP_DOMAIN, sha256, version]).encode()


def sign_bootstrap(private_key: ec.EllipticCurvePrivateKey, *, sha256: str,
                   version: str) -> str:
    return base64.b64encode(private_key.sign(
        bootstrap_payload(sha256=sha256, version=version),
        ec.ECDSA(hashes.SHA256()),
    )).decode()


def verify_bootstrap(public_key: ec.EllipticCurvePublicKey, *, sha256: str, version: str,
                     signature: str) -> bool:
    try:
        public_key.verify(
            base64.b64decode(signature),
            bootstrap_payload(sha256=sha256, version=version),
            ec.ECDSA(hashes.SHA256()),
        )
        return True
    except (InvalidSignature, KeyError, ValueError):
        return False
