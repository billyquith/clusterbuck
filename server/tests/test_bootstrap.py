"""Worker bootstrap (`install/worker/join.py`) — the join-password gate.

These routes exist so the OPERATOR KEY NEVER LEAVES THE COORDINATOR. A joining machine
presents a join password once and receives a single-use token plus the broker URL; it has
no business holding the key that mints tokens, approves model installs and deletes models.

The gate here is load-bearing in a way the rest of the API's is not: both routes are
exempt from the operator-key middleware, so `CBK_JOIN_PASSWORD` is the *only* thing in
front of the Redis credential that `/nodes/bootstrap` hands out.
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from clusterbuck.api import create_app
from clusterbuck.config import JOIN_PASSWORD_MIN_LEN

GOOD_PASSWORD = "a-sufficiently-long-join-password"
HEADER = "X-CBK-Join-Password"


@pytest.fixture()
def fleet_file(tmp_path):
    f = tmp_path / "fleet.yaml"
    f.write_text(
        "capabilities:\n"
        "  8b-extract:\n"
        "    queue: 'q:8b-extract'\n"
        "    model_server: 'http://127.0.0.1:1/v1'\n"
        "    model: 'm'\n"
    )
    return f


def _client(tmp_path, redis_url, fleet_file, *,
            join_password=GOOD_PASSWORD, artifact=None, api_key=None,
            signing_key=None,
            advertise="redis://:pw@192.168.1.10:6379/0"):
    """A coordinator with bootstrap configured as the test wants it.

    Passed as arguments rather than patched onto `settings`, which is a frozen dataclass —
    and the same shape `api_key` already uses, so a destructive config value is visible at
    the call site.
    """
    app = create_app(redis_url=redis_url, db_path=str(tmp_path / "b.db"),
                     fleet_path=str(fleet_file), start_scheduler=False, api_key=api_key,
                     join_password=join_password, worker_artifact=artifact,
                     update_signing_key=signing_key,
                     # Routable on purpose: the test Redis is on localhost, and bootstrap
                     # refuses to advertise a loopback address to a remote worker.
                     broker_advertise_url=advertise)
    return TestClient(app)


# --- the gate -----------------------------------------------------------------------


def test_a_correct_password_returns_a_token_and_the_broker_url(
    tmp_path, redis_url, fleet_file
):
    with _client(tmp_path, redis_url, fleet_file) as c:
        r = c.post("/nodes/bootstrap", headers={HEADER: GOOD_PASSWORD})
    assert r.status_code == 201
    body = r.json()
    assert body["join_token"]
    assert body["redis_url"] == "redis://:pw@192.168.1.10:6379/0"
    assert body["capabilities"] == ["8b-extract"]


def test_the_operator_key_is_never_returned(
    tmp_path, redis_url, fleet_file
):
    """The entire point of the route. A worker authenticates with a join token and then
    its own per-node key; handing it the operator secret would recreate exactly the
    anonymous-chain finding ADR 26 closed."""
    with _client(tmp_path, redis_url, fleet_file,
                 api_key="super-secret-operator-key") as c:
        body = c.post("/nodes/bootstrap", headers={HEADER: GOOD_PASSWORD}).json()
    assert "super-secret-operator-key" not in json.dumps(body)
    assert set(body) == {"join_token", "redis_url", "consumer_group", "capabilities"}


def test_a_wrong_password_is_401(tmp_path, redis_url, fleet_file):
    with _client(tmp_path, redis_url, fleet_file) as c:
        assert c.post("/nodes/bootstrap", headers={HEADER: "wrong"}).status_code == 401


def test_no_password_at_all_is_401(tmp_path, redis_url, fleet_file):
    with _client(tmp_path, redis_url, fleet_file) as c:
        assert c.post("/nodes/bootstrap").status_code == 401


def test_unconfigured_is_404_not_401(tmp_path, redis_url, fleet_file):
    """404 rather than 401 so a coordinator that has not opted in does not advertise that
    the feature exists at all.

    Passes "" rather than None deliberately: None means "fall back to settings" (matching
    `api_key`), which would make this test depend on whether CBK_JOIN_PASSWORD happens to
    be set in the environment running it.
    """
    with _client(tmp_path, redis_url, fleet_file, join_password="") as c:
        r = c.post("/nodes/bootstrap", headers={HEADER: GOOD_PASSWORD})
    assert r.status_code == 404


def test_a_too_short_password_leaves_bootstrap_disabled(
    tmp_path, redis_url, fleet_file
):
    """Fails closed on the feature, not the service: the coordinator still starts, but a
    weak password does not get to stand in front of the broker credential.

    Asserted because the tempting alternative — accept whatever is configured — turns a
    careless env var into a published Redis password.
    """
    weak = "x" * (JOIN_PASSWORD_MIN_LEN - 1)
    with _client(tmp_path, redis_url, fleet_file, join_password=weak) as c:
        assert c.post("/nodes/bootstrap", headers={HEADER: weak}).status_code == 404
        assert c.get("/worker/artifact", headers={HEADER: weak}).status_code == 404


def test_bootstrap_needs_no_operator_key(tmp_path, redis_url, fleet_file):
    """A joining machine has no operator key — that is the whole premise. If the auth
    middleware covered these routes the feature could not work at all."""
    with _client(tmp_path, redis_url, fleet_file,
                 api_key="operator-key-is-set") as c:
        # No X-CBK-Api-Key anywhere, only the join password.
        assert c.post("/nodes/bootstrap",
                      headers={HEADER: GOOD_PASSWORD}).status_code == 201
        # And the key still guards everything else.
        assert c.get("/nodes").status_code == 401


# --- token hygiene ------------------------------------------------------------------


def test_a_failed_attempt_mints_no_token(tmp_path, redis_url, fleet_file):
    """Validate before minting. Minting first would let an unauthenticated caller grow
    the token table with failed guesses."""
    with _client(tmp_path, redis_url, fleet_file) as c:
        for _ in range(5):
            c.post("/nodes/bootstrap", headers={HEADER: "wrong"})
        store = c.app.state.store
        assert store.unused_token_count() == 0

        c.post("/nodes/bootstrap", headers={HEADER: GOOD_PASSWORD})
        assert store.unused_token_count() == 1


def test_the_issued_token_actually_enrolls(tmp_path, redis_url, fleet_file):
    """End to end through the real enrolment path: the token bootstrap hands out must be
    the one `POST /nodes/enroll` accepts, and it must be single-use."""
    with _client(tmp_path, redis_url, fleet_file) as c:
        token = c.post("/nodes/bootstrap",
                       headers={HEADER: GOOD_PASSWORD}).json()["join_token"]
        body = {
            "join_token": token, "hostname": "newpc", "os": "linux", "arch": "x86_64",
            "hw": {"ram_gb": 16.0, "accelerator": "cpu", "disk_free_gb": 100.0},
            "profile": "shared",
        }
        first = c.post("/nodes/enroll", json=body)
        assert first.status_code == 201
        assert first.json()["node_key"]
        assert c.post("/nodes/enroll", json=body).status_code == 401, "token is single-use"


# --- the artifact -------------------------------------------------------------------


def test_the_artifact_is_served_behind_the_same_password(
    tmp_path, redis_url, fleet_file
):
    """Gated even though the artifact carries no secret of its own. Not a claim of secrecy —
    the joining script holds the password anyway, so gating costs nothing and keeps the
    invariant simple: this coordinator serves files to no unauthenticated caller."""
    art = tmp_path / "cbk.pyz"
    art.write_bytes(b"PK\x03\x04 pretend zipapp")
    with _client(tmp_path, redis_url, fleet_file, artifact=str(art)) as c:
        assert c.get("/worker/artifact").status_code == 401
        assert c.get("/worker/artifact", headers={HEADER: "wrong"}).status_code == 401
        r = c.get("/worker/artifact", headers={HEADER: GOOD_PASSWORD})
    assert r.status_code == 200
    assert r.content == b"PK\x03\x04 pretend zipapp"


def test_no_configured_artifact_is_404_with_a_usable_message(
    tmp_path, redis_url, fleet_file
):
    with _client(tmp_path, redis_url, fleet_file, artifact=None) as c:
        r = c.get("/worker/artifact", headers={HEADER: GOOD_PASSWORD})
    assert r.status_code == 404
    assert "CBK_WORKER_ARTIFACT" in r.json()["detail"], "say how to fix it"


def test_a_missing_artifact_file_is_404_not_a_crash(
    tmp_path, redis_url, fleet_file
):
    """Configured but absent — a stale path after a rebuild, which is likelier than never
    configuring it."""
    with _client(tmp_path, redis_url, fleet_file,
                 artifact=str(tmp_path / "gone.pyz")) as c:
        r = c.get("/worker/artifact", headers={HEADER: GOOD_PASSWORD})
    assert r.status_code == 404


# --- the broker address a REMOTE worker is given -------------------------------------


LAN_BROKER = "redis://:pw@192.168.1.10:6379/0"


def test_a_loopback_broker_address_is_refused_not_served(
    tmp_path, redis_url, fleet_file
):
    """The bug this guards, found on a live coordinator after deploying without it.

    Redis normally runs on the coordinator box, so its own CBK_REDIS_URL is loopback.
    Handing that to a joining worker points it at its OWN localhost — and the failure is
    invisible at join time: install, enrolment and service start all succeed, and only
    later does the worker find no broker. Refusing is the only honest answer.

    Note the single-host e2e cannot catch this by construction: there, coordinator and
    worker share a machine, so a loopback address works by accident.
    """
    app = create_app(redis_url="redis://:pw@127.0.0.1:6379/0",
                     db_path=str(tmp_path / "lb.db"), fleet_path=str(fleet_file),
                     start_scheduler=False, join_password=GOOD_PASSWORD)
    with TestClient(app) as c:
        r = c.post("/nodes/bootstrap", headers={HEADER: GOOD_PASSWORD})
    assert r.status_code == 503
    assert "CBK_BROKER_ADVERTISE_URL" in r.json()["detail"], "say how to fix it"


def test_the_advertise_url_overrides_the_coordinators_own(
    tmp_path, redis_url, fleet_file
):
    """The coordinator's own connection and the address it advertises are different
    concerns — loopback for itself, a routable address for everyone else."""
    app = create_app(redis_url="redis://:pw@127.0.0.1:6379/0",
                     db_path=str(tmp_path / "adv.db"), fleet_path=str(fleet_file),
                     start_scheduler=False, join_password=GOOD_PASSWORD,
                     broker_advertise_url=LAN_BROKER)
    with TestClient(app) as c:
        r = c.post("/nodes/bootstrap", headers={HEADER: GOOD_PASSWORD})
    assert r.status_code == 201
    assert r.json()["redis_url"] == LAN_BROKER


def test_localhost_by_name_is_refused_too(tmp_path, redis_url, fleet_file):
    """`localhost` is the likelier spelling in a hand-written env file than 127.0.0.1."""
    app = create_app(redis_url="redis://:pw@localhost:6379/0",
                     db_path=str(tmp_path / "lh.db"), fleet_path=str(fleet_file),
                     start_scheduler=False, join_password=GOOD_PASSWORD)
    with TestClient(app) as c:
        assert c.post("/nodes/bootstrap",
                      headers={HEADER: GOOD_PASSWORD}).status_code == 503


def test_refusing_mints_no_token(tmp_path, redis_url, fleet_file):
    """The loopback check runs before minting, so a misconfigured coordinator does not
    leak tokens to every retry."""
    app = create_app(redis_url="redis://:pw@127.0.0.1:6379/0",
                     db_path=str(tmp_path / "nt.db"), fleet_path=str(fleet_file),
                     start_scheduler=False, join_password=GOOD_PASSWORD)
    with TestClient(app) as c:
        c.post("/nodes/bootstrap", headers={HEADER: GOOD_PASSWORD})
        assert c.app.state.store.unused_token_count() == 0


def test_a_token_survives_being_claimed_concurrently(tmp_path):
    """Single-use has to hold against CONCURRENT claims, not just sequential ones.

    `/nodes/enroll` is auth-exempt by design — a joining machine has no operator key — so
    it is precisely the client-driven endpoint `Store.insert`'s docstring warns about:
    "as many concurrent writers as the retry storm it is there to absorb". The old
    `claim_token` read the row, tested `used`, then assigned and committed, so two joiners
    could both see `used = 0` and both be granted a node identity off one token.

    Threads rather than tasks, because the store is synchronous SQLite and this is a
    write-write race in the database, not in the event loop.
    """
    import threading

    from clusterbuck.store import Store

    store = Store(str(tmp_path / "race.db"))
    store.mint_token("jt_contended", created_at="2026-01-01T00:00:00Z")

    winners: list[bool] = []
    lock = threading.Lock()
    start = threading.Barrier(8)

    def claim(i: int) -> None:
        start.wait()
        got = store.claim_token("jt_contended", used_by=f"node-{i}")
        with lock:
            winners.append(got)

    threads = [threading.Thread(target=claim, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(winners) == 1, f"exactly one claimant may win, got {sum(winners)}"
    assert store.unused_token_count() == 0


# --- the artifact a joining machine is about to run as a service --------------------


def _artifact(tmp_path, body=b"#!/usr/bin/env python3\nzipapp-ish\n"):
    a = tmp_path / "cbk.pyz"
    a.write_bytes(body)
    return a


def test_bootstrap_publishes_a_digest_for_the_artifact_it_serves(
    tmp_path, redis_url, fleet_file
):
    """`join.py` installs this file as a root service. It used to check only that the
    download was non-empty — while the update channel that patches the same binary
    afterwards verifies a signature and a digest before writing a byte.

    The digest must describe the bytes THIS coordinator will actually hand over, so both
    endpoints resolve the artifact through one helper.
    """
    import hashlib

    art = _artifact(tmp_path)
    with _client(tmp_path, redis_url, fleet_file, artifact=art) as c:
        body = c.post("/nodes/bootstrap", headers={HEADER: GOOD_PASSWORD}).json()
        served = c.get("/worker/artifact", headers={HEADER: GOOD_PASSWORD}).content

    assert body["artifact_sha256"] == hashlib.sha256(art.read_bytes()).hexdigest()
    assert body["artifact_sha256"] == hashlib.sha256(served).hexdigest()


def test_bootstrap_signs_the_artifact_and_hands_over_the_verifying_key(
    tmp_path, redis_url, fleet_file
):
    """The digest travels the same connection as the artifact, so it cannot defend
    against an on-path attacker — only a signature checked against an out-of-band key
    can. The key also goes to the node, because without `CBK_UPDATE_PUBKEY` the agent
    refuses every update: correct, but it left the patch channel permanently inert on
    every node built the documented way.
    """
    from clusterbuck.signing import (generate_keypair, private_pem, public_pem,
                                     verify_bootstrap)

    key = generate_keypair()
    key_file = tmp_path / "signing.pem"
    key_file.write_text(private_pem(key))

    art = _artifact(tmp_path)
    with _client(tmp_path, redis_url, fleet_file, artifact=art,
                 signing_key=str(key_file)) as c:
        body = c.post("/nodes/bootstrap", headers={HEADER: GOOD_PASSWORD}).json()

    assert verify_bootstrap(
        key.public_key(), sha256=body["artifact_sha256"],
        version=body["artifact_version"], signature=body["artifact_signature"])
    assert body["update_public_key"].strip() == public_pem(key).strip()


def test_an_artifact_signature_does_not_verify_against_a_different_digest(
    tmp_path, redis_url, fleet_file
):
    """Domain separation and binding, both. The signature covers the digest, so it cannot
    be lifted onto a different binary — which is the whole point of signing it."""
    from clusterbuck.signing import generate_keypair, private_pem, verify_bootstrap

    key = generate_keypair()
    key_file = tmp_path / "signing.pem"
    key_file.write_text(private_pem(key))

    art = _artifact(tmp_path)
    with _client(tmp_path, redis_url, fleet_file, artifact=art,
                 signing_key=str(key_file)) as c:
        body = c.post("/nodes/bootstrap", headers={HEADER: GOOD_PASSWORD}).json()

    assert not verify_bootstrap(
        key.public_key(), sha256="0" * 64,
        version=body["artifact_version"], signature=body["artifact_signature"])


def test_no_artifact_configured_means_no_digest_rather_than_a_crash(
    tmp_path, redis_url, fleet_file
):
    """Bootstrap still has to work for a coordinator that serves no artifact — the joiner
    warns and carries on, so this must not 500."""
    with _client(tmp_path, redis_url, fleet_file, artifact=None) as c:
        r = c.post("/nodes/bootstrap", headers={HEADER: GOOD_PASSWORD})
    assert r.status_code == 201
    assert "artifact_sha256" not in r.json()
    assert r.json()["join_token"]
