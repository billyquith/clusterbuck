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
            join_password=GOOD_PASSWORD, artifact=None, api_key=None):
    """A coordinator with bootstrap configured as the test wants it.

    Passed as arguments rather than patched onto `settings`, which is a frozen dataclass —
    and the same shape `api_key` already uses, so a destructive config value is visible at
    the call site.
    """
    app = create_app(redis_url=redis_url, db_path=str(tmp_path / "b.db"),
                     fleet_path=str(fleet_file), start_scheduler=False, api_key=api_key,
                     join_password=join_password, worker_artifact=artifact)
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
    assert body["redis_url"] == redis_url
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
    """Gated even though the artifact is public Apache-2.0 code. Not a claim of secrecy —
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
