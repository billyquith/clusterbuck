"""Shared-secret auth on the coordinator API (DESIGN.md → Security).

The headline test is `test_anonymous_privilege_escalation_chain_is_closed`: the audited
exploit path (mint token → enroll → flip auto_approve → scan → get an approved multi-GB
install) must be blocked at its first unauthenticated step.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from clusterbuck.api import create_app

KEY = "s3cret-operator-key"

# Deliberately NOT using the `redis_url` fixture: auth is decided before any handler runs,
# and every path asserted here touches only SQLite. Depending on Redis would make this
# suite silently skip when Docker is down — precisely the false-confidence failure mode
# these tests exist to prevent. redis-py connects lazily, so an unused URL is fine.
DUMMY_REDIS = "redis://127.0.0.1:6379/15"


@pytest.fixture()
def authed(tmp_path):
    app = create_app(redis_url=DUMMY_REDIS, db_path=str(tmp_path / "a.db"),
                     start_scheduler=False, api_key=KEY)
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def open_app(tmp_path):
    """No key configured — dev default, everything open."""
    app = create_app(redis_url=DUMMY_REDIS, db_path=str(tmp_path / "b.db"),
                     start_scheduler=False, api_key=None)
    with TestClient(app) as c:
        yield c


# --- the gate ---

@pytest.mark.parametrize("method,path", [
    ("post", "/nodes/tokens"),
    ("get", "/nodes"),
    ("get", "/jobs/job_x"),
    ("get", "/usage"),
    ("get", "/ability"),
    ("post", "/ability/clear"),
    ("get", "/eval"),
    ("post", "/eval/run"),
    ("get", "/catalog"),
    ("post", "/catalog"),
    ("get", "/proposals"),
    ("post", "/proposals/scan"),
    ("get", "/queues"),
    ("get", "/fleet"),
    ("get", "/reservations"),
    ("get", "/"),
    ("get", "/ui/nodes"),
])
def test_protected_without_key(authed, method, path):
    assert getattr(authed, method)(path).status_code == 401


def test_accepts_header_bearer_and_cookie(authed):
    assert authed.get("/fleet", headers={"X-CBK-Api-Key": KEY}).status_code == 200
    assert authed.get("/fleet", headers={"Authorization": f"Bearer {KEY}"}).status_code == 200
    assert authed.get("/fleet", cookies={"cbk_key": KEY}).status_code == 200


def test_wrong_key_rejected(authed):
    assert authed.get("/fleet", headers={"X-CBK-Api-Key": "nope"}).status_code == 401


def test_query_key_exchanges_for_a_cookie(authed):
    # Browser affordance: one visit with ?key= sets the cookie, then fragments work.
    r = authed.get("/", params={"key": KEY})
    assert r.status_code == 200
    assert "cbk_key" in r.cookies or "cbk_key" in authed.cookies
    assert authed.get("/ui/nodes").status_code == 200  # cookie now carried


# --- exemptions: a node must still be able to bootstrap ---

def test_healthz_and_static_are_exempt(authed):
    assert authed.get("/healthz").status_code == 200
    assert authed.get("/static/htmx.min.js").status_code == 200


def test_node_bootstrap_paths_use_their_own_credentials(authed):
    # enroll is gated by the join token, not the shared key…
    body = {"join_token": "jt_bogus", "hostname": "h", "os": "linux", "arch": "arm64",
            "hw": {"ram_gb": 8, "accelerator": "cpu", "disk_free_gb": 10},
            "profile": "shared"}
    assert authed.post("/nodes/enroll", json=body).status_code == 401  # bad token, not bad key

    token = authed.post("/nodes/tokens", headers={"X-CBK-Api-Key": KEY}).json()["join_token"]
    enrolled = authed.post("/nodes/enroll", json={**body, "join_token": token})
    assert enrolled.status_code == 201
    node = enrolled.json()

    # …and heartbeat by the node_key, so a worker needs no operator secret.
    hb = authed.post(f"/nodes/{node['node_id']}/heartbeat", json={"mode": "active"},
                     headers={"x-cbk-node-key": node["node_key"]})
    assert hb.status_code == 200
    assert authed.post(f"/nodes/{node['node_id']}/heartbeat", json={"mode": "active"},
                       headers={"x-cbk-node-key": "wrong"}).status_code == 401


# --- the audited exploit chain ---

def test_anonymous_privilege_escalation_chain_is_closed(authed):
    # Step 1 of the chain — minting a join token — is now refused, so the rest is unreachable.
    assert authed.post("/nodes/tokens").status_code == 401
    assert authed.post("/proposals/scan").status_code == 401
    assert authed.post("/nodes/any-node/policy", json={"auto_approve": True}).status_code == 401
    assert authed.post("/proposals/prop_x/approve").status_code == 401


def test_policy_requires_a_body_not_query_params(authed):
    h = {"X-CBK-Api-Key": KEY}
    token = authed.post("/nodes/tokens", headers=h).json()["join_token"]
    node = authed.post("/nodes/enroll", json={
        "join_token": token, "hostname": "h", "os": "linux", "arch": "arm64",
        "hw": {"ram_gb": 8, "accelerator": "cpu", "disk_free_gb": 10},
        "profile": "shared"}).json()
    nid = node["node_id"]

    # The old query-param form no longer flips anything: a body is required.
    assert authed.post(f"/nodes/{nid}/policy?auto_approve=true", headers=h).status_code == 422
    # Unknown fields are rejected outright.
    assert authed.post(f"/nodes/{nid}/policy", json={"bogus": 1}, headers=h).status_code == 422
    # The legitimate body form still works.
    ok = authed.post(f"/nodes/{nid}/policy", json={"auto_approve": True}, headers=h)
    assert ok.status_code == 200 and ok.json()["auto_approve"] is True


# --- dev default stays usable ---

def test_no_key_configured_leaves_everything_open(open_app):
    assert open_app.get("/fleet").status_code == 200
    assert open_app.post("/nodes/tokens").status_code == 201
    assert open_app.get("/").status_code == 200
