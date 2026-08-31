"""Model catalog + planner proposals (M6b): the three gates and the approval flow."""

from __future__ import annotations

import json

import pytest

from clusterbuck.catalog import (
    PROFILE_DISK_QUOTA_GB,
    RECLAIM_UNUSED_DAYS,
    fits,
    propose_reeval,
    quota_for,
    scan_node,
    seed_catalog,
)
from clusterbuck.evaluation import SCALE_VERSION, seed_ability
from clusterbuck.orm.node import Node
from clusterbuck.store import Store


@pytest.fixture()
def store(tmp_path) -> Store:
    s = Store(str(tmp_path / "cat.db"))
    seed_catalog(s, now="t")
    seed_ability(s, now="t")
    return s


def _node(**over) -> Node:
    """A `Node` not persisted through `Store` — `fits`/`scan_node`/`next_action` only
    read attributes, so building one directly lets tests vary just the fields they
    care about without an enroll_node() round trip."""
    base = {
        "node_id": "node-a", "node_key": "k", "profile": "shared", "ram_gb": 64.0,
        "disk_quota_gb": None, "auto_approve": 0, "installed": json.dumps(["llama3.2:3b"]),
        "enrolled_at": "t",
    }
    base.update(over)
    return Node(**base)


# --- gate 1: fits ------------------------------------------------------------

def test_quota_defaults_follow_profile():
    assert quota_for("dedicated", None) == PROFILE_DISK_QUOTA_GB["dedicated"]
    assert quota_for("background", None) == PROFILE_DISK_QUOTA_GB["background"]
    assert quota_for("shared", 7.5) == 7.5  # explicit override wins


def test_fits_rejects_insufficient_ram(store):
    big = next(r for r in store.list_catalog() if r.artifact == "llama3.1:70b")
    ok, why = fits(big, _node(ram_gb=16.0), quota_gb=500)
    assert not ok and "RAM" in why


def test_fits_rejects_over_quota(store):
    big = next(r for r in store.list_catalog() if r.artifact == "llama3.1:70b")
    ok, why = fits(big, _node(ram_gb=128.0), quota_gb=10.0)
    assert not ok and "quota" in why


def test_fits_accepts_when_within_both(store):
    small = next(r for r in store.list_catalog() if r.artifact == "llama3.1:8b")
    ok, _ = fits(small, _node(ram_gb=64.0), quota_gb=100.0)
    assert ok


# --- gate 2: the upgrade proposal -------------------------------------------

def test_upgrade_proposed_for_better_fitting_artifact(store):
    ids = scan_node(store, _node(), now="t")
    props = {p.artifact: p for p in store.list_proposals()}
    # 64 GB / 50 GB shared quota: 8b, 14b, 32b fit; 70b needs 64 GB RAM (ok) but is 40 GB
    # (within 50) — so it fits too. All beat the installed 3b's ability.
    assert "qwen2.5:32b" in props
    assert props["qwen2.5:32b"].kind == "upgrade"
    assert props["qwen2.5:32b"].status == "pending"   # human decides by default
    assert len(ids) >= 1


def test_no_upgrade_when_nothing_fits(store):
    # A tiny node with a tiny quota: the only fitting artifact is already installed.
    ids = scan_node(store, _node(ram_gb=8.0, disk_quota_gb=3.0), now="t")
    kinds = {p.kind for p in store.list_proposals()}
    assert "upgrade" not in kinds


def test_installed_artifact_is_not_proposed(store):
    scan_node(store, _node(installed=json.dumps(["qwen2.5:32b"])), now="t")
    arts = {p.artifact for p in store.list_proposals() if p.kind == "upgrade"}
    assert "qwen2.5:32b" not in arts


def test_scan_is_idempotent(store):
    first = scan_node(store, _node(), now="t")
    second = scan_node(store, _node(), now="t")
    assert first and not second  # no duplicate proposals on a repeat pass


# --- gate 3: approval -------------------------------------------------------

def test_auto_approve_opt_in_skips_human(store):
    scan_node(store, _node(auto_approve=1), now="t")
    ups = [p for p in store.list_proposals() if p.kind == "upgrade"]
    assert ups and all(p.status == "approved" for p in ups)


def test_reclaim_is_never_auto_approved(store):
    # Even with auto_approve on, deleting an owner's data waits for a human.
    scan_node(store, _node(auto_approve=1, installed=json.dumps(["unused:1b"])), now="t")
    rec = [p for p in store.list_proposals() if p.kind == "reclaim"]
    assert rec and all(p.status == "pending" for p in rec)


def test_decide_proposal_is_single_shot(store):
    ids = scan_node(store, _node(), now="t")
    pid = ids[0]
    assert store.decide_proposal(pid, "approved", "t2") is True
    assert store.decide_proposal(pid, "denied", "t3") is False  # already decided
    assert store.get_proposal(pid).status == "approved"


# --- reclaim + reeval -------------------------------------------------------

def test_reclaim_proposed_for_unused_model(store):
    scan_node(store, _node(installed=json.dumps(["llama3.2:3b", "stale:7b"])), now="t")
    rec = {p.artifact for p in store.list_proposals() if p.kind == "reclaim"}
    assert "stale:7b" in rec  # no usage rows at all ⇒ unused


def test_reclaim_skipped_for_recently_used_model(store):
    import time

    today = time.strftime("%Y-%m-%d")
    store.record_usage(job_id="j1", ts="t", capability="8b-extract", model="busy:7b",
                       node="node-a", venue="local", tokens_in=1, tokens_out=1,
                       outcome="done", cost=0.0, day=today)
    scan_node(store, _node(installed=json.dumps(["busy:7b"])), now="t")
    rec = {p.artifact for p in store.list_proposals() if p.kind == "reclaim"}
    assert "busy:7b" not in rec


def test_digest_change_records_and_proposes_reeval(store):
    now = "t"
    assert store.observe_node_models("node-a", {"m:7b": "sha256:aaa"}, now) == []
    changed = store.observe_node_models("node-a", {"m:7b": "sha256:bbb"}, now)
    assert changed == [("m:7b", "sha256:aaa", "sha256:bbb")]

    pid = propose_reeval(store, "node-a", "m:7b", "sha256:aaa", "sha256:bbb", now=now)
    assert pid is not None
    prop = store.get_proposal(pid)
    assert prop.kind == "reeval" and prop.status == "pending"
    # Idempotent while one is open.
    assert propose_reeval(store, "node-a", "m:7b", "sha256:aaa", "sha256:bbb", now=now) is None


def test_missing_digest_does_not_false_trigger(store):
    store.observe_node_models("node-a", {"m:7b": "sha256:aaa"}, "t")
    # A server that stops reporting digests must not look like a change.
    assert store.observe_node_models("node-a", {"m:7b": None}, "t") == []


# --- API surface ------------------------------------------------------------

def _enroll(client, ram_gb=64.0):
    token = client.post("/nodes/tokens").json()["join_token"]
    return client.post("/nodes/enroll", json={
        "join_token": token, "hostname": "h", "os": "darwin", "arch": "arm64",
        "hw": {"ram_gb": ram_gb, "accelerator": "metal", "disk_free_gb": 900},
        "profile": "shared",
    }).json()


def test_catalog_endpoint_lists_seeded_artifacts(client):
    arts = {a["artifact"] for a in client.get("/catalog").json()["artifacts"]}
    assert "qwen2.5:32b" in arts and "llama3.2:3b" in arts


def test_scan_then_approve_and_deny(client):
    node = _enroll(client)
    # Report an installed model so there's an incumbent to beat.
    client.post(f"/nodes/{node['node_id']}/heartbeat",
                json={"mode": "active", "installed": ["llama3.2:3b"]},
                headers={"x-cbk-node-key": node["node_key"]})

    created = client.post("/proposals/scan").json()["created"]
    assert created, "expected at least one proposal"

    pending = client.get("/proposals", params={"status": "pending"}).json()["proposals"]
    assert len(pending) == len(created)

    pid = created[0]
    ok = client.post(f"/proposals/{pid}/approve")
    assert ok.status_code == 200 and ok.json()["status"] == "approved"
    # Deciding twice is a conflict, not a silent overwrite.
    assert client.post(f"/proposals/{pid}/deny").status_code == 409
    assert client.post("/proposals/prop_missing/approve").status_code == 404

    if len(created) > 1:
        d = client.post(f"/proposals/{created[1]}/deny")
        assert d.json()["status"] == "denied"


def test_digest_change_via_heartbeat_raises_reeval(client):
    node = _enroll(client)
    hdr = {"x-cbk-node-key": node["node_key"]}
    url = f"/nodes/{node['node_id']}/heartbeat"

    r1 = client.post(url, json={"mode": "active", "installed": ["m:7b"],
                                "digests": {"m:7b": "sha256:aaa"}}, headers=hdr)
    assert r1.json()["planner_notes"] == []

    r2 = client.post(url, json={"mode": "active", "installed": ["m:7b"],
                                "digests": {"m:7b": "sha256:bbb"}}, headers=hdr)
    assert any("changed upstream" in n for n in r2.json()["planner_notes"])
    kinds = {p["kind"] for p in client.get("/proposals").json()["proposals"]}
    assert "reeval" in kinds


def test_digest_change_actually_drops_the_stale_score(client):
    """ADR 15: a changed digest is a NEW artifact and inherits nothing.

    Raising a `reeval` proposal was not enough — nothing consumed those proposals and nothing
    cleared the ability row, so the stale score kept driving routing forever. This asserts the
    EFFECT (score gone, artifact re-queued for measurement), not the announcement.
    """
    from clusterbuck.evaluation import SCALE_VERSION

    node = _enroll(client)
    hdr = {"x-cbk-node-key": node["node_key"]}
    url = f"/nodes/{node['node_id']}/heartbeat"
    store = client.app.state.store

    client.post(url, json={"mode": "active", "installed": ["m:7b"],
                           "digests": {"m:7b": "sha256:aaa"}}, headers=hdr)
    store.set_ability(artifact="m:7b", task_class="extract", score=9.0,
                      scale_version=SCALE_VERSION, updated_at="t")
    assert store.get_ability("m:7b", "extract", SCALE_VERSION) == 9.0

    # The artifact changes upstream.
    client.post(url, json={"mode": "active", "installed": ["m:7b"],
                           "digests": {"m:7b": "sha256:bbb"}}, headers=hdr)

    assert store.get_ability("m:7b", "extract", SCALE_VERSION) is None, \
        "a changed digest must not inherit the previous artifact's score"
    # …nor the measurements that score was computed from: the heartbeat must open a new
    # measurement generation, or the next batch is averaged with the old artifact's items.
    assert store.current_eval_generation("m:7b") == 2, \
        "a changed digest must start a new measurement generation, not extend the old one"
    # And it is genuinely back in the measurement queue.
    from clusterbuck.eval_runner import artifacts_needing_eval
    assert "m:7b" in [a for a, _ in artifacts_needing_eval(store)]


def test_node_policy_sets_quota_and_auto_approve(client):
    node = _enroll(client)
    # A JSON body, not query params — see ADR 26: as bare scalars these bound as query
    # parameters, which made flipping the human-approval gate a one-line request.
    r = client.post(f"/nodes/{node['node_id']}/policy",
                    json={"disk_quota_gb": 12.5, "auto_approve": True}).json()
    assert r["disk_quota_gb"] == 12.5 and r["auto_approve"] is True
    assert client.post("/nodes/node-missing/policy",
                       json={"auto_approve": True}).status_code == 404
    # The old query-param form no longer carries any authority.
    assert client.post(f"/nodes/{node['node_id']}/policy",
                       params={"auto_approve": True}).status_code == 422


def test_proposals_fragment_renders_buttons(client):
    node = _enroll(client)
    client.post(f"/nodes/{node['node_id']}/heartbeat",
                json={"mode": "active", "installed": ["llama3.2:3b"]},
                headers={"x-cbk-node-key": node["node_key"]})
    client.post("/proposals/scan")

    html = client.get("/ui/proposals").text
    assert "approve" in html and "deny" in html
    assert "hx-post" in html
