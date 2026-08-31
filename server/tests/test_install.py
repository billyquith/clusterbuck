"""Approved model installs/removals (M6c): who may act, when, and what a result implies."""

from __future__ import annotations

import json

import pytest

from clusterbuck.catalog import (
    apply_action_result,
    install_allowed,
    next_action,
    scan_node,
    seed_catalog,
)
from clusterbuck.evaluation import SCALE_VERSION, seed_ability
from clusterbuck.models import ActionResult
from clusterbuck.store import Store


@pytest.fixture()
def store(tmp_path) -> Store:
    s = Store(str(tmp_path / "inst.db"))
    seed_catalog(s, now="t")
    seed_ability(s, now="t")
    return s


def _enrolled(store: Store, *, profile="shared", ram=64.0, auto=False) -> str:
    """Insert a node directly (enrollment itself is covered in test_registry)."""
    class _Req:
        hostname, os, arch, profile_ = "h", "darwin", "arm64", profile

        class hw:
            ram_gb, accelerator, vram_gb, disk_free_gb, bench_tps_small = ram, "metal", None, 900.0, None

    req = _Req()
    req.profile = profile
    store.enroll_node(node_id="node-a", node_key="k", req=req,
                      capabilities=json.dumps(["8b-extract"]), enrolled_at="t")
    store.record_heartbeat(node_id="node-a", mode="away",
                           installed=json.dumps(["llama3.2:3b"]), loaded="[]", queues="[]",
                           jobs_done=None, tps=None, last_heartbeat="t")
    if auto:
        store.set_node_flags("node-a", auto_approve=True)
    return "node-a"


# --- presence gating --------------------------------------------------------

def test_install_allowed_respects_owner_presence():
    # A machine someone uses: only pull while they're away.
    assert install_allowed("away", "shared") is True
    assert install_allowed("active", "shared") is False
    assert install_allowed("paused", "shared") is False
    # A dedicated box exists to serve.
    assert install_allowed("active", "dedicated") is True
    assert install_allowed("paused", "dedicated") is False


def test_no_install_while_owner_active(store):
    node_id = _enrolled(store)
    scan_node(store, store.get_node(node_id), now="t")
    # Approve ONLY the upgrade: reclaims are intentionally not presence-gated, so leaving
    # one approved here would mask what this test is checking.
    up = next(p for p in store.list_proposals("pending") if p.kind == "upgrade")
    store.decide_proposal(up.id, "approved", "t")

    assert next_action(store, store.get_node(node_id), mode="active") is None
    assert next_action(store, store.get_node(node_id), mode="away") is not None


def test_dedicated_node_may_install_while_active(store):
    node_id = _enrolled(store, profile="dedicated")
    scan_node(store, store.get_node(node_id), now="t")
    for p in store.list_proposals("pending"):
        store.decide_proposal(p.id, "approved", "t")
    assert next_action(store, store.get_node(node_id), mode="active") is not None


# --- action selection -------------------------------------------------------

def test_only_approved_proposals_become_actions(store):
    node_id = _enrolled(store)
    scan_node(store, store.get_node(node_id), now="t")
    # Nothing approved yet ⇒ nothing to do, however many proposals are pending.
    assert store.list_proposals("pending")
    assert next_action(store, store.get_node(node_id), mode="away") is None


def test_action_carries_registry_ref_and_source(store):
    node_id = _enrolled(store)
    scan_node(store, store.get_node(node_id), now="t")
    up = next(p for p in store.list_proposals("pending") if p.kind == "upgrade")
    store.decide_proposal(up.id, "approved", "t")

    action = next_action(store, store.get_node(node_id), mode="away")
    assert action["kind"] == "install"
    assert action["proposal_id"] == up.id
    assert action["registry_ref"] and action["source"] == "ollama"


def test_reclaim_becomes_a_remove_action_even_when_active(store):
    node_id = _enrolled(store)
    scan_node(store, store.get_node(node_id), now="t")
    rec = next(p for p in store.list_proposals("pending") if p.kind == "reclaim")
    store.decide_proposal(rec.id, "approved", "t")
    # Freeing disk is cheap, so it isn't presence-gated (but `paused` still means stop).
    act = next_action(store, store.get_node(node_id), mode="active")
    assert act["kind"] == "remove" and act["artifact"] == rec.artifact
    assert next_action(store, store.get_node(node_id), mode="paused") is None


# --- results ----------------------------------------------------------------

def test_successful_install_marks_applied_and_demands_reeval(store):
    node_id = _enrolled(store)
    scan_node(store, store.get_node(node_id), now="t")
    up = next(p for p in store.list_proposals("pending") if p.kind == "upgrade")
    store.decide_proposal(up.id, "approved", "t")

    notes = apply_action_result(store, node_id,
                               ActionResult(proposal_id=up.id, ok=True), now="t")
    assert store.get_proposal(up.id).status == "applied"
    # A freshly installed artifact has no measured ability — it must not inherit one.
    reevals = [p for p in store.list_proposals() if p.kind == "reeval"]
    assert any(p.artifact == up.artifact for p in reevals)
    assert any("unmeasured" in n for n in notes)
    assert store.get_ability(up.artifact, "reason", SCALE_VERSION) is None


def test_failed_install_marks_failed_and_does_not_reeval(store):
    node_id = _enrolled(store)
    scan_node(store, store.get_node(node_id), now="t")
    up = next(p for p in store.list_proposals("pending") if p.kind == "upgrade")
    store.decide_proposal(up.id, "approved", "t")

    notes = apply_action_result(
        store, node_id,
        ActionResult(proposal_id=up.id, ok=False, error="disk full"), now="t")
    assert store.get_proposal(up.id).status == "failed"
    assert any("disk full" in n for n in notes)
    assert not [p for p in store.list_proposals() if p.kind == "reeval"]


def test_unknown_proposal_result_is_ignored(store):
    assert apply_action_result(store, "node-a",
                               ActionResult(proposal_id="prop_nope", ok=True), now="t") == []


def test_a_node_cannot_report_on_another_nodes_proposal(store):
    """The heartbeat proves WHICH node is speaking (node_key), but the proposal id is just a
    string in the body. Unchecked, any enrolled node could mark another node's approved
    install `applied` — recording an install that never happened and triggering a re-eval for
    it — or `failed`, cancelling one that was approved."""
    owner = _enrolled(store)
    scan_node(store, store.get_node(owner), now="t")
    up = next(p for p in store.list_proposals("pending") if p.kind == "upgrade")
    store.decide_proposal(up.id, "approved", "t")

    assert apply_action_result(store, "node-somebody-else",
                               ActionResult(proposal_id=up.id, ok=True), now="t") == []
    assert store.get_proposal(up.id).status == "approved", \
        "another node's report must not advance this proposal"
    assert not [p for p in store.list_proposals() if p.kind == "reeval"]

    # The owning node still can.
    assert apply_action_result(store, owner,
                               ActionResult(proposal_id=up.id, ok=True), now="t")
    assert store.get_proposal(up.id).status == "applied"


def test_applied_proposal_is_not_reissued(store):
    node_id = _enrolled(store)
    scan_node(store, store.get_node(node_id), now="t")
    up = next(p for p in store.list_proposals("pending") if p.kind == "upgrade")
    store.decide_proposal(up.id, "approved", "t")
    apply_action_result(store, node_id, ActionResult(proposal_id=up.id, ok=True), now="t")

    nxt = next_action(store, store.get_node(node_id), mode="away")
    assert nxt is None or nxt["proposal_id"] != up.id


# --- through the API --------------------------------------------------------

def _api_enroll(client):
    token = client.post("/nodes/tokens").json()["join_token"]
    return client.post("/nodes/enroll", json={
        "join_token": token, "hostname": "h", "os": "darwin", "arch": "arm64",
        "hw": {"ram_gb": 64.0, "accelerator": "metal", "disk_free_gb": 900},
        "profile": "shared",
    }).json()


def test_heartbeat_issues_action_and_accepts_result(client):
    node = _api_enroll(client)
    hdr = {"x-cbk-node-key": node["node_key"]}
    url = f"/nodes/{node['node_id']}/heartbeat"

    # Away, with an incumbent installed, so upgrades are proposable and permitted.
    client.post(url, json={"mode": "away", "installed": ["llama3.2:3b"]}, headers=hdr)
    created = client.post("/proposals/scan").json()["created"]
    up = next(p for p in client.get("/proposals").json()["proposals"]
              if p["kind"] == "upgrade")
    client.post(f"/proposals/{up['id']}/approve")

    issued = client.post(url, json={"mode": "away", "installed": ["llama3.2:3b"]},
                         headers=hdr).json()["action"]
    assert issued is not None and issued["kind"] == "install"
    assert issued["proposal_id"] == up["id"]

    # Report success; the proposal is applied and re-eval is demanded.
    done = client.post(url, json={
        "mode": "away", "installed": ["llama3.2:3b", up["artifact"]],
        "action_result": {"proposal_id": up["id"], "ok": True},
    }, headers=hdr).json()
    assert any("applied" in n for n in done["planner_notes"])
    statuses = {p["id"]: p["status"] for p in client.get("/proposals").json()["proposals"]}
    assert statuses[up["id"]] == "applied"
    assert created  # scan produced work in the first place


def test_heartbeat_withholds_action_while_active(client):
    node = _api_enroll(client)
    hdr = {"x-cbk-node-key": node["node_key"]}
    url = f"/nodes/{node['node_id']}/heartbeat"
    client.post(url, json={"mode": "away", "installed": ["llama3.2:3b"]}, headers=hdr)
    client.post("/proposals/scan")
    up = next(p for p in client.get("/proposals").json()["proposals"] if p["kind"] == "upgrade")
    client.post(f"/proposals/{up['id']}/approve")

    # The owner is at the keyboard: no multi-GB transfer.
    assert client.post(url, json={"mode": "active", "installed": ["llama3.2:3b"]},
                       headers=hdr).json()["action"] is None
