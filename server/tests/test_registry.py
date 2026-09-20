"""Dynamic registry (M4a): join-token mint/burn, enrollment, heartbeat auth, listing."""

from __future__ import annotations


def _enroll_body(token: str, ram_gb: float = 64) -> dict:
    return {
        "join_token": token, "hostname": "node-x", "os": "darwin", "arch": "arm64",
        "hw": {"ram_gb": ram_gb, "accelerator": "metal", "disk_free_gb": 512},
        "profile": "shared",
    }


def test_mint_enroll_heartbeat_list(client):
    token = client.post("/nodes/tokens").json()["join_token"]

    enr = client.post("/nodes/enroll", json=_enroll_body(token, ram_gb=64))
    assert enr.status_code == 201, enr.text
    body = enr.json()
    node_id, node_key = body["node_id"], body["node_key"]
    assert "70b-reason" in body["proposed"]["capabilities"]  # 64 GB → full ladder
    assert body["proposed"]["ladder"]["active"] == ["8b-extract"]

    # A one-time token can't be reused.
    assert client.post("/nodes/enroll", json=_enroll_body(token)).status_code == 401

    # Heartbeat requires the node key.
    hb = client.post(f"/nodes/{node_id}/heartbeat",
                     json={"mode": "away", "loaded": ["qwen2.5:32b"]},
                     headers={"x-cbk-node-key": node_key})
    assert hb.status_code == 200
    body = hb.json()
    assert body["update"] is None and body["planner_notes"] == []
    assert body["action"] is None  # nothing approved to do

    assert client.post(f"/nodes/{node_id}/heartbeat", json={"mode": "away"},
                       headers={"x-cbk-node-key": "wrong"}).status_code == 401
    assert client.post("/nodes/node-missing/heartbeat", json={"mode": "away"},
                       headers={"x-cbk-node-key": node_key}).status_code == 404

    nodes = client.get("/nodes").json()["nodes"]
    me = next(n for n in nodes if n["node_id"] == node_id)
    assert me["mode"] == "away" and me["loaded"] == ["qwen2.5:32b"]
    assert "node_key" not in me  # never leaked


def test_capability_proposal_scales_with_ram(client):
    def caps(ram):
        tok = client.post("/nodes/tokens").json()["join_token"]
        return client.post("/nodes/enroll", json=_enroll_body(tok, ram)).json()["proposed"]["capabilities"]

    assert caps(16) == ["8b-extract"]
    assert caps(32) == ["8b-extract", "32b-reason"]
    assert caps(64) == ["8b-extract", "32b-reason", "70b-reason"]


def test_enroll_rejects_bad_token(client):
    assert client.post("/nodes/enroll", json=_enroll_body("jt_never_minted")).status_code == 401


# --- the registry/worker model mismatch, surfaced across the whole fleet ----------------

def _joined(client, caps=None):
    token = client.post("/nodes/tokens").json()["join_token"]
    body = client.post("/nodes/enroll", json=_enroll_body(token)).json()
    return body["node_id"], body["node_key"]


def test_nodes_reports_tiers_a_node_cannot_actually_honour(client):
    """A 64 GB node is proposed all three tiers. If its model server only has the 3B, the
    two larger tiers are addresses that resolve to a model it will never run — and every
    other signal (enrolment, heartbeat, fitness) says the node is healthy."""
    node_id, node_key = _joined(client)
    client.post(f"/nodes/{node_id}/heartbeat",
                json={"mode": "active", "installed": ["llama3.2:3b"]},
                headers={"x-cbk-node-key": node_key})

    node = next(n for n in client.get("/nodes").json()["nodes"] if n["node_id"] == node_id)
    warnings = " ".join(node["capability_warnings"])
    assert "32b-reason" in warnings and "70b-reason" in warnings
    assert "8b-extract" not in warnings, "flagged the one tier it CAN serve"


def test_a_node_holding_every_registered_model_is_clean(client):
    node_id, node_key = _joined(client)
    client.post(
        f"/nodes/{node_id}/heartbeat",
        json={"mode": "active",
              "installed": ["llama3.2:3b", "qwen2.5:32b", "llama3.1:70b"]},
        headers={"x-cbk-node-key": node_key})
    node = next(n for n in client.get("/nodes").json()["nodes"] if n["node_id"] == node_id)
    assert node["capability_warnings"] == []


def test_the_mismatch_is_not_pushed_back_to_the_worker(client):
    """It is a coordinator-side configuration fact the worker can do nothing about, and it
    holds on every heartbeat — returning it would repeat forever in the worker's log."""
    node_id, node_key = _joined(client)
    hb = client.post(f"/nodes/{node_id}/heartbeat",
                     json={"mode": "active", "installed": ["llama3.2:3b"]},
                     headers={"x-cbk-node-key": node_key})
    assert hb.json()["planner_notes"] == []


# --- capability proposals read the memory that will actually hold the model ------------

def _enroll_hw(client, **hw):
    token = client.post("/nodes/tokens").json()["join_token"]
    body = _enroll_body(token)
    body["hw"].update(hw)
    return client.post("/nodes/enroll", json=body).json()["proposed"]["capabilities"]


def test_a_measured_vram_budget_wins_over_system_ram(client):
    """A 64 GB Mac offers ~48 GB to the GPU, not 64 — so it is proposed two tiers, not
    three. That is the CORRECTION, not a regression: the old proposal advertised a 70B
    tier on a machine that would have to swap to serve it."""
    assert _enroll_hw(client, ram_gb=64, accelerator="metal", vram_gb=48.0) == [
        "8b-extract", "32b-reason"]


def test_a_big_card_still_earns_the_top_tier(client):
    assert _enroll_hw(client, ram_gb=128, accelerator="cuda", vram_gb=80.0) == [
        "8b-extract", "32b-reason", "70b-reason"]


def test_plenty_of_ram_behind_a_small_card_does_not(client):
    """The case a RAM-only gate could not see: a workstation that can HOLD a 70B only by
    spilling it across the bus every token."""
    assert _enroll_hw(client, ram_gb=128, accelerator="cuda", vram_gb=8.0) == ["8b-extract"]


def test_a_cpu_node_is_not_proposed_the_largest_tier(client):
    """It would run, at a speed that makes the tier a promise the node cannot keep."""
    assert _enroll_hw(client, ram_gb=256, accelerator="cpu", vram_gb=None) == [
        "8b-extract", "32b-reason"]


def test_unknown_vram_falls_back_to_system_ram(client):
    """A pre-0.9.0 worker sends no vram_gb. RAM is the honest budget then — it genuinely
    is the memory the model will live in."""
    assert _enroll_hw(client, ram_gb=64, accelerator="metal") == [
        "8b-extract", "32b-reason", "70b-reason"]
