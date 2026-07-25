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
    assert hb.json() == {"update": None, "planner_notes": []}

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
