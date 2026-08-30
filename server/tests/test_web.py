"""The htmx dashboard (M3b): page + fragments render, vendored asset is served."""

from __future__ import annotations


def test_dashboard_page(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "clusterbuck" in r.text
    assert "/static/htmx.min.js" in r.text  # vendored, not a CDN URL


def test_models_page(client):
    r = client.get("/models")
    assert r.status_code == 200
    assert "clusterbuck" in r.text
    assert "/static/htmx.min.js" in r.text


def test_vendored_htmx_served(client):
    r = client.get("/static/htmx.min.js")
    assert r.status_code == 200
    assert "htmx" in r.text.lower()


def test_headline_fragment(client):
    r = client.get("/ui/headline")
    assert r.status_code == 200
    assert "avoided cloud spend" in r.text


def test_queues_fragment(client):
    assert client.get("/ui/queues").status_code == 200


def test_fleet_fragment_renders_capabilities(client):
    # The conftest client runs from server/, so it loads the seed fleet.yaml.
    r = client.get("/ui/fleet")
    assert r.status_code == 200
    assert "8b-extract" in r.text
    assert "node-a" in r.text and "node-b" in r.text  # capability -> serving node(s)


def test_reservations_fragment(client):
    r = client.get("/ui/reservations")
    assert r.status_code == 200
    assert "Reservations" in r.text


def test_nodes_fragment(client):
    r = client.get("/ui/nodes")
    assert r.status_code == 200
    assert "nodes" in r.text.lower()


def test_nodes_fragment_shows_hardware(client):
    token = client.post("/nodes/tokens").json()["join_token"]
    client.post("/nodes/enroll", json={
        "join_token": token, "hostname": "node-x", "os": "darwin", "arch": "arm64",
        "hw": {"ram_gb": 64, "accelerator": "metal", "vram_gb": 32, "disk_free_gb": 512},
        "profile": "shared",
    })
    r = client.get("/ui/nodes")
    assert r.status_code == 200
    assert "node-x" in r.text
    assert "64 GB RAM" in r.text
    assert "metal" in r.text
    assert "32 GB VRAM" in r.text
    assert "512 GB free" in r.text
    assert "shared" in r.text


def test_ability_fragment(client):
    r = client.get("/ui/ability")
    assert r.status_code == 200
    assert "ability" in r.text.lower()
    assert "llama3.1:70b" in r.text  # seeded artifact
