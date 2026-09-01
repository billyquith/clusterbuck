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


def test_vendored_dashboard_js_served(client):
    # Converts stored-UTC timestamps to the viewer's local time client-side (no server-side
    # timezone to be right or wrong about) — every page includes it alongside htmx.
    r = client.get("/static/dashboard.js")
    assert r.status_code == 200
    assert "data-utc" in r.text
    for page in ("/", "/models", "/performance"):
        assert "/static/dashboard.js" in client.get(page).text


def test_headline_fragment(client):
    r = client.get("/ui/headline")
    assert r.status_code == 200
    assert "avoided cloud spend" in r.text


def test_queues_fragment(client):
    assert client.get("/ui/queues").status_code == 200


def test_connections_fragment_empty(client):
    r = client.get("/ui/connections")
    assert r.status_code == 200
    assert "no workers enrolled" in r.text


def test_connections_fragment_shows_enrolled_worker(client):
    token = client.post("/nodes/tokens").json()["join_token"]
    client.post("/nodes/enroll", json={
        "join_token": token, "hostname": "node-x", "os": "darwin", "arch": "arm64",
        "hw": {"ram_gb": 64, "accelerator": "metal", "vram_gb": 32, "disk_free_gb": 512},
        "profile": "shared",
    })
    r = client.get("/ui/connections")
    assert r.status_code == 200
    assert "node-x" in r.text  # the hostname people actually know it by on the LAN
    assert "0 jobs" in r.text


def test_connections_fragment_lists_cloud_providers(client):
    # The conftest client runs from server/, so it loads the seed fleet.yaml, which
    # registers anthropic (claude-sonnet) and openai (gpt-4o-mini) provider accounts.
    r = client.get("/ui/connections")
    assert r.status_code == 200
    assert "anthropic" in r.text
    assert "openai" in r.text


def test_timeline_fragment_empty(client):
    r = client.get("/ui/timeline")
    assert r.status_code == 200
    assert "no jobs metered yet" in r.text


def test_timeline_fragment_carries_full_utc_timestamp_for_client_side_local_conversion(client):
    # The server has no reliable notion of the viewer's timezone, so it renders a UTC
    # fallback and leaves the actual local-time conversion to dashboard.js in the browser
    # (data-utc carries the full, unsliced value that fallback is truncated from).
    client.app.state.store.record_usage(
        job_id="j1", ts="2026-01-01T00:00:00.123456Z", capability="8b-extract", model="m",
        node="n", venue="local", tokens_in=1, tokens_out=1, outcome="done", cost=0.0,
        day="2026-01-01")
    r = client.get("/ui/timeline")
    assert r.status_code == 200
    assert 'data-utc="2026-01-01T00:00:00.123456Z"' in r.text


def test_activity_series_endpoint_is_zero_filled_json(client):
    r = client.get("/ui/activity-series")
    assert r.status_code == 200
    data = r.json()
    assert set(data) == {"days", "local_jobs", "cloud_jobs", "avoided_spend", "cloud_spend"}
    assert len(data["days"]) == 30
    assert len(data["local_jobs"]) == 30
    assert sum(data["local_jobs"]) == 0  # no usage recorded in this test's fresh db


def test_dashboard_page_vendors_the_chart_library(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "/static/uPlot.iife.min.js" in r.text  # vendored, not a CDN URL
    assert "activity-chart" in r.text
    assert client.get("/static/uPlot.iife.min.js").status_code == 200


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
