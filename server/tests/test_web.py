"""The htmx dashboard (M3b): page + fragments render, vendored asset is served."""

from __future__ import annotations

import pytest


@pytest.fixture()
def fleet_client(redis_url, tmp_path):
    """A dashboard backed by a real capability, so the queues panel has a row to render.

    The default `client` has no fleet, and `/ui/queues` iterates `fleet.capabilities` —
    so without one the panel renders its empty state and asserts nothing about the
    numbers.
    """
    from fastapi.testclient import TestClient

    from clusterbuck.api import create_app

    fleet = tmp_path / "fleet.yaml"
    fleet.write_text(
        "capabilities:\n"
        "  8b-extract:\n"
        "    queue: 'q:8b-extract'\n"
        "    model_server: 'http://127.0.0.1:1/v1'\n"
        "    model: 'm'\n"
    )
    app = create_app(redis_url=redis_url, db_path=str(tmp_path / "web.db"),
                     fleet_path=str(fleet), start_scheduler=False)
    with TestClient(app) as c:
        yield c


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


def test_queues_fragment_leads_with_backlog_not_in_flight_work(fleet_client):
    """A 200-only assertion is why the wrong number shipped in the first place.

    The panel used to lead with the pending count and label it "the real backlog", but
    pending counts work a worker has ALREADY claimed. Never-delivered work appears in
    neither pending nor (usefully) depth, so the panel showed a healthy green zero for
    exactly the queue it was meant to raise the alarm on.
    """
    r = fleet_client.get("/ui/queues")
    assert r.status_code == 200
    assert "backlog" in r.text
    assert "in flight" in r.text, "pending is labelled for what it is"
    # The specific wrong pairing, not merely the phrase: "the real backlog" is now
    # attached to the backlog column, which is correct. What must never come back is it
    # describing the pending count.
    assert 'not yet acked — the real backlog"' not in r.text
    backlog_col = r.text.partition(">backlog<")[0]
    assert "the real backlog" in backlog_col, "the phrase belongs to the backlog column"


def test_queues_fragment_flags_a_queue_nobody_is_serving(fleet_client):
    """Work queued with no live worker is the state that must never render as healthy."""
    for _ in range(3):
        fleet_client.post("/jobs", json={
            "capability": "8b-extract",
            "messages": [{"role": "user", "content": "hello"}],
        })
    r = fleet_client.get("/ui/queues")
    assert "stuck" in r.text, "backlog with nothing serving is called out explicitly"


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


# --- the models page: one joined table, not four panels sharing no key -----------------

@pytest.fixture()
def models_client(redis_url, tmp_path):
    """A fleet with one tier, one enrolled node holding two artifacts, a seeded score and
    a measured one, and a catalog entry with capability facts — enough for every column
    of the joined row to be exercised at once."""
    import json

    from fastapi.testclient import TestClient

    from clusterbuck.api import create_app
    from clusterbuck.evaluation import SCALE_VERSION
    from clusterbuck.orm.node import Node
    from clusterbuck.store import Store

    fleet = tmp_path / "fleet.yaml"
    fleet.write_text(
        "capabilities:\n"
        "  8b-extract:\n"
        "    model_server: 'http://127.0.0.1:1/v1'\n"
        "    model: 'good:8b'\n"
    )
    db = str(tmp_path / "models.db")
    s = Store(db)
    # A specialist: strong at extract, weak at reason. The old page averaged this away.
    s.set_ability(artifact="good:8b", task_class="extract", score=9.0,
                  scale_version=SCALE_VERSION, updated_at="t", n_items=10, n_passed=9)
    s.set_ability(artifact="good:8b", task_class="reason", score=3.0,
                  scale_version=SCALE_VERSION, updated_at="t", n_items=10, n_passed=3)
    # A shipped guess, which must never look like the measurement above.
    s.set_ability(artifact="guess:3b", task_class="extract", score=4.0,
                  scale_version=SCALE_VERSION, updated_at="t", provenance="seed")
    s.upsert_catalog(artifact="good:8b", family="x", params_b=8.0, quant="q4",
                     size_gb=4.5, min_ram_gb=11.0, source="ollama",
                     registry_ref="good:8b", expected_ability=None, added_at="t",
                     context_tokens=128000, supports_tools=True, supports_vision=False)
    with s._session() as sess:  # noqa: SLF001 — fixture seeding, no enroll round-trip
        sess.add(Node(node_id="node-1", node_key="k", hostname="box", os="linux",
                      arch="x64", mode="active", enrolled_at="t", tps=42.0,
                      capabilities=json.dumps(["8b-extract"]),
                      installed=json.dumps(["good:8b", "guess:3b"]),
                      loaded=json.dumps(["good:8b"])))
        sess.commit()

    app = create_app(redis_url=redis_url, db_path=db, fleet_path=str(fleet),
                     start_scheduler=False)
    with TestClient(app) as c:
        yield c


def test_models_shows_one_row_per_artifact_per_node(models_client):
    html = models_client.get("/ui/models").text
    assert html.count("<tr>") == 3, "header + one row per (artifact, node)"
    assert "good:8b" in html and "guess:3b" in html
    assert "box" in html, "the node must be named on the row, not in a separate panel"


def test_models_shows_each_task_class_not_a_mean(models_client):
    """The single change that makes models comparable.

    `web.py` used to render `sum(scores)/len(scores)`, so a 9-at-extract/3-at-reason
    specialist displayed as 6.0 — identical to a model that is a flat 6 at everything.
    """
    html = models_client.get("/ui/models").text
    assert "9.0" in html and "3.0" in html
    assert "6.0" not in html, "the mean of 9 and 3 must not appear anywhere"


def test_models_distinguishes_a_seed_from_a_measurement(models_client):
    """A placeholder shipped in the code and a 10-item measurement rendered identically."""
    html = models_client.get("/ui/models").text
    assert "seed" in html
    assert "9/10" in html, "evidence count must be shown for a measured score"


def test_models_shows_what_a_model_can_do(models_client):
    """context_tokens / tools / vision are curated per artifact and were on no page."""
    html = models_client.get("/ui/models").text
    assert "128,000" in html
    assert "tools" in html
    assert "4.5 GB" in html


def test_models_flags_an_uncurated_artifact(models_client):
    """guess:3b has no catalog entry, so its size and capabilities are unknown — which
    is a curation prompt, not a blank."""
    assert "uncurated" in models_client.get("/ui/models").text


def test_the_models_page_loads_the_joined_table(client):
    assert "/ui/models" in client.get("/models").text
