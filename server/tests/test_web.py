"""The htmx dashboard (M3b): page + fragments render, vendored asset is served."""

from __future__ import annotations

from datetime import UTC

import pytest


@pytest.fixture()
def fleet_client(redis_url, tmp_path):
    """A dashboard backed by a real capability, so the queues panel has a row to render.

    The default `client` has no fleet, and `/ui/queues` iterates `fleet.capabilities` —
    so without one the panel renders its empty state and asserts nothing about the
    numbers.
    """
    from clusterbuck.api import create_app
    from fastapi.testclient import TestClient

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


def test_status_strip_replaces_the_headline(client):
    r = client.get("/ui/status")
    assert r.status_code == 200
    assert "avoided spend" in r.text


def test_status_strip_is_quiet_when_every_queue_is_clear(fleet_client):
    html = fleet_client.get("/ui/status").text
    assert "queues" in html and "clear" in html
    assert "8b-extract" not in html, "a healthy queue is not news"
    assert "reservations" not in html, "an empty reservations list is not shown at all"


def test_status_strip_flags_a_queue_nobody_is_serving(fleet_client):
    """Work queued with no live worker is the state that must never render as healthy.

    The strip leads with backlog — never-delivered work — not `pending`, which counts
    work a worker has already claimed and so reads a healthy zero on exactly this queue.
    """
    for _ in range(3):
        fleet_client.post("/jobs", json={
            "capability": "8b-extract",
            "messages": [{"role": "user", "content": "hello"}],
        })
    html = fleet_client.get("/ui/status").text
    assert "8b-extract" in html and "3 stuck" in html
    assert "clear" not in html


def test_the_panels_the_strip_replaced_are_gone(client):
    """A deletion the suite cannot see is one that did not happen."""
    for route in ("/ui/headline", "/ui/queues", "/ui/reservations", "/ui/fleet"):
        assert client.get(route).status_code == 404, route
    page = client.get("/").text
    assert "/ui/status" in page and "/ui/queues" not in page
    assert "/ui/fleet" not in client.get("/models").text


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


def test_connections_shows_which_models_each_worker_has_warm(client):
    """The question this panel is looked at for: what can each machine answer with now."""
    node_id = _enroll(client, "node-x")
    client.app.state.store.record_heartbeat(
        node_id=node_id, mode="active", installed='["hot:8b", "cold:3b"]',
        loaded='["hot:8b"]', queues="[]", jobs_done=0, tps=None,
        last_heartbeat="2026-01-01T00:00:00Z")
    html = client.get("/ui/connections").text
    warm = html.partition("hot:8b")[0].rpartition("<span")[2]
    cold = html.partition("cold:3b")[0].rpartition("<span")[2]
    assert "ok" in warm, "a loaded model is marked warm"
    assert "ok" not in cold, "an installed-only model is not"


def test_connections_fragment_lists_cloud_providers(client):
    # The conftest client runs from server/, so it loads the seed fleet.yaml, which
    # registers anthropic (claude-sonnet) and openai (gpt-5.6-*) provider accounts.
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


def test_activity_series_by_node_is_one_zero_filled_series_per_worker(client):
    from datetime import datetime

    node_id = _enroll(client, "node-x")
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    store = client.app.state.store
    for i, (node, venue) in enumerate(
            [(node_id, "local"), (node_id, "local"), ("cloud:openai", "cloud")]):
        store.record_usage(
            job_id=f"j{i}", ts=f"{today}T00:00:0{i}Z", capability="c", model="m",
            node=node, venue=venue, tokens_in=1, tokens_out=1, outcome="done",
            cost=0.0, day=today)

    data = client.get("/ui/activity-series?by=node").json()
    assert len(data["days"]) == 30
    labels = [s["label"] for s in data["series"]]
    assert labels == ["node-x", "cloud:openai"], "hostname, not node_id; cloud last"
    local, cloud = data["series"]
    assert len(local["jobs"]) == 30 and local["jobs"][-1] == 2 and sum(local["jobs"]) == 2
    assert cloud["cloud"] is True and local["cloud"] is False


def test_dashboard_page_vendors_the_chart_library(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "/static/uPlot.iife.min.js" in r.text  # vendored, not a CDN URL
    assert "activity-chart" in r.text
    assert client.get("/static/uPlot.iife.min.js").status_code == 200


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

    from clusterbuck.api import create_app
    from clusterbuck.evaluation import SCALE_VERSION
    from clusterbuck.orm.node import Node
    from clusterbuck.store import Store
    from fastapi.testclient import TestClient

    fleet = tmp_path / "fleet.yaml"
    fleet.write_text(
        "capabilities:\n"
        "  8b-extract:\n"
        "    description: 'Pull fields into JSON and tag things'\n"
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
    with s._session() as sess:
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


def test_every_task_class_is_described():
    """A class added to the suite without a description would render as a blank row in
    the legend, and its column header would explain nothing."""
    from clusterbuck.evaluation import TASK_CLASS_DESCRIPTIONS, TASK_CLASSES

    assert list(TASK_CLASS_DESCRIPTIONS) == TASK_CLASSES
    for d in TASK_CLASS_DESCRIPTIONS.values():
        assert d["short"] and d["tests"] and d["good_for"]


def test_the_models_page_explains_its_columns(client):
    """The legend sits on the page, not in the polled partial — a partial swapped every
    15s would close an open <details> mid-read."""
    from clusterbuck.evaluation import TIER1_MAX_ABILITY

    page = client.get("/models").text
    assert 'id="models-legend"' in page
    for tc in ("extract", "summarize", "reason", "code"):
        assert f"<strong>{tc}</strong>" in page
    # The one caveat that answers "is it good at coding?" must not be lost.
    assert "not that it can build" in page
    assert f"{TIER1_MAX_ABILITY:g} is the ceiling" in page
    assert "models-legend" not in client.get("/ui/models").text


def test_a_tier_is_shown_with_what_it_is_for(models_client):
    """`8b-extract` alone says a size and one job kind, not what to send there."""
    for fragment in ("/ui/models", "/ui/nodes"):
        html = models_client.get(fragment).text
        assert "8b-extract" in html
        assert "Pull fields into JSON and tag things" in html, fragment


def test_a_tier_without_a_description_falls_back_to_its_model():
    from clusterbuck.fleet import Fleet
    from clusterbuck.web import _tier_view

    fleet = Fleet(capabilities={"8b-extract": {"model": "good:8b"}})
    assert _tier_view(fleet, "8b-extract") == {
        "name": "8b-extract", "description": None, "model": "good:8b", "defined": True}
    # A tier the registry does not define can never route; the view must say so.
    assert _tier_view(fleet, "ghost")["defined"] is False


def test_the_fleet_endpoint_carries_the_description(models_client):
    body = models_client.get("/fleet").json()
    assert body["capabilities"]["8b-extract"]["description"] == (
        "Pull fields into JSON and tag things")


def test_task_class_headers_carry_their_description(models_client):
    from clusterbuck.evaluation import TASK_CLASS_DESCRIPTIONS

    html = models_client.get("/ui/models").text
    assert TASK_CLASS_DESCRIPTIONS["code"]["short"] in html


# --- node liveness: a registry row is a declaration, not a heartbeat -------------------

def _enroll(client, hostname: str) -> str:
    token = client.post("/nodes/tokens").json()["join_token"]
    r = client.post("/nodes/enroll", json={
        "join_token": token, "hostname": hostname, "os": "linux", "arch": "x86_64",
        "hw": {"ram_gb": 32, "accelerator": "cuda", "vram_gb": 12, "disk_free_gb": 256},
        "profile": "shared",
    })
    return r.json()["node_id"]


def _heartbeat_at(client, node_id: str, when: str) -> None:
    """Write a heartbeat stamp directly: the HTTP route always stamps `now`, and the
    whole point here is a node that last spoke long ago."""
    client.app.state.store.record_heartbeat(
        node_id=node_id, mode="active", installed="[]", loaded="[]", queues="[]",
        jobs_done=0, tps=None, last_heartbeat=when)


def test_heartbeat_age_falls_back_to_enrollment():
    """A node that enrolled and never heartbeated has been silent since it enrolled —
    reading that as "no information" would exempt exactly the nodes that never came up."""
    from datetime import datetime
    from types import SimpleNamespace

    from clusterbuck.web import heartbeat_age_s

    now = datetime(2026, 1, 2, tzinfo=UTC)
    never = SimpleNamespace(last_heartbeat=None, enrolled_at="2026-01-01T00:00:00Z")
    assert heartbeat_age_s(never, now=now) == 86400

    spoke = SimpleNamespace(last_heartbeat="2026-01-01T23:59:00Z",
                            enrolled_at="2026-01-01T00:00:00Z")
    assert heartbeat_age_s(spoke, now=now) == 60

    unreadable = SimpleNamespace(last_heartbeat="not-a-date", enrolled_at=None)
    assert heartbeat_age_s(unreadable, now=now) is None


def test_humanize_age_picks_one_coarse_unit():
    from clusterbuck.web import humanize_age

    assert humanize_age(9) == "9s"
    assert humanize_age(600) == "10m"
    assert humanize_age(7200) == "2h"
    assert humanize_age(86400 * 3) == "3d"


def test_nodes_fragment_marks_a_silent_node(client):
    """The defect this closes: `mode` is what the node last declared, and nothing expired
    it, so a machine powered off months ago still rendered a plain `active` pill."""
    node_id = _enroll(client, "node-gone")
    _heartbeat_at(client, node_id, "2026-01-01T00:00:00Z")

    html = client.get("/ui/nodes").text
    assert "silent" in html and "no heartbeat for" in html
    assert "2026-01-01 00:00" in html, "the stamp itself must be visible, not just the pill"


def test_nodes_fragment_leaves_a_live_node_alone(client):
    from datetime import UTC, datetime

    node_id = _enroll(client, "node-here")
    seen = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    _heartbeat_at(client, node_id, seen)

    html = client.get("/ui/nodes").text
    assert "node-here" in html
    # Positive half: the row rendered AND carries its stamp. Asserting only the absence
    # of the silent pill would pass just as happily if the whole cell failed to render.
    assert f'data-utc="{seen}"' in html
    assert "no heartbeat for" not in html


def test_nodes_fragment_says_never_seen_before_a_first_heartbeat(client):
    _enroll(client, "node-fresh")
    assert "never seen" in client.get("/ui/nodes").text


def test_connections_diagram_marks_a_silent_worker(client):
    """The spoke is drawn from the same declared `mode`, so it claimed a connection to a
    machine that may have been off for months."""
    node_id = _enroll(client, "node-gone")
    _heartbeat_at(client, node_id, "2026-01-01T00:00:00Z")

    html = client.get("/ui/connections").text
    assert "node-gone" in html
    assert "silent" in html and "no heartbeat for" in html


def test_an_unreadable_stamp_is_silent_not_healthy(client):
    """Fail closed. A corrupted or hand-edited row is exactly where a fail-open reading
    would resurrect the original defect: a clean `active` pill on a node nobody has heard
    from."""
    node_id = _enroll(client, "node-corrupt")
    _heartbeat_at(client, node_id, "not-a-date")
    with client.app.state.store._session() as s:
        from clusterbuck.orm.node import Node
        node = s.get(Node, node_id)
        node.enrolled_at = "also-not-a-date"
        s.add(node)
        s.commit()

    html = client.get("/ui/nodes").text
    assert "node-corrupt" in html
    assert "age unknown" in html


# --- proposals: the reasoning laid out, not compressed into one sentence ---------------

def _seed_upgrade(client) -> str:
    """A node that already holds a 5.0-at-extract model, offered a candidate expected to
    score 8 that overflows its 12 GB card — so every column of the card has a value and
    the fit is `degraded`."""
    from clusterbuck.evaluation import SCALE_VERSION

    store = client.app.state.store
    node_id = _enroll(client, "node-x")  # 32 GB RAM, 12 GB VRAM, shared profile
    store.record_heartbeat(
        node_id=node_id, mode="active", installed='["small:3b"]', loaded="[]",
        queues="[]", jobs_done=0, tps=None, last_heartbeat="2026-01-01T00:00:00Z")
    store.set_ability(artifact="small:3b", task_class="extract", score=5.0,
                      scale_version=SCALE_VERSION, updated_at="t", n_items=10, n_passed=5)
    store.upsert_catalog(artifact="big:14b", family="x", params_b=14.0, quant="q4",
                         size_gb=20.0, min_ram_gb=24.0, source="ollama",
                         registry_ref="big:14b", expected_ability=8.0, added_at="t",
                         context_tokens=32768, supports_tools=True)
    store.insert_proposal(
        id="prop_up", kind="upgrade", node_id=node_id, artifact="big:14b",
        incumbent=None, task_class="extract", status="pending", created_at="t",
        rationale="WILL RUN SLOWLY: big:14b ...")
    return node_id


def test_proposal_shows_candidate_against_incumbent(client):
    _seed_upgrade(client)
    html = client.get("/ui/proposals").text
    assert "small:3b" in html, "the incumbent is recomputed; scan_node stores None"
    assert "5" in html and "8" in html and "+3" in html


def test_proposal_sets_requirements_against_hardware(client):
    _seed_upgrade(client)
    html = client.get("/ui/proposals").text
    assert "20 GB" in html and "50 GB quota" in html, "disk need vs the owner's quota"
    assert "24 GB" in html and "32 GB" in html, "RAM need vs RAM present"
    assert "12 GB VRAM" in html
    assert "32,768" in html and "tools" in html, "what the candidate can do"


def test_a_degraded_upgrade_leads_with_the_warning(client):
    """catalog.py puts the warning first in the rationale on purpose; the card must too."""
    _seed_upgrade(client)
    html = client.get("/ui/proposals").text
    assert "will run slowly" in html
    assert html.index("will run slowly") < html.index(">upgrade<")


def test_proposal_actions_are_tooltips_not_a_column(client):
    _seed_upgrade(client)
    html = client.get("/ui/proposals").text
    assert 'title="approve: install big:14b on node-x' in html
    assert 'title="deny: leave node-x' in html
    assert "accept:" not in html, "the old verbose action column is gone"


def test_reeval_and_reclaim_keep_their_rationale(client):
    """Their facts (a digest change, days unused) live only in the text."""
    node_id = _enroll(client, "node-x")
    client.app.state.store.insert_proposal(
        id="prop_re", kind="reclaim", node_id=node_id, artifact="old:7b", incumbent=None,
        task_class=None, status="pending", created_at="t",
        rationale="old:7b has served no jobs in 30 days")
    assert "served no jobs in 30 days" in client.get("/ui/proposals").text


def test_proposals_tab_carries_the_pending_count(client):
    page = client.get("/models").text
    assert 'data-tab="models"' in page and 'data-tab="proposals"' in page
    assert 'id="proposal-count"' in page
    _seed_upgrade(client)
    html = client.get("/ui/proposals").text
    assert 'id="proposal-count"' in html and 'hx-swap-oob="true">1<' in html


def test_deciding_a_proposal_drops_it_and_the_count(client):
    _seed_upgrade(client)
    html = client.post("/ui/proposals/prop_up/approve").text
    assert 'hx-swap-oob="true">0<' in html
    assert "no pending proposals" in html


def test_per_worker_series_follow_enrollment_so_colours_do_not_shift(client):
    """The chart colours a series by position. An idle worker must keep its slot, and a
    newcomer whose hostname sorts first must not push everyone else along."""
    first = _enroll(client, "zz-first")
    _enroll(client, "aa-second")
    today = __import__("datetime").datetime.now(UTC).strftime("%Y-%m-%d")
    client.app.state.store.record_usage(
        job_id="j1", ts=f"{today}T00:00:00Z", capability="c", model="m", node=first,
        venue="local", tokens_in=1, tokens_out=1, outcome="done", cost=0.0, day=today)
    series = client.get("/ui/activity-series?by=node").json()["series"]
    assert [s["label"] for s in series] == ["zz-first", "aa-second"]
    assert sum(series[1]["jobs"]) == 0, "an idle worker still has its (empty) series"


def test_a_cloud_tier_is_not_nowhere_installed(client):
    """Its model lives at the provider; no node was ever going to have it."""
    node_id = _enroll(client, "node-x")
    client.app.state.store.record_heartbeat(
        node_id=node_id, mode="active", installed='["other:1b"]', loaded="[]", queues="[]",
        jobs_done=0, tps=None, last_heartbeat="2026-01-01T00:00:00Z")
    orphans = client.get("/ui/models").text.partition("nowhere installed")[2]
    assert orphans, "a local tier with no node holding its model is still reported"
    assert "gpt-5.6-sol" not in orphans


def test_timeline_names_the_machine_not_its_node_id(client):
    node_id = _enroll(client, "node-x")
    client.app.state.store.record_usage(
        job_id="j1", ts="2026-01-01T00:00:00Z", capability="c", model="m", node=node_id,
        venue="local", tokens_in=1, tokens_out=1, outcome="done", cost=0.0, day="2026-01-01")
    html = client.get("/ui/timeline").text
    assert ">node-x<" in html


# --- placement advice on the proposals tab, gaps on the models tab ----------------------

def _seed_placement(client) -> str:
    """A 32 GB / 12 GB-VRAM shared node holding a measured model, offered one clear
    upgrade (an equally capable model that fits the card where the incumbent spills), one
    candidate it already has, and one that loses — so all three verdicts render."""
    from clusterbuck.evaluation import SCALE_VERSION, TASK_CLASSES

    store = client.app.state.store
    node_id = _enroll(client, "node-p")
    store.record_heartbeat(
        node_id=node_id, mode="active", installed='["have:dense"]', loaded="[]",
        queues="[]", jobs_done=0, tps=None, last_heartbeat="2026-01-01T00:00:00Z")
    for artifact, size, ram, score in (("have:dense", 16.0, 24.0, 7.0),
                                       ("fits:card", 8.0, 16.0, 7.0),
                                       ("weak:1b", 1.0, 2.0, 2.0)):
        store.upsert_catalog(artifact=artifact, family="x", params_b=8.0, quant="q4",
                             size_gb=size, min_ram_gb=ram, source="ollama",
                             registry_ref=artifact, expected_ability=score, added_at="t")
        if artifact != "fits:card":
            for tc in TASK_CLASSES:
                store.set_ability(artifact=artifact, task_class=tc, score=score,
                                  scale_version=SCALE_VERSION, updated_at="t",
                                  n_items=10, n_passed=10)
    for pid, artifact in (("p-weak", "weak:1b"), ("p-have", "have:dense"),
                          ("p-fits", "fits:card")):
        store.insert_proposal(id=pid, kind="upgrade", node_id=node_id, artifact=artifact,
                              incumbent=None, task_class="embed", status="pending",
                              created_at="t", rationale="...")
    return node_id


def test_placement_recommends_per_machine(client):
    _seed_placement(client)
    html = client.get("/ui/placement").text
    assert "node-p" in html
    assert "&#9733; install" in html or "★ install" in html
    assert "fits:card" in html
    assert "runs on the accelerator" in html


def test_proposals_are_ranked_starred_and_superseded(client):
    """They used to be presented equally, oldest-written first — including an upgrade to
    a model the node already has, and one keyed to a task class that no longer exists."""
    _seed_placement(client)
    html = client.get("/ui/proposals").text
    star, have, weak = (html.index(f"/ui/proposals/{p}/approve")
                        for p in ("p-fits", "p-have", "p-weak"))
    assert star < weak < have, "recommended first, superseded last"
    assert "recommended" in html and "superseded" in html and "not recommended" in html
    assert "embed" not in html, "a stale task class must not be shown as the reason"


def test_the_models_tab_names_what_the_fleet_is_missing(client):
    _seed_placement(client)
    html = client.get("/ui/advice").text
    assert "What the fleet is missing" in html
    assert 'data-gap="offline-node"' in html  # its last heartbeat is months old
    # The example fleet this client loads registers cloud tiers, so that gap is closed.
    assert 'data-gap="no-cloud"' not in html


def test_asset_urls_change_when_the_file_does(client, tmp_path, monkeypatch):
    """No Cache-Control is sent, so a browser can reuse a stale copy for days. After a
    deploy that ran new HTML against the old CSS and JS: dead tabs, unstyled cards."""
    import re

    from clusterbuck import web

    page = client.get("/models").text
    for asset in ("dashboard.css", "dashboard.js", "htmx.min.js"):
        assert re.search(rf'/static/{re.escape(asset)}\?v=[0-9a-f]{{12}}"', page), asset
    assert client.get(re.search(r'/static/dashboard\.css\?v=\w+', page)[0]).status_code == 200

    (tmp_path / "static").mkdir()
    f = tmp_path / "static" / "x.css"
    monkeypatch.setattr(web, "WEB_DIR", tmp_path)
    web._asset_version.cache_clear()
    f.write_text("a")
    before = web.static_url("x.css")
    web._asset_version.cache_clear()
    f.write_text("b")
    assert web.static_url("x.css") != before
    web._asset_version.cache_clear()
