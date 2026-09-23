"""Load-test driver (Performance page): category generators, run lifecycle, dashboard
fragments. Integration tests answer jobs directly via Redis (a fake worker), mirroring
test_api.py's `test_poll_queued_then_done` pattern, rather than running a real worker.
"""

from __future__ import annotations

import json
import random
import time

import pytest
import redis
from clusterbuck.perf_runner import CATEGORIES

# --- queries come from the shared suite: pure unit tests, no fixtures ---

def _long_reply(n: int = 120) -> str:
    return " ".join(["word"] * n)


@pytest.mark.parametrize("name", sorted(CATEGORIES))
def test_every_category_is_a_task_class_drawn_from_the_shared_suite(name):
    """One category per task class now, each drawing prompts from `SEED_SUITE`.

    The seven bespoke generators are gone: they carried a parallel set of prompts and
    "deliberately approximate" checkers, which made a third notion of correctness that
    agreed with nothing. A load run and an ability measurement now ask the same questions.
    """
    from clusterbuck.evaluation import TASK_CLASSES, items_for

    assert name in TASK_CLASSES
    prompts = {it.prompt for it in items_for(name)}
    seen = set()
    for seed in range(20):
        gq = CATEGORIES[name](random.Random(seed))
        assert gq.category == name and gq.task_class == name
        assert 1 <= gq.min_ability <= 10
        assert gq.prompt in prompts, "a load query must come from the shared suite"
        seen.add(gq.prompt)
    assert len(seen) > 1, "20 draws should not all land on the same item"


@pytest.mark.parametrize("name", sorted(CATEGORIES))
def test_a_categorys_check_rejects_a_bad_reply(name):
    """Proves the check is not trivially always-true, and that it still returns the
    (ok, detail) pair the sample rows record."""
    for seed in range(20):
        gq = CATEGORIES[name](random.Random(seed))
        # A long reply fails every class: the extract/reason/code items want a specific
        # value, and the summarize items impose a word limit.
        ok, detail = gq.check(_long_reply())
        assert ok is False
        assert isinstance(detail, str) and detail


def test_the_ability_floors_still_separate_served_from_unassigned():
    """`deploy/e2e/perf.sh` depends on these: a default run must produce BOTH served and
    unassigned samples on a fleet whose best local artifact sits around 4. Flattening the
    floors would make every sample land the same way and the load test stop showing
    routing gaps at all."""
    floors = {name: CATEGORIES[name](random.Random(0)).min_ability for name in CATEGORIES}
    assert floors["extract"] <= 4 and floors["summarize"] <= 4, "these must be servable"
    assert floors["reason"] > 4 and floors["code"] > 4, "these must outrun a 4-ability fleet"


# --- integration: real HTTP + SQLite, jobs answered directly via Redis ---

@pytest.fixture()
def app_client(redis_url, tmp_path):
    from clusterbuck.api import create_app
    from fastapi.testclient import TestClient

    app = create_app(
        redis_url=redis_url, db_path=str(tmp_path / "perf.db"), start_scheduler=False
    )
    with TestClient(app) as c:
        yield app, c


def _drain_pending_jobs(store, redis_url: str, content: str = "generic reply") -> None:
    """Answer every job with no result yet, generically, as a fake worker would."""
    conn = redis.from_url(redis_url, decode_responses=True)
    try:
        for row in store.jobs_awaiting_usage():
            if conn.exists(row.result_key):
                continue
            conn.set(row.result_key, json.dumps({
                "job_id": row.id, "status": "done", "worker": "node-fake",
                "completed_at": "t",
                "completion": {"choices": [
                    {"message": {"role": "assistant", "content": content}}
                ]},
                "usage": {"prompt_tokens": 10, "completion_tokens": 5},
            }))
    finally:
        conn.close()


def _wait_for_run(client, run_id: str, *, redis_url: str, store, timeout_s: float = 15.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        _drain_pending_jobs(store, redis_url)
        got = client.get(f"/perf/runs/{run_id}").json()
        if got["status"] != "running":
            return got
        time.sleep(0.15)
    pytest.fail(f"perf run {run_id} never finished")


def test_run_served_jobs_recorded(app_client, redis_url):
    app, client = app_client
    r = client.post("/perf/runs", json={
        # Pinned to `extract`: its suite items check for a specific extracted value, so a
        # canned reply fails all of them. `summarize` cannot be used here — see
        # test_three_summarize_items_pass_any_short_reply below.
        "label": "served", "n_jobs": 3, "duration_s": 10, "warmup_s": 0, "concurrency": 1,
        "categories": ["extract"],
    })
    assert r.status_code == 201
    run_id = r.json()["id"]
    got = _wait_for_run(client, run_id, redis_url=redis_url, store=app.state.store)

    assert got["status"] == "done"
    assert got["n_measured"] == 3
    assert got["n_served"] == 3
    assert got["n_unassigned"] == 0
    # A generic canned reply matches none of the extract checks — real discrimination,
    # not a rigged all-pass (mirrors eval.sh's own stub-server philosophy).
    assert got["pass_rate"] == 0.0
    assert got["jobs_per_s"] is not None


def test_unassigned_when_no_local_model_clears_the_bar(app_client):
    _app, client = app_client
    # SEED_ABILITY's strongest local artifact tops out well under 10 on every task class.
    r = client.post("/perf/runs", json={
        "label": "stretch", "n_jobs": 2, "duration_s": 10, "warmup_s": 0,
        "concurrency": 1, "min_ability_override": 10,
    })
    run_id = r.json()["id"]
    deadline = time.monotonic() + 10
    got = None
    while time.monotonic() < deadline:
        got = client.get(f"/perf/runs/{run_id}").json()
        if got["status"] != "running":
            break
        time.sleep(0.1)
    assert got["status"] == "done"
    assert got["n_unassigned"] == 2
    assert got["n_served"] == 0


def test_warmup_samples_excluded_from_stats(app_client):
    _app, client = app_client
    r = client.post("/perf/runs", json={
        "label": "warmup", "n_jobs": 2, "duration_s": 10, "warmup_s": 100,
        "concurrency": 1, "min_ability_override": 10,  # unassigned ⇒ instant, no worker needed
    })
    run_id = r.json()["id"]
    deadline = time.monotonic() + 10
    got = None
    while time.monotonic() < deadline:
        got = client.get(f"/perf/runs/{run_id}").json()
        if got["status"] != "running":
            break
        time.sleep(0.1)
    assert got["n_warmup"] == 2
    assert got["n_measured"] == 0
    assert got["pass_rate"] is None
    assert got["jobs_per_s"] is None


def test_cancel_stops_a_running_run(app_client):
    _app, client = app_client
    r = client.post("/perf/runs", json={
        "label": "cancel-me", "duration_s": 30, "warmup_s": 0, "concurrency": 1,
        "min_ability_override": 10,
    })
    run_id = r.json()["id"]
    cr = client.post(f"/perf/runs/{run_id}/cancel")
    assert cr.status_code == 200
    deadline = time.monotonic() + 5
    status = None
    while time.monotonic() < deadline:
        status = client.get(f"/perf/runs/{run_id}").json()["status"]
        if status != "running":
            break
        time.sleep(0.1)
    assert status == "cancelled"


def test_unknown_category_rejected(app_client):
    _app, client = app_client
    r = client.post("/perf/runs", json={"categories": ["not-a-real-category"]})
    assert r.status_code == 422


def test_three_summarize_items_pass_any_short_reply():
    """A weakness in the SHARED tier-1 suite, recorded here because this is where it
    first bit: three of the ten `summarize` items check only a word limit, so any short
    string clears them.

    That is not a load-driver problem — it means a model can bank roughly 30% of the
    summarize class by emitting anything brief, which inflates its measured ability. Left
    as-is deliberately: tightening the suite changes what every existing score means and
    needs a `SUITE_VERSION` bump, which is a decision about the instrument, not about
    this page. Locked down so it stays a known quantity rather than a surprise.
    """
    from clusterbuck.evaluation import items_for

    lenient = [i for i, it in enumerate(items_for("summarize"))
               if it.check("completely unrelated filler text")]
    assert lenient == [4, 6, 9], (
        f"the lenient summarize items moved to {lenient}; if the suite was tightened on "
        "purpose, bump SUITE_VERSION so existing scores are re-measured")


def test_ui_start_form_creates_a_run(app_client):
    """The htmx form posts application/x-www-form-urlencoded, not JSON — a real gap the
    JSON-API tests above don't exercise (this caught a missing python-multipart dep)."""
    _app, client = app_client
    r = client.post("/ui/perf/start", data={
        "label": "from-the-form", "categories": ["extract"],
        "concurrency": "1", "duration_s": "10", "warmup_s": "0",
        "n_jobs": "1", "min_ability_override": "10",
    })
    assert r.status_code == 200
    assert "from-the-form" in r.text


def test_dashboard_fragments_render(app_client, redis_url):
    app, client = app_client
    assert client.get("/performance").status_code == 200
    assert "Start a run" in client.get("/performance").text
    assert client.get("/ui/perf/runs").status_code == 200

    r = client.post("/perf/runs", json={
        "label": "frag-test", "n_jobs": 1, "duration_s": 10, "warmup_s": 0,
        "concurrency": 1, "min_ability_override": 10,
    })
    run_id = r.json()["id"]
    _wait_for_run(client, run_id, redis_url=redis_url, store=app.state.store)

    listing = client.get("/ui/perf/runs")
    assert listing.status_code == 200
    assert "frag-test" in listing.text
    # started_at is UTC (contract); dashboard.js converts it to the viewer's local time
    # client-side, so the fragment must carry the full value for it to read.
    assert 'data-utc="' in listing.text

    detail = client.get(f"/ui/perf/runs/{run_id}")
    assert detail.status_code == 200
    assert "by category" in detail.text


# --- list view is a lightweight SQL aggregate, not full-sample loading (see advisor
# review: the runs list polls every 3s and must not deserialize every sample of every
# run to render it) ---

def test_perf_run_stats_matches_full_summary(app_client, redis_url):
    from clusterbuck.perf_runner import perf_run_list_view, perf_run_view

    app, client = app_client
    r = client.post("/perf/runs", json={
        "label": "list-vs-detail", "n_jobs": 3, "duration_s": 10, "warmup_s": 0,
        "concurrency": 1,
    })
    run_id = r.json()["id"]
    got = _wait_for_run(client, run_id, redis_url=redis_url, store=app.state.store)
    assert got["n_served"] == 3

    store = app.state.store
    row = store.get_perf_run(run_id)
    full = perf_run_view(store, row)
    lite = perf_run_list_view(store, row)

    # The SQL-aggregate list view must agree with the full per-sample summary on every
    # field it computes (it just omits percentiles, which need the sorted sample list).
    for key in ("n_warmup", "n_measured", "n_served", "n_unassigned", "n_failed",
                "pass_rate", "mean_latency_s", "jobs_per_s", "tokens_per_s"):
        assert lite[key] == full[key], key


def test_cancel_stale_perf_runs_reconciles_orphaned_running_row(app_client):
    """A row left 'running' after a crash (SIGKILL — no task ever finalizes it) must not
    make its cancel button a no-op forever; startup reconciles it to 'cancelled'."""
    app, _client = app_client
    store = app.state.store
    store.create_perf_run(
        id="perf_orphan", label="orphan", config="{}", snapshot=None,
        started_at="2020-01-01T00:00:00Z",
    )
    n = store.cancel_stale_perf_runs("2020-01-01T00:05:00Z")
    assert n == 1
    row = store.get_perf_run("perf_orphan")
    assert row.status == "cancelled"
    assert row.finished_at == "2020-01-01T00:05:00Z"
