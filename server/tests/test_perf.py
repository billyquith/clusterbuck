"""Load-test driver (Performance page): category generators, run lifecycle, dashboard
fragments. Integration tests answer jobs directly via Redis (a fake worker), mirroring
test_api.py's `test_poll_queued_then_done` pattern, rather than running a real worker.
"""

from __future__ import annotations

import json
import random
import re
import time

import pytest
import redis

from clusterbuck.perf_runner import CATEGORIES

# --- category generators: pure unit tests, no fixtures ---

@pytest.mark.parametrize("name", sorted(CATEGORIES))
def test_category_shape_and_negative_case(name):
    """Every category, across many seeds, yields a well-shaped query whose check rejects
    an obviously-wrong reply (proves the check isn't trivially always-true)."""
    gen = CATEGORIES[name]
    for seed in range(20):
        gq = gen(random.Random(seed))
        assert gq.category == name
        assert gq.task_class in {"extract", "summarize", "reason", "code"}
        assert 1 <= gq.min_ability <= 10
        assert gq.prompt and isinstance(gq.prompt, str)
        ok, detail = gq.check("completely unrelated filler text")
        assert ok is False
        assert isinstance(detail, str) and detail


def test_extract_ticket_positive_case():
    gq = CATEGORIES["extract-ticket"](random.Random(1))
    order_id = re.search(r"Order #(\d+)", gq.prompt).group(1)
    amount = re.search(r"\$([\d.]+)", gq.prompt).group(1)
    reply = json.dumps({"order_id": order_id, "refund_amount": amount, "deadline": "x"})
    assert gq.check(reply) == (True, "ok")


def test_extract_log_positive_case():
    gq = CATEGORIES["extract-log"](random.Random(2))
    code = re.search(r"ERROR (\w+) retrying", gq.prompt).group(1)
    ok, _ = gq.check(json.dumps({"timestamp": "t", "error_code": code}))
    assert ok is True


def test_extract_emails_positive_case():
    gq = CATEGORIES["extract-emails"](random.Random(3))
    emails = re.findall(r"[\w.]+@[\w.]+\.\w+", gq.prompt)
    assert emails
    ok, _ = gq.check(json.dumps(emails))
    assert ok is True
    ok, _ = gq.check(json.dumps(emails[:-1]))  # missing one address
    assert ok is False


def test_summarize_positive_case():
    gq = CATEGORIES["summarize"](random.Random(4))
    codename = re.search(r"project (\w+):", gq.prompt).group(1)
    ok, _ = gq.check(f"The {codename} release fixes bugs and adds a sync feature.")
    assert ok is True


def test_reason_arithmetic_positive_case():
    # Both branches: force each by patching random.random via a controlled seed search.
    seen_branches = set()
    for seed in range(50):
        rng = random.Random(seed)
        first_draw = rng.random()
        gq = CATEGORIES["reason-arithmetic"](random.Random(seed))
        if first_draw < 0.5:
            m = re.search(r"^A train.*?(\d+) hour\(s\) later", gq.prompt)
            answer = m.group(1)
            seen_branches.add("catchup")
        else:
            total = int(re.search(r"shipment of (\d+) crates", gq.prompt).group(1))
            delta = int(re.search(r"received (\d+) more crates", gq.prompt).group(1))
            b = (total - delta) // 4
            answer = str(b)
            seen_branches.add("split")
        ok, _ = gq.check(f"The answer is {answer}.")
        assert ok is True
    assert seen_branches == {"catchup", "split"}  # both branches actually exercised


# --- integration: real HTTP + SQLite, jobs answered directly via Redis ---

@pytest.fixture()
def app_client(redis_url, tmp_path):
    from fastapi.testclient import TestClient

    from clusterbuck.api import create_app

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
        "label": "served", "n_jobs": 3, "duration_s": 10, "warmup_s": 0, "concurrency": 1,
    })
    assert r.status_code == 201
    run_id = r.json()["id"]
    got = _wait_for_run(client, run_id, redis_url=redis_url, store=app.state.store)

    assert got["status"] == "done"
    assert got["n_measured"] == 3
    assert got["n_served"] == 3
    assert got["n_unassigned"] == 0
    # A generic canned reply matches none of the categories' checks — real discrimination,
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


def test_ui_start_form_creates_a_run(app_client):
    """The htmx form posts application/x-www-form-urlencoded, not JSON — a real gap the
    JSON-API tests above don't exercise (this caught a missing python-multipart dep)."""
    _app, client = app_client
    r = client.post("/ui/perf/start", data={
        "label": "from-the-form", "categories": ["extract-ticket"],
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
