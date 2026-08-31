"""Usage metering (M3a): capture, idempotency, the status-race gate, expiry, headline."""

from __future__ import annotations

import json
import time

import pytest

from clusterbuck.fleet import CapabilitySpec, Fleet
from clusterbuck.queue import Queue
from clusterbuck.store import Store
from clusterbuck.usage import build_usage_summary, usage_scan

CAP = "8b-extract"
FLEET = Fleet(capabilities={CAP: CapabilitySpec(
    queue="q:8b-extract", model_server="x", model="m",
    price_in_per_1k=0.001, price_out_per_1k=0.002)})


@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(str(tmp_path / "u.db"))


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


def _job(store: Store, jid: str, *, deadline: float | None = None) -> None:
    store.insert(id=jid, result_key=f"res_{jid}", capability=CAP, created_at="t",
                 deadline_epoch=deadline)


async def _result(queue: Queue, jid: str, *, tin=10, tout=20, status="done") -> None:
    await queue.client.set(f"res_{jid}", json.dumps({
        "job_id": jid, "status": status, "worker": "node-x", "completed_at": "t",
        "completion": {"model": "m", "choices": [
            {"message": {"role": "assistant", "content": "x"}}]},
        "usage": {"prompt_tokens": tin, "completion_tokens": tout,
                  "total_tokens": tin + tout},
    }))


async def test_captures_completed_job_with_cost(store, queue):
    _job(store, "job_a")
    await _result(queue, "job_a", tin=1000, tout=1000)

    assert await usage_scan(store, queue, FLEET) == 1
    h = store.usage_headline()
    assert h["jobs"] == 1
    assert h["tokens_in"] == 1000 and h["tokens_out"] == 1000
    assert abs(h["local_cost"] - 0.003) < 1e-9  # 1000/1k*0.001 + 1000/1k*0.002


async def test_capture_is_idempotent(store, queue):
    _job(store, "job_b")
    await _result(queue, "job_b")
    assert await usage_scan(store, queue, FLEET) == 1
    assert await usage_scan(store, queue, FLEET) == 0  # already captured
    assert store.usage_headline()["jobs"] == 1


async def test_capture_gated_on_usage_not_status(store, queue):
    # A client's GET may have flipped status to done before the scan; capture must still
    # happen because it's gated on the usage row, not the job status.
    _job(store, "job_c")
    await _result(queue, "job_c")
    store.set_status("job_c", "done")
    assert await usage_scan(store, queue, FLEET) == 1


async def test_past_deadline_expires_and_records(store, queue):
    _job(store, "job_d", deadline=100.0)  # no result blob
    assert await usage_scan(store, queue, FLEET, now=200.0) == 1
    assert store.get("job_d").status == "expired"
    assert store.usage_headline()["jobs"] == 1


async def test_no_deadline_no_result_not_captured(store, queue):
    _job(store, "job_e")
    assert await usage_scan(store, queue, FLEET, now=200.0) == 0
    assert store.usage_headline()["jobs"] == 0


def test_summary_headline_and_budget(store):
    store.record_usage(job_id="j1", ts="t", capability=CAP, model="m", node="n",
                       venue="local", tokens_in=1000, tokens_out=0, outcome="done",
                       cost=0.001, day=time.strftime("%Y-%m-%d"))
    s = build_usage_summary(store, budget_monthly=10.0)
    assert s["headline"]["avoided_cloud_spend"] == 0.001
    assert s["headline"]["cloud_spend"] == 0.0
    assert s["headline"]["net_avoided"] == 0.001
    assert s["budget"]["monthly_cap"] == 10.0
    assert s["budget"]["cloud_spent_this_month"] == 0.0
    assert s["budget"]["enforced"] is False
    assert s["totals"]["jobs"] == 1


def test_usage_endpoint_shape(client):
    # Endpoint wiring; real capture through the running coordinator is proven by
    # deploy/e2e/usage.sh.
    data = client.get("/usage").json()
    assert set(data) == {"headline", "budget", "totals", "by_model", "by_node", "by_day"}
    assert data["totals"]["jobs"] == 0
    assert data["headline"]["currency"] == "USD"
