"""Usage metering (M3a): capture, idempotency, the status-race gate, expiry, headline."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime

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


CLOUD_CAP = "frontier"
FLEET_WITH_CLOUD = Fleet(capabilities={
    CAP: FLEET.capabilities[CAP],
    CLOUD_CAP: CapabilitySpec(queue="q:frontier", model_server="https://api.example.invalid/v1",
                              model="frontier-x", cloud=True,
                              price_in_per_1k=0.003, price_out_per_1k=0.015),
})


async def test_cloud_capability_is_captured_with_cloud_venue_and_real_cost(store, queue):
    """Regression: `venue` used to be hardcoded 'local' for every row, so
    `cloud_spend_in_month` — and therefore any budget gate built on it — was structurally
    always zero, no matter how the capability was configured."""
    store.insert(id="job_cloud", result_key="res_job_cloud", capability=CLOUD_CAP,
                created_at="t")
    await _result(queue, "job_cloud", tin=1000, tout=1000)

    assert await usage_scan(store, queue, FLEET_WITH_CLOUD) == 1
    h = store.usage_headline()
    assert h["local_cost"] == 0.0
    assert abs(h["cloud_cost"] - 0.018) < 1e-9  # 1000/1k*0.003 + 1000/1k*0.015
    assert store.cloud_spend_in_month(datetime.now(UTC).strftime("%Y-%m")) > 0.0


async def test_local_capability_still_captured_as_local_venue(store, queue):
    """The fix must not flip local capabilities to 'cloud' — only ones flagged `cloud`."""
    _job(store, "job_local")
    await _result(queue, "job_local", tin=1000, tout=1000)

    assert await usage_scan(store, queue, FLEET_WITH_CLOUD) == 1
    assert store.cloud_spend_in_month(datetime.now(UTC).strftime("%Y-%m")) == 0.0
    assert store.usage_headline()["local_cost"] > 0.0


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
    # A configured cap is now really enforced (routing.py → budget.py, ADR 30), not just
    # shown — `enforced` reflects that a cap is set, not merely that it's been hit.
    assert s["budget"]["enforced"] is True
    assert s["totals"]["jobs"] == 1


def test_summary_unenforced_when_no_cap_configured(store):
    s = build_usage_summary(store, budget_monthly=None)
    assert s["budget"]["monthly_cap"] is None
    assert s["budget"]["enforced"] is False


def test_usage_endpoint_shape(client):
    # Endpoint wiring; real capture through the running coordinator is proven by
    # deploy/e2e/usage.sh.
    data = client.get("/usage").json()
    assert set(data) == {"headline", "budget", "totals", "by_model", "by_node", "by_day"}
    assert data["totals"]["jobs"] == 0
    assert data["headline"]["currency"] == "USD"
