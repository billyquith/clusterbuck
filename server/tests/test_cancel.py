"""Cancel (protocols.md §1b) — withdrawing a job, and being honest about how far it went.

A client that gives up (a killed request, a person walking away) previously left its job
on the fleet: it ran to completion and the result was collected by nobody. That is real
wasted capacity on a small fleet.

What cancel can and cannot do is asymmetric, and these tests pin both halves. Unclaimed
work is genuinely withdrawn. Work a worker already holds cannot be interrupted — no
coordinator-side bookkeeping stops a model call in flight — so the verdict is `cancelling`
and the run finishes with its result discarded. The one thing always delivered is that the
client stops waiting.
"""

from __future__ import annotations

import json

import pytest
import redis

from clusterbuck.queue import Queue, stream_key
from clusterbuck.store import Store
from clusterbuck.usage import usage_scan

CAP = "8b-extract"
GROUP = "cbk-workers"


def _submit(client, **overrides):
    body = {
        "capability": CAP,
        "messages": [{"role": "user", "content": "hello"}],
        "urgency": "waitable",
        "privacy": "local_only",
    }
    body.update(overrides)
    return client.post("/jobs", json=body)


def _claim(redis_url: str, consumer: str = "node-alpha") -> None:
    conn = redis.from_url(redis_url, decode_responses=True)
    try:
        conn.xreadgroup(GROUP, consumer, {stream_key(CAP): ">"}, count=10)
    finally:
        conn.close()


def _stream_len(redis_url: str) -> int:
    conn = redis.from_url(redis_url, decode_responses=True)
    try:
        return len(conn.xrange(stream_key(CAP)))
    finally:
        conn.close()


# --- the provable case --------------------------------------------------------------


def test_an_unclaimed_job_is_provably_cancelled(client, redis_url):
    job_id = _submit(client).json()["id"]
    assert _stream_len(redis_url) == 1

    got = client.delete(f"/jobs/{job_id}").json()
    assert got["status"] == "cancelled", "nobody had it, so this is not a guess"
    assert _stream_len(redis_url) == 0, "the entry is gone: no worker can pick it up"
    assert client.get(f"/jobs/{job_id}").json()["status"] == "cancelled"


def test_a_cancelled_job_is_metered_so_it_stops_being_rescanned(client, redis_url):
    """The leak this must not reintroduce.

    `jobs_awaiting_usage` selects jobs with no usage row, and the metering scan skips a
    job with no result blob and no past deadline. A terminal status alone would therefore
    be re-selected on **every coordinator tick, forever**. Writing both halves is what
    ends it — asserted by scanning twice and requiring the second to capture nothing.
    """
    job_id = _submit(client).json()["id"]
    client.delete(f"/jobs/{job_id}")

    store = client.app.state.store
    rows = [r for r in store.recent_usage(limit=10) if r.job_id == job_id]
    assert rows and rows[0].outcome == "cancelled"
    assert rows[0].cost == 0.0

    assert job_id not in [j.id for j in store.jobs_awaiting_usage()]


def test_cancelling_a_finished_job_changes_nothing(client, redis_url):
    job_id = _submit(client).json()["id"]
    conn = redis.from_url(redis_url, decode_responses=True)
    conn.set(f"res_{job_id[4:]}", json.dumps({
        "job_id": job_id, "status": "done", "worker": "node-a",
        "completed_at": "2026-09-02T00:00:00Z",
        "completion": {"id": "c1", "choices": []},
    }))
    conn.close()

    got = client.delete(f"/jobs/{job_id}").json()
    assert got["status"] == "done", "the result is the first gate, not the stream"


def test_cancel_is_idempotent(client):
    job_id = _submit(client).json()["id"]
    assert client.delete(f"/jobs/{job_id}").json()["status"] == "cancelled"
    assert client.delete(f"/jobs/{job_id}").json()["status"] == "cancelled"


def test_unknown_job_is_404(client):
    assert client.delete("/jobs/job_nope").status_code == 404


# --- the honest case ----------------------------------------------------------------


def test_a_claimed_job_reports_cancelling_not_cancelled(client, redis_url):
    """A worker is already generating. We will not claim to have stopped it."""
    job_id = _submit(client).json()["id"]
    _claim(redis_url)

    got = client.delete(f"/jobs/{job_id}").json()
    assert got["status"] == "cancelling", "honest: the run is out of our hands"


def test_a_cancelling_job_is_terminalised_even_if_the_worker_vanishes(
    client, redis_url
):
    """The subtle leak behind `cancel_requested`.

    Since Redis 7, `XAUTOCLAIM` DROPS pending entries whose stream entry no longer
    exists. Withdrawing a claimed entry and then losing that worker therefore leaves a
    job the reaper cannot see, with no blob and no terminal status — re-scanned forever.
    Flooring the deadline hands it to the expiry sweep instead, which lands it as
    `cancelled` rather than `expired` so the metering vocabulary stays honest.
    """
    job_id = _submit(client).json()["id"]
    _claim(redis_url)
    assert client.delete(f"/jobs/{job_id}").json()["status"] == "cancelling"

    store = client.app.state.store
    assert store.get(job_id).cancel_requested == 1
    assert store.get(job_id).deadline_epoch is not None, "deadline floored to now"

    import anyio
    queue = Queue.from_url(redis_url)

    async def sweep():
        try:
            await usage_scan(store, queue, None)
        finally:
            await queue.aclose()

    anyio.run(sweep)

    assert store.get(job_id).status == "cancelled"
    rows = [r for r in store.recent_usage(limit=10) if r.job_id == job_id]
    assert rows and rows[0].outcome == "cancelled", "not 'expired'"


def test_a_worker_that_finishes_first_still_wins(client, redis_url):
    """The floor must not eagerly terminalise: if the run completes, its result is the
    truth and the job is `done`, not `cancelled`."""
    job_id = _submit(client).json()["id"]
    _claim(redis_url)
    client.delete(f"/jobs/{job_id}")

    conn = redis.from_url(redis_url, decode_responses=True)
    conn.set(f"res_{job_id[4:]}", json.dumps({
        "job_id": job_id, "status": "done", "worker": "node-alpha",
        "completed_at": "2026-09-02T00:00:00Z",
        "completion": {"id": "c1", "choices": []},
    }))
    conn.close()

    assert client.get(f"/jobs/{job_id}").json()["status"] == "done"


# --- a cancelled job must not stir the fleet ----------------------------------------


def test_a_cancelled_job_never_escalates(tmp_path) -> None:
    """Escalation filtered on urgency, not status, so a cancelled job with a due
    `escalate_at` would still promote — and promotion may WAKE A PHYSICAL MACHINE. The
    worst available outcome in a cold-by-default fleet, for work nobody wants."""
    store = Store(str(tmp_path / "esc.db"))
    store.insert(id="job_c", result_key="res_c", capability=CAP, created_at="t",
                 urgency="waitable", escalate_at=1.0)
    store.set_status("job_c", "cancelled")

    assert [j.id for j in store.due_for_escalation(now=2.0)] == []


# --- the queue primitive ------------------------------------------------------------


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


async def test_withdraw_distinguishes_unclaimed_from_claimed(queue):
    await queue.ensure_group(CAP)
    entry = await queue.enqueue({
        "id": "job_w", "created_at": "t", "capability": CAP, "prompt": "x",
        "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": "res_w", "attempts": 0, "max_attempts": 3,
    })
    assert await queue.withdraw(stream_key(CAP), entry, group=GROUP) == "deleted"


async def test_withdraw_reports_claimed_when_a_consumer_holds_it(queue):
    await queue.ensure_group(CAP)
    entry = await queue.enqueue({
        "id": "job_h", "created_at": "t", "capability": CAP, "prompt": "x",
        "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": "res_h", "attempts": 0, "max_attempts": 3,
    })
    await queue.client.xreadgroup(GROUP, "node-alpha", {stream_key(CAP): ">"}, count=1)
    assert await queue.withdraw(stream_key(CAP), entry, group=GROUP) == "claimed"


async def test_withdraw_reports_gone_for_an_entry_that_is_not_there(queue):
    await queue.ensure_group(CAP)
    assert await queue.withdraw(stream_key(CAP), "1-1", group=GROUP) == "gone"
