"""Queue stats (queue.py) — the four numbers that were routinely conflated.

`depth` is XLEN (retained history, acked entries included) and `pending` is the
pending-entries count (already claimed, in flight). Neither counts work that no worker has
ever been handed, so a queue full of permanently stuck jobs read as healthy on those two
alone. `backlog` is that missing number, and `queue_position` is a client's own slice of it.
"""

from __future__ import annotations

import pytest

from clusterbuck.queue import (
    CLOUD_EXECUTOR_CONSUMER,
    REAPER_CONSUMER,
    Queue,
    live_worker_consumers,
    stream_key,
)

CAP = "8b-extract"
GROUP = "cbk-workers"


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


async def _submit(queue: Queue, job_id: str) -> str:
    return await queue.enqueue({
        "id": job_id, "created_at": "t", "capability": CAP, "prompt": "work",
        "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": f"res_{job_id}", "attempts": 0, "max_attempts": 3,
    })


# --- the number that was missing ---------------------------------------------------


async def test_backlog_counts_never_delivered_work(queue):
    for n in range(3):
        await _submit(queue, f"job_{n}")

    stats = await queue.depth(CAP, GROUP)
    assert stats["backlog"] == 3
    assert stats["pending"] == 0, "nothing claimed yet"
    assert stats["depth"] == 3


async def test_a_stuck_queue_does_not_read_as_healthy(queue):
    """The exact failure the old rendering hid: work queued, nothing running.

    `pending` is 0 because nobody has claimed anything, and `depth` is indistinguishable
    from retained history — so leading with either reported a green, healthy queue.
    """
    for n in range(3):
        await _submit(queue, f"job_stuck_{n}")

    stats = await queue.depth(CAP, GROUP)
    assert stats["pending"] == 0 and stats["consumers"] == 0
    assert stats["backlog"] == 3, "the only number that shows the queue is stuck"


async def test_claiming_moves_work_from_backlog_to_pending(queue):
    for n in range(3):
        await _submit(queue, f"job_mv_{n}")
    await queue.client.xreadgroup(GROUP, "node-alpha", {stream_key(CAP): ">"}, count=1)

    stats = await queue.depth(CAP, GROUP)
    assert stats["pending"] == 1, "in flight"
    assert stats["backlog"] == 2, "still untouched"
    assert stats["depth"] == 3, "XLEN is unchanged by claiming — it is not a backlog"


async def test_acking_leaves_depth_alone(queue):
    """XACK does not remove a stream entry, which is why `depth` cannot be a backlog."""
    entry = await _submit(queue, "job_ack")
    await queue.client.xreadgroup(GROUP, "node-alpha", {stream_key(CAP): ">"}, count=1)
    await queue.ack(CAP, GROUP, entry)

    stats = await queue.depth(CAP, GROUP)
    assert stats["depth"] == 1
    assert stats["pending"] == 0 and stats["backlog"] == 0


# --- position ----------------------------------------------------------------------


async def test_queue_position_counts_only_work_ahead(queue):
    first = await _submit(queue, "job_p0")
    second = await _submit(queue, "job_p1")
    third = await _submit(queue, "job_p2")

    assert await queue.undelivered(CAP, GROUP, before_entry_id=first) == (0, False)
    assert await queue.undelivered(CAP, GROUP, before_entry_id=second) == (1, False)
    assert await queue.undelivered(CAP, GROUP, before_entry_id=third) == (2, False)


async def test_position_of_a_delivered_entry_is_not_zero_ahead(queue):
    """An entry at or below last-delivered-id has been handed out, so it has no position.

    Decided by comparing ids: an inverted XRANGE also returns [], which would render as
    "nobody ahead of you" for a job that is already running.
    """
    first = await _submit(queue, "job_d0")
    await _submit(queue, "job_d1")
    await queue.client.xreadgroup(GROUP, "node-alpha", {stream_key(CAP): ">"}, count=1)

    # Claimed, so it is in flight rather than queued behind anything.
    assert await queue.undelivered(CAP, GROUP, before_entry_id=first) == (0, False)


async def test_position_is_capped_and_says_so(queue):
    entries = [await _submit(queue, f"job_c{n}") for n in range(6)]
    count, capped = await queue.undelivered(
        CAP, GROUP, before_entry_id=entries[-1], count=3)
    assert (count, capped) == (3, True), "past the cap the answer means 'at least 3'"


async def test_no_group_yet_is_not_an_error(queue):
    assert await queue.undelivered("never-used", GROUP) == (0, False)
    assert await queue.group_info("never-used", GROUP) is None
    assert await queue.claims("never-used", GROUP) == []


# --- who counts as a worker --------------------------------------------------------


def test_dead_consumers_are_not_live():
    """Nothing ever calls XGROUP DELCONSUMER, so a worker that died months ago is still
    listed. Counting it reported a fleet that was not there."""
    consumers = [
        {"name": "node-alive", "idle": 500},
        {"name": "node-gone", "idle": 90_000},
    ]
    live = live_worker_consumers(consumers, dead_ms=60_000)
    assert [c["name"] for c in live] == ["node-alive"]


def test_the_reaper_is_never_counted_as_a_worker():
    """`cbk-reaper` refreshes its own idle time on every scan (~60s, i.e. inside
    dead_ms), so counting it made the coordinator's own bookkeeping look like a live
    worker — inflating the consumer count and suppressing wakes."""
    consumers = [{"name": REAPER_CONSUMER, "idle": 100}]
    assert live_worker_consumers(consumers, dead_ms=60_000) == []
    assert live_worker_consumers(
        consumers, dead_ms=60_000, include=(CLOUD_EXECUTOR_CONSUMER,)) == []


def test_the_cloud_executor_counts_only_when_asked():
    """It is the only consumer a cloud-only capability will ever have, so it must not be
    blanket-excluded — but it is not a machine either, so callers opt in."""
    consumers = [{"name": CLOUD_EXECUTOR_CONSUMER, "idle": 100}]
    assert live_worker_consumers(consumers, dead_ms=60_000) == []
    assert len(live_worker_consumers(
        consumers, dead_ms=60_000, include=(CLOUD_EXECUTOR_CONSUMER,))) == 1


def test_a_consumer_with_no_idle_field_is_treated_as_dead():
    assert live_worker_consumers([{"name": "odd"}], dead_ms=60_000) == []
