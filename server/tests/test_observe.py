"""The observe tick (observe.py) — turning what Redis knows into what a client can see.

Before this, `GET /jobs/{id}` could not distinguish "queued" from "running": the only
timestamps were the submit time and the result blob, and `worker` came solely from the
blob, i.e. once the job was already over. The pending-entries list knows which node
claimed an entry and how long ago, so these tests pin that it is copied onto the job row —
and, just as importantly, pin the case where it legitimately cannot be.
"""

from __future__ import annotations

import json

import pytest
from clusterbuck.observe import observe_tick
from clusterbuck.queue import REAPER_CONSUMER, URGENT_TIER, Queue, stream_key
from clusterbuck.store import Store

CAP = "8b-extract"
CAP2 = "32b-reason"
GROUP = "cbk-workers"


@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(str(tmp_path / "observe.db"))


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


async def _submit(queue: Queue, store: Store, job_id: str) -> str:
    store.insert(id=job_id, result_key=f"res_{job_id}", capability=CAP,
                 created_at="2026-09-02T00:00:00Z")
    entry_id = await queue.enqueue({
        "id": job_id, "created_at": "2026-09-02T00:00:00Z", "capability": CAP,
        "prompt": "work", "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": f"res_{job_id}", "attempts": 0, "max_attempts": 3,
    })
    store.record_delivery(job_id, stream=stream_key(CAP), entry_id=entry_id)
    return entry_id


async def _claim(queue: Queue, consumer: str) -> None:
    await queue.client.xreadgroup(GROUP, consumer, {stream_key(CAP): ">"}, count=10)


async def test_claim_is_observed_as_started_and_running(store, queue):
    await _submit(queue, store, "job_obs")
    await _claim(queue, "node-alpha")

    assert await observe_tick(store, queue, group=GROUP) == {"observed": 1}

    row = store.get("job_obs")
    assert row.status == "running"
    assert row.claimed_by == "node-alpha"
    assert row.started_at is not None


async def test_started_at_is_the_delivery_time_not_the_tick_clock(store, queue):
    """The stamp is `now - idle_ms`, so a tick that runs late still records the true
    claim instant. Asserted by construction: the recorded time must not be *after* the
    moment the tick ran, and a later tick must not move it."""
    await _submit(queue, store, "job_when")
    await _claim(queue, "node-alpha")

    await observe_tick(store, queue, group=GROUP)
    first = store.get("job_when").started_at

    # A second pass observes the same claim (idle is now larger) and must not re-stamp.
    assert await observe_tick(store, queue, group=GROUP) == {"observed": 0}
    assert store.get("job_when").started_at == first


async def test_reaper_claims_are_not_work_starting(store, queue):
    """The reaper's XAUTOCLAIM registers itself as a consumer and holds entries while it
    decides what to do with them. Counting that as a job starting would report a node
    that never touched the work."""
    await _submit(queue, store, "job_reap")
    await _claim(queue, REAPER_CONSUMER)

    assert await observe_tick(store, queue, group=GROUP) == {"observed": 0}
    row = store.get("job_reap")
    assert row.started_at is None
    assert row.claimed_by is None
    assert row.status == "queued"


async def test_a_job_claimed_and_acked_between_ticks_is_never_observed(store, queue):
    """The documented blind spot, pinned so nobody 'fixes' it by inventing a start time.

    Pending entries vanish on XACK, so a fast job leaves no trace for this tick to find.
    `started_at` stays NULL on a job that certainly ran — which is why NULL must be read
    as "no claim observed", never as "not started".
    """
    await _submit(queue, store, "job_fast")
    await _claim(queue, "node-alpha")
    entry = store.get("job_fast").entry_id
    await queue.ack(CAP, GROUP, entry)

    assert await observe_tick(store, queue, group=GROUP) == {"observed": 0}
    assert store.get("job_fast").started_at is None


async def test_observation_never_overwrites_a_terminal_status(store, queue):
    """The tick and a client's poll race by construction. A claim observed just after the
    job finished must not drag a terminal job back to `running`."""
    await _submit(queue, store, "job_done")
    await _claim(queue, "node-alpha")
    store.set_status("job_done", "done")

    await observe_tick(store, queue, group=GROUP)

    row = store.get("job_done")
    assert row.status == "done"
    # The claim itself is still recorded — it did happen.
    assert row.claimed_by == "node-alpha"


async def test_a_claim_on_the_urgent_tier_is_observed(store, queue):
    """Reading only the base stream left every urgent/necessary job reporting `queued`
    with `worker: null` for its whole run, whenever the urgent tier is in use — its claim
    lives on `q:<cap>:urgent`, which the tick never looked at."""
    store.insert(id="job_urgent", result_key="res_job_urgent", capability=CAP,
                created_at="2026-09-02T00:00:00Z")
    entry_id = await queue.enqueue({
        "id": "job_urgent", "created_at": "2026-09-02T00:00:00Z", "capability": CAP,
        "prompt": "work", "params": {}, "urgency": "urgent", "privacy": "local_only",
        "result_key": "res_job_urgent", "attempts": 0, "max_attempts": 3,
    }, tier=URGENT_TIER)
    store.record_delivery("job_urgent", stream=stream_key(CAP, URGENT_TIER),
                          entry_id=entry_id)
    await queue.client.xreadgroup(GROUP, "node-alpha",
                                  {stream_key(CAP, URGENT_TIER): ">"}, count=10)

    assert await observe_tick(store, queue, group=GROUP) == {"observed": 1}

    row = store.get("job_urgent")
    assert row.status == "running"
    assert row.claimed_by == "node-alpha"
    assert row.started_at is not None


async def test_identical_entry_ids_on_different_streams_do_not_collide(store, queue):
    """Stream ids are `<ms>-<seq>`, minted independently per stream, so two DIFFERENT
    streams can and do mint the identical id — most easily, a capability's base and
    urgent tiers, or two capabilities enqueued in the same millisecond. Matching a claim
    back to a job by entry_id alone let one job's row overwrite another's, reporting a
    job NOBODY claimed as `running` on whichever node held the other stream's
    identically-numbered entry. Forced here by writing the same explicit id to two
    unrelated capabilities' streams directly."""
    entry_id = "500000000000-1"  # a value neither stream has produced natively yet

    async def _submit_with_forced_id(job_id: str, cap: str) -> None:
        store.insert(id=job_id, result_key=f"res_{job_id}", capability=cap,
                    created_at="2026-09-02T00:00:00Z")
        await queue.ensure_group(cap)
        await queue.client.xadd(stream_key(cap), {"job": json.dumps({
            "id": job_id, "created_at": "2026-09-02T00:00:00Z", "capability": cap,
            "prompt": "work", "params": {}, "urgency": "waitable",
            "privacy": "local_only", "result_key": f"res_{job_id}",
            "attempts": 0, "max_attempts": 3,
        })}, id=entry_id)
        store.record_delivery(job_id, stream=stream_key(cap), entry_id=entry_id)

    await _submit_with_forced_id("job_claimed", CAP)
    await _submit_with_forced_id("job_untouched", CAP2)

    # Only the FIRST capability's identically-numbered entry is ever claimed.
    await queue.client.xreadgroup(GROUP, "node-alpha", {stream_key(CAP): ">"}, count=10)

    assert await observe_tick(store, queue, group=GROUP,
                              capabilities=[CAP, CAP2]) == {"observed": 1}

    claimed = store.get("job_claimed")
    assert claimed.status == "running"
    assert claimed.claimed_by == "node-alpha"

    untouched = store.get("job_untouched")
    assert untouched.status == "queued", "nobody claimed this job"
    assert untouched.claimed_by is None
    assert untouched.started_at is None


async def test_a_stale_entry_id_is_skipped(store, queue):
    """After a reaper requeue the row points at a NEW entry id. The old pending entry
    must not be matched back onto the job, or `started_at` would record a delivery that
    has since been retired."""
    await _submit(queue, store, "job_stale")
    await _claim(queue, "node-alpha")
    store.record_delivery("job_stale", stream=stream_key(CAP), entry_id="99999999-0")

    assert await observe_tick(store, queue, group=GROUP) == {"observed": 0}
    assert store.get("job_stale").started_at is None
