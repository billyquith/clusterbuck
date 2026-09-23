"""Terminating backstops (backstop.py) — nothing waits for an answer that never comes.

The gap these close: a `waitable` job with no `deadline` and no `escalate_after_min` was
excluded from escalation, excluded from the deadline sweep, and invisible to the reaper
(`XAUTOCLAIM` walks the pending list, and an entry nobody claimed never enters it). Its
only exit was `MAXLEN ~` trimming it away with no result and no status change.
"""

from __future__ import annotations

import pytest

from clusterbuck.backstop import COORDINATOR, backstop_scan
from clusterbuck.queue import Queue, stream_key
from clusterbuck.store import Store

CAP = "8b-extract"
GROUP = "cbk-workers"
LONG_AGO = "2020-01-01T00:00:00Z"
JUST_NOW = "2099-01-01T00:00:00Z"


def _thresholds(*, max_queue_age_s: int | None = None) -> dict:
    """Backstop thresholds. Grace is 0 here so "older than the grace window" is simply
    "not in the future" — the tests use fixed far-past and far-future timestamps rather
    than sleeping."""
    return {"orphan_grace_s": 0, "max_queue_age_s": max_queue_age_s, "group": GROUP}


async def _really_enqueue(store, queue, job_id: str) -> str:
    """Put the job on the stream for real and record where it landed.

    Tests used to fake this with `record_delivery(..., entry_id="1-1")`, which asserts a
    delivery that does not exist. That was invisible while the orphan sweep selected on
    `entry_id IS NULL` alone — a fabricated id was enough to be skipped. Now that the
    sweep verifies the entry is actually on the stream, a fake id reads (correctly) as a
    job whose entry has vanished, so these fixtures have to be real.
    """
    entry_id = await queue.enqueue({"id": job_id, "capability": CAP,
                                    "result_key": f"res_{job_id}"})
    store.record_delivery(job_id, stream=stream_key(CAP), entry_id=entry_id)
    return entry_id


@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(str(tmp_path / "backstop.db"))


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


# --- the orphan sweep: always on ----------------------------------------------------


async def test_a_job_that_was_never_enqueued_is_failed(store, queue):
    """The coordinator committed the row and died before the XADD. No stream entry
    exists, so no worker will ever see it and the reaper cannot find it — before this it
    stayed `queued` forever."""
    store.insert(id="job_orphan", result_key="res_orphan", capability=CAP,
                 created_at=LONG_AGO)

    counts = await backstop_scan(store, queue, None, **_thresholds())
    assert counts["orphaned"] == 1

    assert store.get("job_orphan").status == "failed"
    result = await queue.read_result("res_orphan")
    assert result["status"] == "failed"
    assert result["worker"] == COORDINATOR
    assert "never enqueued" in result["error"]


async def test_an_orphan_is_answered_not_resubmitted(store, queue):
    """It cannot be re-enqueued: `messages`/`prompt`/`params` are not columns, so the
    payload cannot be rebuilt from SQLite. Inventing one would run something the client
    never asked for; failing tells a client retrying under an idempotency key to use a
    fresh key."""
    store.insert(id="job_o2", result_key="res_o2", capability=CAP, created_at=LONG_AGO)
    await backstop_scan(store, queue, None, **_thresholds())

    assert await queue.client.xlen(stream_key(CAP)) == 0, "nothing was queued"


async def test_a_recent_orphan_is_left_alone(store, queue):
    """The grace window exists so a job mid-submit is not shot in the back."""
    store.insert(id="job_new", result_key="res_new", capability=CAP,
                 created_at=JUST_NOW)
    assert (await backstop_scan(store, queue, None, **_thresholds()))["orphaned"] == 0
    assert store.get("job_new").status == "queued"


async def test_a_properly_enqueued_job_is_not_an_orphan(store, queue):
    """A job whose entry is really on the stream is somebody's to run, however old it is
    — that is the opt-in backstop's business, not the orphan sweep's."""
    store.insert(id="job_live", result_key="res_live", capability=CAP,
                 created_at=LONG_AGO)
    await _really_enqueue(store, queue, "job_live")

    assert (await backstop_scan(store, queue, None, **_thresholds()))["orphaned"] == 0
    assert store.get("job_live").status == "queued"


async def test_the_sweep_does_not_rescan_what_it_already_answered(store, queue):
    """The leak every terminal path has to avoid: `jobs_awaiting_usage` selects on the
    absence of a usage row, so a status alone would be re-selected every tick forever."""
    store.insert(id="job_once", result_key="res_once", capability=CAP,
                 created_at=LONG_AGO)
    assert (await backstop_scan(store, queue, None, **_thresholds()))["orphaned"] == 1
    assert (await backstop_scan(store, queue, None, **_thresholds()))["orphaned"] == 0
    assert "job_once" not in [j.id for j in store.jobs_awaiting_usage()]


# --- maximum queue age: opt-in ------------------------------------------------------


async def test_max_queue_age_is_off_by_default(store, queue):
    """Deliberate, and stated to the client rather than hidden: on a fleet whose nodes
    sleep for days, a patient job outliving a fixed cutoff is correct. So with this
    unset, the only bounds are the ones the client set."""
    from clusterbuck.config import settings as live
    assert live.max_queue_age_s is None, "the shipped default must stay off"
    store.insert(id="job_patient", result_key="res_patient", capability=CAP,
                 created_at=LONG_AGO)
    await _really_enqueue(store, queue, "job_patient")

    assert (await backstop_scan(store, queue, None, **_thresholds()))["expired"] == 0
    assert store.get("job_patient").status == "queued"


async def test_max_queue_age_expires_an_unclaimed_job_when_configured(store, queue):
    entry = await queue.enqueue({
        "id": "job_aged", "created_at": LONG_AGO, "capability": CAP, "prompt": "x",
        "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": "res_aged", "attempts": 0, "max_attempts": 3,
    })
    store.insert(id="job_aged", result_key="res_aged", capability=CAP,
                 created_at=LONG_AGO)
    store.record_delivery("job_aged", stream=stream_key(CAP), entry_id=entry)

    assert (await backstop_scan(
        store, queue, None, **_thresholds(max_queue_age_s=60)))["expired"] == 1
    assert store.get("job_aged").status == "expired"
    result = await queue.read_result("res_aged")
    assert result["status"] == "expired" and "maximum queue age" in result["error"]


async def test_the_entry_is_withdrawn_so_it_cannot_run_after_being_failed(
    store, queue
):
    """Order matters: withdraw first, then answer. Otherwise a worker could pick the job
    up and run it *after* the client was told it expired."""
    await queue.ensure_group(CAP)
    entry = await queue.enqueue({
        "id": "job_w", "created_at": LONG_AGO, "capability": CAP, "prompt": "x",
        "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": "res_w", "attempts": 0, "max_attempts": 3,
    })
    store.insert(id="job_w", result_key="res_w", capability=CAP, created_at=LONG_AGO)
    store.record_delivery("job_w", stream=stream_key(CAP), entry_id=entry)

    await backstop_scan(store, queue, None, **_thresholds(max_queue_age_s=60))

    assert await queue.client.xlen(stream_key(CAP)) == 0, "entry withdrawn"
    assert await queue.read_one(CAP, GROUP, "late-worker") is None


async def test_an_already_terminal_job_is_left_alone(store, queue):
    store.insert(id="job_done", result_key="res_done", capability=CAP,
                 created_at=LONG_AGO)
    store.set_status("job_done", "done")

    counts = await backstop_scan(
        store, queue, None, **_thresholds(max_queue_age_s=60))
    assert counts == {"orphaned": 0, "failed": 0, "expired": 0, "adopted": 0}
    assert store.get("job_done").status == "done"


async def test_an_in_flight_job_with_no_recorded_delivery_is_adopted(store, queue):
    """The deploy hazard this sweep must not become.

    Migration leaves every non-terminal row with `entry_id IS NULL`, because the column is
    new — so "no delivery recorded" describes an in-flight job on an upgraded coordinator
    just as well as a genuine orphan. Failing on that alone would mean the act of deploying
    destroyed live work. The entry is looked for first, and adopted when found.
    """
    entry = await queue.enqueue({
        "id": "job_inflight", "created_at": LONG_AGO, "capability": CAP, "prompt": "x",
        "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": "res_inflight", "attempts": 0, "max_attempts": 3,
    })
    # Exactly the post-migration shape: the job is queued and real, but SQLite has no
    # record of which entry is its own.
    store.insert(id="job_inflight", result_key="res_inflight", capability=CAP,
                 created_at=LONG_AGO)

    counts = await backstop_scan(store, queue, None, **_thresholds())

    assert counts["adopted"] == 1 and counts["orphaned"] == 0
    row = store.get("job_inflight")
    assert row.status == "queued", "still someone's to run"
    assert row.entry_id == entry, "and now we know which entry is its own"
    assert row.stream == stream_key(CAP)
    assert await queue.read_result("res_inflight") is None, "no terminal answer written"


async def test_a_job_whose_entry_was_trimmed_away_is_still_failed(store, queue):
    """The other half: no entry anywhere means the job genuinely cannot be served, whether
    it was never enqueued or its entry was trimmed unserved. The error says both, because
    from here the two are indistinguishable — and the outcome is the same either way."""
    store.insert(id="job_lost", result_key="res_lost", capability=CAP,
                 created_at=LONG_AGO)

    counts = await backstop_scan(store, queue, None, **_thresholds())

    assert counts["orphaned"] == 1 and counts["adopted"] == 0
    assert store.get("job_lost").status == "failed"
    assert "no queue entry exists" in (await queue.read_result("res_lost"))["error"]



async def test_a_trimmed_unserved_entry_is_no_longer_invisible(store, queue):
    """The hole this closes: `MAXLEN ~` trims by LENGTH, not by ack state, so a
    never-delivered entry can be discarded by later traffic on the same capability.

    Such a job used to fall through every net at once. It keeps its `entry_id`, so the
    orphan sweep's old `entry_id IS NULL` filter skipped it; it never entered a pending
    list, so `XAUTOCLAIM` could not see it; and no result blob was ever written. It
    polled `queued` forever, and the only thing that would ever answer it was the opt-in
    `CBK_MAX_QUEUE_AGE_S`.
    """
    entry = await _really_enqueue(store, queue, "job_trimmed")
    store.insert(id="job_trimmed", result_key="res_job_trimmed", capability=CAP,
                 created_at=LONG_AGO)
    store.record_delivery("job_trimmed", stream=stream_key(CAP), entry_id=entry)
    # Exactly what a trim does, without having to write 10k entries to provoke one.
    await queue.client.xdel(stream_key(CAP), entry)

    counts = await backstop_scan(store, queue, None, **_thresholds())

    assert counts["orphaned"] == 1
    assert store.get("job_trimmed").status == "failed"
    assert "trimmed away unserved" in (await queue.read_result("res_job_trimmed"))["error"]


async def test_the_backstop_will_not_terminalise_a_job_a_worker_is_holding(store, queue):
    """`withdraw` XDELs before it probes, so running it on a CLAIMED entry throws away a
    generation in flight AND orphans the pending row that Redis then drops — taking the
    reaper's recovery path with it. The verdict has to be honoured, not discarded.

    Reachable because a claim is only visible in SQLite once `observe_tick` has copied it
    out of the pending list, so a job claimed between two ticks still reads `queued`.
    """
    await queue.ensure_group(CAP)
    entry = await queue.enqueue({
        "id": "job_held", "created_at": LONG_AGO, "capability": CAP, "prompt": "x",
        "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": "res_held", "attempts": 0, "max_attempts": 3,
    })
    store.insert(id="job_held", result_key="res_held", capability=CAP,
                 created_at=LONG_AGO)
    store.record_delivery("job_held", stream=stream_key(CAP), entry_id=entry)
    # A worker claims it; the row still says `queued` because no tick has observed it.
    assert await queue.read_one(CAP, GROUP, "busy-worker") is not None

    counts = await backstop_scan(store, queue, None,
                                 **_thresholds(max_queue_age_s=60))

    assert counts["expired"] == 0
    assert store.get("job_held").status == "queued", "left alone, not terminalised"
    assert await queue.read_result("res_held") is None, "no answer invented"
    assert await queue.client.xlen(stream_key(CAP)) == 1, "the entry survives"


async def test_a_completion_that_lands_first_is_not_overwritten(store, queue):
    """First writer wins on the coordinator's paths too, not just the executors'.

    The backstop used to write unconditionally, so a result that arrived between the scan
    and this write was replaced by `expired` — the inversion of design.md's "terminal has
    to mean terminal", and worse than the case that rule was written for: the client is
    told a job failed that actually succeeded.
    """
    entry = await _really_enqueue(store, queue, "job_raced")
    store.insert(id="job_raced", result_key="res_job_raced", capability=CAP,
                 created_at=LONG_AGO)
    store.record_delivery("job_raced", stream=stream_key(CAP), entry_id=entry)
    await queue.write_result("res_job_raced", {
        "job_id": "job_raced", "status": "done", "worker": "node-a",
        "completed_at": LONG_AGO, "completion": {"choices": []},
    })

    await backstop_scan(store, queue, None, **_thresholds(max_queue_age_s=60))

    landed = await queue.read_result("res_job_raced")
    assert landed["status"] == "done", "the real completion stands"
    assert landed["worker"] == "node-a"
    assert store.get("job_raced").status == "done", "the row follows the blob"
