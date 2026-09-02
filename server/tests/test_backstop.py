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
    """`entry_id IS NULL` is the whole signal. A job with a recorded delivery is
    somebody's to run, however old it is — that is the opt-in backstop's business."""
    store.insert(id="job_live", result_key="res_live", capability=CAP,
                 created_at=LONG_AGO)
    store.record_delivery("job_live", stream=stream_key(CAP), entry_id="1-1")

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
    store.record_delivery("job_patient", stream=stream_key(CAP), entry_id="1-1")

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
    assert counts == {"orphaned": 0, "failed": 0, "expired": 0}
    assert store.get("job_done").status == "done"
