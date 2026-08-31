"""The visibility-timeout reaper (ADR 20) — the "laptop closed its lid mid-job" recovery.

The scenario each test builds is the one that was silently broken: a worker claims an entry
with XREADGROUP and dies before XACK, so the entry sits in the pending-entries list and the
job never runs again. These assert it is now requeued, retried a bounded number of times, and
finally dead-lettered so a polling client gets an answer.
"""

from __future__ import annotations

import json

import pytest

from clusterbuck.queue import Queue, stream_key
from clusterbuck.reaper import reaper_scan
from clusterbuck.store import Store

CAP = "8b-extract"
GROUP = "cbk-workers"


@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(str(tmp_path / "reap.db"))


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


async def _submit(queue: Queue, store: Store, job_id: str, *, attempts: int = 0,
                  max_attempts: int = 3) -> None:
    store.insert(id=job_id, result_key=f"res_{job_id}", capability=CAP, created_at="t")
    await queue.enqueue({
        "id": job_id, "created_at": "t", "capability": CAP,
        "prompt": "work", "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": f"res_{job_id}", "attempts": attempts, "max_attempts": max_attempts,
    })


async def _claim_and_die(queue: Queue, consumer: str = "dead-laptop") -> None:
    """Claim everything available, then never ack — i.e. the worker vanished."""
    await queue.client.xreadgroup(GROUP, consumer, {stream_key(CAP): ">"}, count=10)


async def _pending(queue: Queue) -> int:
    summary = await queue.client.xpending(stream_key(CAP), GROUP)
    return int(summary["pending"]) if summary else 0


async def test_abandoned_job_is_requeued_and_redeliverable(store, queue):
    await _submit(queue, store, "job_lid")
    await _claim_and_die(queue)
    assert await _pending(queue) == 1

    # Before the fix, a fresh worker asking for new messages got nothing — proven here.
    fresh = await queue.client.xreadgroup(GROUP, "healthy", {stream_key(CAP): ">"}, count=1)
    assert not fresh

    # min_idle_ms=0 makes every pending entry eligible immediately.
    assert await reaper_scan(store, queue, group=GROUP, min_idle_ms=0,
                             capabilities=[CAP]) == {"requeued": 1, "dead_lettered": 0}

    # Now a live worker receives it, with the attempt count carried forward.
    got = await queue.client.xreadgroup(GROUP, "healthy", {stream_key(CAP): ">"}, count=1)
    assert got
    job = json.loads(got[0][1][0][1]["job"])
    assert job["id"] == "job_lid" and job["attempts"] == 1
    assert store.get("job_lid").attempts == 1


async def test_retries_are_bounded_then_dead_lettered(store, queue):
    await _submit(queue, store, "job_doomed", max_attempts=2)

    # Attempt 1: claimed, abandoned, requeued.
    await _claim_and_die(queue)
    first = await reaper_scan(store, queue, group=GROUP, min_idle_ms=0, capabilities=[CAP])
    assert first["requeued"] == 1

    # Attempt 2: abandoned again — now out of attempts, so it must fail explicitly.
    await _claim_and_die(queue)
    out = await reaper_scan(store, queue, group=GROUP, min_idle_ms=0, capabilities=[CAP])
    assert out == {"requeued": 0, "dead_lettered": 1}

    result = await queue.read_result("res_job_doomed")
    assert result is not None and result["status"] == "failed"
    assert "max_attempts" in result["error"]
    assert store.get("job_doomed").status == "failed"
    assert await _pending(queue) == 0  # nothing left stranded


async def test_busy_worker_is_not_robbed(store, queue):
    """A worker mid-inference isn't reading Redis; a large idle threshold must leave it be."""
    await _submit(queue, store, "job_busy")
    await _claim_and_die(queue)  # "claimed", but only moments ago

    assert await reaper_scan(store, queue, group=GROUP, min_idle_ms=600_000,
                             capabilities=[CAP]) == {"requeued": 0, "dead_lettered": 0}
    assert await _pending(queue) == 1  # still owned by the original consumer
    assert store.get("job_busy").attempts == 0


async def test_finished_but_unacked_job_is_not_rerun(store, queue):
    """The worker wrote its result and died before acking — re-running would duplicate work."""
    await _submit(queue, store, "job_done")
    await _claim_and_die(queue)
    await queue.client.set("res_job_done", json.dumps(
        {"job_id": "job_done", "status": "done", "worker": "w", "completed_at": "t"}))

    assert await reaper_scan(store, queue, group=GROUP, min_idle_ms=0,
                             capabilities=[CAP]) == {"requeued": 0, "dead_lettered": 0}
    assert await _pending(queue) == 0          # tidied away
    # And no duplicate delivery.
    assert not await queue.client.xreadgroup(GROUP, "healthy", {stream_key(CAP): ">"}, count=1)


async def test_scan_discovers_capabilities_from_redis(store, queue):
    """Streams exist because clients submitted to them, not only because fleet.yaml names them."""
    await _submit(queue, store, "job_disc")
    await _claim_and_die(queue)

    assert CAP in await queue.known_capabilities()
    # No explicit capability list — the reaper must find the stream itself.
    out = await reaper_scan(store, queue, group=GROUP, min_idle_ms=0)
    assert out["requeued"] == 1


async def test_streams_are_trimmed(store, queue, monkeypatch):
    """An untrimmed stream keeps every prompt ever submitted resident in Redis."""
    from dataclasses import replace

    from clusterbuck import queue as queue_mod

    # Settings is a frozen dataclass, so swap the module's reference to a modified copy.
    monkeypatch.setattr(queue_mod, "settings",
                        replace(queue_mod.settings, stream_maxlen=5))

    # `MAXLEN ~` trims whole macro-nodes rather than exactly, so it only engages once enough
    # entries accumulate — measured behaviour: 1000 writes at ~5 settles around 14 entries.
    # Exact trimming would be O(N) on every XADD, a real cost on the hot path, so the
    # guarantee we want is *bounded*, not exact.
    for i in range(300):
        await queue.enqueue({
            "id": f"job_trim{i}", "created_at": "t", "capability": CAP, "prompt": "p",
            "params": {}, "urgency": "waitable", "privacy": "local_only",
            "result_key": f"res_trim{i}", "attempts": 0, "max_attempts": 3,
        })

    length = await queue.client.xlen(stream_key(CAP))
    assert length < 50, f"stream grew unbounded ({length} entries after 300 writes)"
