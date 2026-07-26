"""The visibility-timeout reaper (protocols.md §2, ADR 20).

This is the "laptop closed its lid mid-job" recovery, and the reason the queue is built on
Redis Streams rather than `BLPOP` lists. A worker claims an entry with `XREADGROUP` and only
`XACK`s it after writing a result; if it dies in between, the entry sits in the consumer
group's pending-entries list forever and the job silently never runs. `XAUTOCLAIM` finds
those entries; this module decides what to do with them.

Policy, matching what the protocol documents:

* An entry idle longer than `min_idle_ms` is presumed abandoned and **requeued** — appended
  fresh to its capability stream with `attempts` incremented, then the stale entry is acked
  away. Re-delivery through the stream (rather than leaving it claimed by the reaper) is what
  lets any live worker pick it up.
* Past `max_attempts`, the job is **dead-lettered**: a terminal `failed` result is written so
  a polling client gets an answer instead of waiting forever.

`min_idle_ms` must exceed the longest plausible inference. A worker mid-generation is not
reading from Redis, so too small a threshold steals work from a healthy-but-busy node and
runs it twice. Real heartbeats (ADR 9) narrow this, but the idle threshold is the backstop.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from .queue import Queue
from .store import Store

_log = logging.getLogger("clusterbuck.reaper")

REAPER_CONSUMER = "cbk-reaper"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


async def reaper_scan(
    store: Store, queue: Queue, *, group: str, min_idle_ms: int,
    capabilities: list[str] | None = None,
) -> dict[str, int]:
    """Requeue abandoned entries; dead-letter those out of attempts.

    Returns {"requeued": n, "dead_lettered": n}.
    """
    requeued = dead = 0
    caps = capabilities if capabilities is not None else await queue.known_capabilities()

    for capability in caps:
        stale = await queue.reclaim_stale(
            capability, group, min_idle_ms=min_idle_ms, consumer=REAPER_CONSUMER
        )
        for entry_id, job in stale:
            # A job whose result already exists finished just as we reclaimed it: the worker
            # wrote the result but died before acking. Nothing to re-run.
            result_key = job.get("result_key")
            if result_key and await queue.read_result(result_key) is not None:
                await queue.ack(capability, group, entry_id)
                continue

            attempts = int(job.get("attempts", 0)) + 1
            max_attempts = int(job.get("max_attempts", 3))
            job_id = job.get("id", "<unknown>")

            if attempts >= max_attempts:
                if result_key:
                    await queue.write_result(result_key, {
                        "job_id": job_id,
                        "status": "failed",
                        "worker": REAPER_CONSUMER,
                        "completed_at": _now_iso(),
                        "error": (
                            f"abandoned by its worker and retried {attempts} times "
                            f"(max_attempts={max_attempts})"
                        ),
                    })
                store.set_status(job_id, "failed")
                store.set_attempts(job_id, attempts)
                await queue.ack(capability, group, entry_id)
                dead += 1
                _log.warning("dead-letter %s [%s] after %d attempts",
                             job_id, capability, attempts)
                continue

            # Requeue for another go, then retire the stale delivery.
            await queue.enqueue({**job, "attempts": attempts})
            store.set_attempts(job_id, attempts)
            await queue.ack(capability, group, entry_id)
            requeued += 1
            _log.info("requeued %s [%s] (attempt %d/%d)",
                      job_id, capability, attempts, max_attempts)

    return {"requeued": requeued, "dead_lettered": dead}
