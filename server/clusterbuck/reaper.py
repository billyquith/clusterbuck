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
from datetime import UTC, datetime

from .models import TERMINAL_STATUSES
from .queue import REAPER_CONSUMER, TIER_ORDER, Queue, stream_key
from .store import Store

_log = logging.getLogger("clusterbuck.reaper")



def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


async def reaper_scan(
    store: Store, queue: Queue, *, group: str, min_idle_ms: int,
    capabilities: list[str] | None = None,
) -> dict[str, int]:
    """Requeue abandoned entries; dead-letter those out of attempts.

    Returns {"requeued": n, "dead_lettered": n}.
    """
    requeued = dead = 0
    caps = capabilities if capabilities is not None else await queue.known_capabilities()

    # Both urgency tiers (ADR 34). An abandoned job on the urgent stream is exactly the
    # one whose recovery matters most, so covering only the base stream would strand it.
    for capability, tier in ((c, t) for c in caps for t in TIER_ORDER):
        stale = await queue.reclaim_stale(
            capability, group, min_idle_ms=min_idle_ms, consumer=REAPER_CONSUMER,
            tier=tier,
        )
        for entry_id, job in stale:
            job_id = job.get("id", "<unknown>")

            # A job whose result already exists finished just as we reclaimed it: the worker
            # wrote the result but died before acking. Nothing to re-run.
            result_key = job.get("result_key")
            if result_key and await queue.read_result(result_key) is not None:
                await queue.ack(capability, group, entry_id, tier=tier)
                continue

            # The coordinator's OWN sweeps terminalise a job by writing the row's status
            # directly, with no result blob at all — the deadline-expiry and
            # cancellation-floor branches in `usage_scan` both do this. The read_result
            # probe above cannot see that: there is nothing in Redis to read. Without this
            # check a claim reclaimed after one of those sweeps had already given up on it
            # was requeued (or dead-lettered) anyway, un-terminalising an `expired` or
            # `cancelled` row back to `queued`/`failed` — exactly what "terminal means
            # terminal" (design.md §12) forbids. The stale entry is still acked away: it
            # is not going to be delivered again regardless, and leaving it pending would
            # only give a later scan the same decision to make.
            row = store.get(job_id)
            if row is not None and row.status in TERMINAL_STATUSES:
                _log.info("%s [%s] is already %s — dropping the stale claim, not "
                          "reviving it", job_id, capability, row.status)
                await queue.ack(capability, group, entry_id, tier=tier)
                continue

            attempts = int(job.get("attempts", 0)) + 1
            max_attempts = int(job.get("max_attempts", 3))

            if attempts >= max_attempts:
                # FIRST WRITER WINS here too (`only_if_absent`), not just on the executor
                # paths. The `read_result` probe above is a check, and a check separated
                # from its write by an await is not an atomic operation: the worker we are
                # about to give up on can land its real `done` in that gap. Writing
                # unconditionally then overwrote a genuine completion with this
                # dead-letter and destroyed the text — the exact inversion of "terminal
                # means terminal" (design.md §12), and worse than the case that decision
                # was written for, because the client is told `failed` about a job that
                # SUCCEEDED.
                #
                # So the SQLite side has to follow the blob rather than assume it won:
                # forcing `failed` after losing the race would leave the row and the
                # answer disagreeing, which is the same lie one layer down.
                status = "failed"
                if result_key:
                    wrote = await queue.write_result(result_key, {
                        "job_id": job_id,
                        "status": "failed",
                        "worker": REAPER_CONSUMER,
                        "completed_at": _now_iso(),
                        "error": (
                            f"abandoned by its worker and retried {attempts} times "
                            f"(max_attempts={max_attempts})"
                        ),
                        "error_code": "job_abandoned",
                    }, only_if_absent=True)
                    if not wrote:
                        landed = await queue.read_result(result_key) or {}
                        status = landed.get("status") or "done"
                        _log.info(
                            "%s [%s] answered by its worker while being dead-lettered — "
                            "keeping that result (%s)", job_id, capability, status)
                store.set_status(job_id, status)
                store.set_attempts(job_id, attempts)
                await queue.ack(capability, group, entry_id, tier=tier)
                if status == "failed":
                    dead += 1
                    _log.warning("dead-letter %s [%s] after %d attempts",
                                 job_id, capability, attempts)
                continue

            # Requeue onto the SAME tier it was reclaimed from, deliberately: this keeps
            # the reaper free of the rollout gate (it cannot know whether tiering is safe
            # for this fleet) and preserves the placement the enqueue path chose. A
            # promotion that happened while the job was claimed is not re-applied here —
            # the promotion path moves unclaimed entries only — so such a job finishes its
            # retries on the base stream. Bounded and documented rather than papered over.
            new_entry_id = await queue.enqueue({**job, "attempts": attempts}, tier=tier)
            store.set_attempts(job_id, attempts)
            # The requeue is a NEW stream entry, so the recorded delivery must follow it
            # or queue position and withdrawal keep pointing at an entry that is about to
            # be acked away. Back to `queued` too: it is at the back of the line again.
            # `started_at` is deliberately left set — started-then-queued is a meaningful
            # combination meaning "a worker had this and died".
            store.record_delivery(job_id, stream=stream_key(capability, tier),
                                  entry_id=new_entry_id)
            store.set_status(job_id, "queued")
            await queue.ack(capability, group, entry_id, tier=tier)
            requeued += 1
            _log.info("requeued %s [%s] (attempt %d/%d)",
                      job_id, capability, attempts, max_attempts)

    return {"requeued": requeued, "dead_lettered": dead}
