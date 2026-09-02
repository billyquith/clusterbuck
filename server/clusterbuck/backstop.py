"""Terminating backstops: no job should wait for an answer that will never come.

A client could previously submit a `waitable` job with no `deadline` and no
`escalate_after_min` and have **nothing** ever give up on it. It was excluded from
escalation (no `escalate_at`), excluded from the deadline sweep (no `deadline`), and
invisible to the reaper — `XAUTOCLAIM` walks the pending-entries list, and an entry no
worker ever claimed never enters it. Its only exit was being trimmed away by `MAXLEN ~`
once enough later traffic arrived on the same capability: no result, no status change,
nothing a poller could observe.

Neither sweep here can reuse the reaper, for that same reason. Both scan SQLite instead,
and both write **both halves** — a terminal status *and* a usage row — because
`jobs_awaiting_usage` selects on the absence of a usage row, so a status alone would be
re-selected on every coordinator tick forever.

They are deliberately two mechanisms, because only one of them can be optional:

* **Orphan sweep** — always on. Catches a job whose row was committed but whose `XADD`
  never happened (the coordinator died in between). Nothing will ever deliver it, so this
  is a crash artifact, not a policy question. It matters more once idempotency exists:
  without it, a client retrying under the same key is told `queued` forever about a job
  that cannot run.
* **Maximum queue age** — opt-in (`CBK_MAX_QUEUE_AGE_S`), off by default. Catches a
  properly-enqueued job nobody ever claimed. That genuinely *is* policy: on a fleet whose
  machines sleep for days, a patient job outliving any fixed cutoff is correct behaviour,
  and clusterbuck has no business imposing a timeout on someone else's backlog.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from .fleet import Fleet
from .models import now_iso
from .queue import Queue
from .store import Store
from .usage import venue_of

_log = logging.getLogger("clusterbuck.backstop")

# The coordinator writes these result blobs itself, standing in for a worker the way the
# reaper's dead-letter does (`result.schema.json` requires a `worker`).
COORDINATOR = "cbk-coordinator"


async def backstop_scan(
    store: Store,
    queue: Queue,
    fleet: Fleet | None,
    *,
    orphan_grace_s: int,
    max_queue_age_s: int | None,
    group: str,
    now: float | None = None,
) -> dict[str, int]:
    """Terminalise jobs that can never be answered. Returns counts per reason.

    Thresholds are parameters rather than reads of `settings`, matching `reaper_scan` —
    `Settings` is a frozen dataclass, and a policy this destructive should be visible at
    the call site anyway.
    """
    at = datetime.fromtimestamp(now, UTC) if now is not None else datetime.now(UTC)
    orphaned = failed = expired = adopted = 0

    cutoff = (at - timedelta(seconds=orphan_grace_s)).isoformat().replace("+00:00", "Z")
    for row in store.orphaned_jobs(cutoff):
        # `entry_id IS NULL` means "no delivery recorded", which is not the same as "never
        # enqueued": a row written before delivery tracking existed has no entry id
        # either. Upgrading the coordinator therefore leaves every in-flight job looking
        # like an orphan, and failing them would mean the deploy itself destroying live
        # work. So look for the entry before concluding it is not there — and if it turns
        # up, adopt it rather than killing the job.
        found = await queue.find_entry_for_job(row.capability, row.id)
        if found is not None:
            stream, entry_id = found
            store.record_delivery(row.id, stream=stream, entry_id=entry_id)
            adopted += 1
            _log.info("adopted %s: found its queue entry, not an orphan", row.id)
            continue

        # Deliberately NOT re-enqueued: `messages`/`prompt`/`params` are not columns, so
        # the payload cannot be rebuilt from SQLite. Failing honestly beats inventing one,
        # and it tells a client retrying under an idempotency key to use a fresh one.
        await _terminalise(store, queue, fleet, row, group=group, status="failed",
                           error=(
            "no queue entry exists for this job: it was either never enqueued (the "
            "coordinator recorded it but did not reach the queue) or its entry was "
            "trimmed away unserved. It cannot be recovered — resubmit it."
        ))
        orphaned += 1
        failed += 1

    if max_queue_age_s is not None:
        cutoff = (at - timedelta(seconds=max_queue_age_s)).isoformat().replace(
            "+00:00", "Z")
        for row in store.stale_queued_jobs(cutoff):
            await _terminalise(store, queue, fleet, row, group=group, status="expired",
                               error=(
                f"unserved for longer than the configured maximum queue age "
                f"({max_queue_age_s}s)"
            ))
            expired += 1

    if orphaned or expired or adopted:
        _log.info("backstop: %d orphaned, %d aged out, %d adopted",
                  orphaned, expired, adopted)
    return {"orphaned": orphaned, "failed": failed, "expired": expired,
            "adopted": adopted}


async def _terminalise(
    store: Store, queue: Queue, fleet: Fleet | None, row, *,
    group: str, status: str, error: str,
) -> None:
    """Write a terminal answer for a job nothing else will ever answer.

    Order matters. The stream entry is withdrawn *first* where one is recorded, so a job
    cannot be picked up and run after being told it failed. Then the result blob (so a
    poller waiting on `result_key` directly gets an answer, not just a status), then the
    status and the usage row together.
    """
    if row.entry_id and row.stream:
        await queue.withdraw(row.stream, row.entry_id, group=group)

    await queue.write_result(row.result_key, {
        "job_id": row.id,
        "status": status,
        "worker": COORDINATOR,
        "completed_at": now_iso(),
        "error": error,
    })
    store.set_status(row.id, status)
    store.record_usage(
        job_id=row.id, ts=now_iso(), capability=row.capability, model=None, node=None,
        venue=venue_of(fleet, row.capability), tokens_in=0, tokens_out=0,
        outcome=status, cost=0.0, day=datetime.now(UTC).strftime("%Y-%m-%d"),
    )
