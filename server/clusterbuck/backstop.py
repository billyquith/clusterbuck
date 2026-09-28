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

* **Orphan sweep** — always on. Catches a job for which no stream entry exists, by
  either route: the row was committed but the `XADD` never happened (the coordinator died
  in between), or the entry was enqueued and then **trimmed away unserved** by `MAXLEN ~`
  before any worker claimed it. Nothing will ever deliver either one, so this is a crash
  or capacity artifact, not a policy question. It matters more once idempotency exists:
  without it, a client retrying under the same key is told `queued` forever about a job
  that cannot run.

  The trimmed case used to fall through every net at once — it keeps its `entry_id`, so
  this sweep's old `entry_id IS NULL` filter skipped it, and it never entered a pending
  list, so the reaper could not see it either.
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
        # Cheap exact check first, for the population that HAS a recorded entry: if the
        # entry is still on its stream the job is fine — waiting, or claimed and running
        # (XACK leaves the entry in place, so presence is not a claim). Only when the
        # recorded entry has vanished is the expensive scan below worth doing.
        if row.entry_id and row.stream:
            if await queue.read_entry(row.stream, row.entry_id) is not None:
                continue

        # `entry_id IS NULL` means "no delivery recorded", which is not the same as "never
        # enqueued": a row written before delivery tracking existed has no entry id
        # either. Upgrading the coordinator therefore leaves every in-flight job looking
        # like an orphan, and failing them would mean the deploy itself destroying live
        # work. So look for the entry before concluding it is not there — and if it turns
        # up, adopt it rather than killing the job.
        found = await queue.find_entry_for_job(row.capability, row.id)
        if found is None and row.rescued_from:
            # A rescue that died between pointing the row at the cloud tier and moving
            # the entry (rescue.py): the entry never left its original stream. Undo the
            # half that happened, so the job is served — and metered — where it really is.
            found = await queue.find_entry_for_job(row.rescued_from, row.id)
            if found is not None:
                store.undo_rescue(row.id)
        if found is not None:
            stream, entry_id = found
            store.record_delivery(row.id, stream=stream, entry_id=entry_id)
            adopted += 1
            _log.info("adopted %s: found its queue entry, not an orphan", row.id)
            continue

        # Deliberately NOT re-enqueued: `messages`/`prompt`/`params` are not columns, so
        # the payload cannot be rebuilt from SQLite. Failing honestly beats inventing one,
        # and it tells a client retrying under an idempotency key to use a fresh one.
        # The message names both causes because from here they are genuinely
        # indistinguishable, and both are now actually reachable: this sweep selects rows
        # with no recorded entry AND queued rows whose recorded entry has gone.
        if await _terminalise(store, queue, fleet, row, group=group, status="failed",
                              error_code="job_orphaned", error=(
            "no queue entry exists for this job: it was either never enqueued (the "
            "coordinator recorded it but did not reach the queue) or its entry was "
            "trimmed away unserved. It cannot be recovered — resubmit it."
        )):
            orphaned += 1
            failed += 1

    if max_queue_age_s is not None:
        cutoff = (at - timedelta(seconds=max_queue_age_s)).isoformat().replace(
            "+00:00", "Z")
        for row in store.stale_queued_jobs(cutoff):
            if await _terminalise(store, queue, fleet, row, group=group, status="expired",
                                  error_code="job_expired", error=(
                f"unserved for longer than the configured maximum queue age "
                f"({max_queue_age_s}s)"
            )):
                expired += 1

    if orphaned or expired or adopted:
        _log.info("backstop: %d orphaned, %d aged out, %d adopted",
                  orphaned, expired, adopted)
    return {"orphaned": orphaned, "failed": failed, "expired": expired,
            "adopted": adopted}


async def _terminalise(
    store: Store, queue: Queue, fleet: Fleet | None, row, *,
    group: str, status: str, error: str, error_code: str,
) -> bool:
    """Write a terminal answer for a job nothing else will ever answer.

    Returns False when the job turned out to be live and was left alone.

    Order matters. The stream entry is withdrawn *first* where one is recorded, so a job
    cannot be picked up and run after being told it failed. Then the result blob (so a
    poller waiting on `result_key` directly gets an answer, not just a status), then the
    status and the usage row together.

    Two refusals, because "nothing else will ever answer" has to be established rather
    than assumed:

    * `withdraw_if_unclaimed` rather than `withdraw`, and returning `"claimed"` means a
      consumer holds the entry — a worker is generating right now. The plain `withdraw`
      cannot be used here: it XDELs *before* it probes (correct for a cancellation, where
      guaranteeing non-delivery is the point), so by the time it reports `"claimed"` it
      has already thrown away the GPU time and orphaned the pending row that Redis then
      drops, taking the reaper's recovery path with it — `store.py`'s note on
      `cancel_requested` documents that hazard precisely. Leave it; it will finish, or
      the reaper will get it.
    * The result blob is written `only_if_absent`, so a completion that landed between the
      caller's scan and this write stands. Without it the coordinator was the one writer
      in the system that could overwrite a `done` with a `failed` (design.md §12).
    """
    if row.entry_id and row.stream:
        outcome = await queue.withdraw_if_unclaimed(
            row.stream, row.entry_id, group=group)
        if outcome == "claimed":
            _log.info("%s is claimed by a worker — not terminalising", row.id)
            return False

    wrote = await queue.write_result(row.result_key, {
        "job_id": row.id,
        "status": status,
        "worker": COORDINATOR,
        "completed_at": now_iso(),
        "error": error,
        "error_code": error_code,
    }, only_if_absent=True)
    if not wrote:
        landed = await queue.read_result(row.result_key) or {}
        status = landed.get("status") or "done"
        _log.info("%s was answered before the backstop reached it — keeping that (%s)",
                  row.id, status)
    store.set_status(row.id, status)
    store.record_usage(
        job_id=row.id, ts=now_iso(), capability=row.capability, model=None, node=None,
        venue=venue_of(fleet, row.capability), tokens_in=0, tokens_out=0,
        outcome=status, cost=0.0, day=datetime.now(UTC).strftime("%Y-%m-%d"),
    )
    return True
