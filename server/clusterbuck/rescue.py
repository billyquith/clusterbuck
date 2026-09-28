"""Cloud rescue: a queued `cloud_ok` job the local fleet is not going to serve in time
moves to the cloud alternate routing chose for it at submit (design.md §8).

Submit-time fallback (routing.resolve's `unavailable`) covers a fleet that is already
known to be dark. This covers the other half: the job was queued for a tier that *could*
be served — a machine was up, or one could be woken — and then nothing came. Without it,
a `cloud_ok` job with a deadline sat on a sleeping tier until the expiry sweep failed it,
which is the one outcome its cloud permission existed to prevent.

It is an AVAILABILITY rescue and deliberately never an overflow one. A job is only moved
while no consumer is reading its tier at all. A tier that is up but behind keeps its work,
because "speed never promotes cloud over local" (§12) — and that case is overflow, which
stays designed rather than built.

Everything that decides whether a job MAY leave was fixed at submit or is re-checked here:

* privacy and the target artifact come from the row (`privacy`, `cloud_alternate*`),
  chosen against the same ability bar and `requires` the job was routed on, so a rescue
  can never land on a model that would not have cleared the client's floor;
* urgency is read now, so a `waitable` job is rescued only once escalation has promoted
  it — it has no cloud rights before then (ADR 18);
* the budget is checked now, per job, with each admitted rescue's estimate committed
  before the next is checked (budget.py).

The move itself is `tiering.move_to_urgent_tier`'s sequence, for the same reasons: one
atomic step in Redis that refuses an entry a consumer holds, with the row changed first
so an interruption is recoverable. A `claimed` entry is left exactly where it is. The
result is first-writer-wins either way, but that protects the answer, not the money:
moving a job a worker is already running would pay a provider for an answer that is
about to be thrown away.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from .budget import check_cloud_budget, estimate_cost
from .fleet import Fleet
from .models import JobRecord
from .queue import URGENT_TIER, Queue, stream_key
from .routing import can_call
from .store import Store
from .wake import WakeCoordinator

_log = logging.getLogger("clusterbuck.rescue")


def _created_epoch(created_at: str) -> float:
    return datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()


def _due(row, *, now: float, lead_s: float, after_s: float | None) -> bool:
    """Whether a job has waited long enough that the cloud should take it.

    A deadline decides it when there is one. A job past its deadline is left for the
    expiry sweep: it has already failed its own terms, and answering it now would spend
    money on a result the client stopped waiting for.
    """
    if row.deadline_epoch is not None:
        return now <= row.deadline_epoch <= now + lead_s
    if after_s is None:
        return False
    return now - _created_epoch(row.created_at) >= after_s


async def rescue_scan(
    store: Store,
    queue: Queue,
    wake: WakeCoordinator,
    fleet: Fleet | None,
    *,
    group: str,
    lead_s: float,
    after_s: float | None,
    max_per_tick: int,
    budget_monthly: float | None,
    reserve_fraction: float,
    now: float | None = None,
) -> list[str]:
    """Move due, unserved `cloud_ok` jobs to their cloud alternate. Returns the ids moved."""
    if fleet is None:
        return []
    now = time.time() if now is None else now
    moved: list[str] = []
    served: dict[str, bool] = {}
    for row in store.rescue_candidates():
        if len(moved) >= max_per_tick:
            break
        if not _due(row, now=now, lead_s=lead_s, after_s=after_s):
            continue
        alt = fleet.capabilities.get(row.cloud_alternate or "")
        if alt is None or not alt.cloud or not can_call(alt):
            # fleet.yaml or the key changed since submit (a restart can drop either):
            # nothing it could go to would answer it, so it waits where it is.
            continue
        if await queue.read_result(row.result_key) is not None:
            continue  # answered, and the status has not caught up yet
        if row.capability not in served:
            served[row.capability] = await wake.has_live_consumer(row.capability)
        if served[row.capability]:
            continue  # a machine is reading this tier: that is overflow, not ours

        decision = check_cloud_budget(
            store, monthly_cap=budget_monthly, urgency=row.urgency,
            reserve_fraction=reserve_fraction, now=now)
        if not decision.allowed:
            _log.info("not rescuing %s to %s: %s", row.id, row.cloud_alternate,
                      decision.reason)
            continue

        if await _move(store, queue, fleet, row, group=group):
            moved.append(row.id)
            _log.info("rescued %s from %s to cloud tier %s: nothing is serving %s",
                      row.id, row.capability, row.cloud_alternate, row.capability)
    return moved


async def _move(store: Store, queue: Queue, fleet: Fleet, row, *, group: str) -> bool:
    payload = await queue.read_entry(row.stream, row.entry_id)
    if payload is None:
        return False  # trimmed away, or unparseable — the orphan sweep's to report

    to = row.cloud_alternate
    # The pin has to move with the job: the executor runs `params.model`, and leaving the
    # local artifact there would ask the provider for a model it does not have.
    payload = {**payload, "capability": to,
               "params": {**(payload.get("params") or {}),
                          "model": row.cloud_alternate_model or fleet.capabilities[to].model}}
    # Same tier it was on: a rescue changes WHERE a job runs, never how urgently.
    tier = URGENT_TIER if row.stream.endswith(f":{URGENT_TIER}") else None
    dst = stream_key(to, tier)
    est = estimate_cost(fleet, to, JobRecord.from_wire(payload))

    src, entry_id = row.stream, row.entry_id
    if not store.begin_rescue(row.id, entry_id=entry_id, to_capability=to, est_cost=est):
        return False  # the row moved under us (reaper, cancel) — look again next tick
    outcome = await queue.move_if_unclaimed(src, dst, entry_id, payload, group=group)
    if outcome == "claimed":
        # A worker took it between the liveness read and the move. It runs where it is.
        store.undo_rescue(row.id)
        store.record_delivery(row.id, stream=src, entry_id=entry_id)
        return False
    if outcome == "gone":
        # Nothing moved. The row goes back to its own tier with the delivery left
        # cleared, so the orphan sweep reports the trimmed entry honestly.
        store.undo_rescue(row.id)
        return False
    store.record_delivery(row.id, stream=dst, entry_id=outcome)
    return True
