"""Workload reservations (protocols.md §8; ADR 17): cold by default, warm by appointment.

A client books expected demand; the coordinator admission-checks it against the fleet and,
if confirmed, wakes the node and (stub) pre-loads the artifact *before* the window opens.

Two distinct axes (protocols §8 conflates them in prose):
  - admission `status`: confirmed | declined   (declined has no lifecycle)
  - lifecycle `state`:  scheduled → warming → open → draining → closed  (or cancelled)

The lifecycle is advanced by a **reconciler tick over stored state** (not per-reservation
timers), mirroring the escalation scan — so a booking survives a server restart, DELETE is
just a state flip the tick ignores, and it's driven directly in tests. Absolute epochs
(`warm_by`/`starts`/`ends`) are computed once at admission; the tick only compares `now`.

M2b scope: one-shot windows (asap / "HH:MM"); recurrence, counter-offers, real job-drain
and idle-timeout, and priority *enforcement* are deferred (priority is stored, not acted
on — that's intra-queue ordering, ADR 24).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .fleet import Fleet
from .routing import NoCapableArtifact, resolve_capability
from .store import Store
from .wake import WakeCoordinator

_log = logging.getLogger("clusterbuck.reservations")

# Lead time: wake + pre-load this long before the window opens, so the cold-load cost is
# paid off the critical path.
DEFAULT_WARM_LEAD_S = 300


@dataclass(frozen=True)
class Admission:
    status: str  # "confirmed" | "declined"
    reason: str | None = None
    capability: str | None = None
    node: str | None = None
    artifact: str | None = None
    warm_by: float | None = None
    starts: float | None = None
    ends: float | None = None


def window_start_epoch(start: str, now: float) -> float:
    """'asap' → now; 'HH:MM' → the next occurrence of that local time."""
    if start == "asap":
        return now
    hh, mm = (int(x) for x in start.split(":"))
    base = datetime.fromtimestamp(now)
    cand = base.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if cand.timestamp() <= now:
        cand += timedelta(days=1)
    return cand.timestamp()


def admit(
    fleet: Fleet | None,
    store: Store,
    *,
    task_class: str,
    min_ability: int,
    window_start: str,
    duration_min: int,
    privacy: str = "local_only",
    now: float | None = None,
    lead_s: int = DEFAULT_WARM_LEAD_S,
) -> Admission:
    """Feasibility check: can some node clear this need in the requested window?"""
    now = time.time() if now is None else now
    if fleet is None:
        return Admission("declined", reason="no fleet registry")

    try:
        # A reservation is a deliberate, admitted booking — not the lazy "never demands
        # capacity" behaviour `waitable` means for ordinary jobs (ADR 18) — so it is checked
        # as `necessary` for cloud eligibility (paced budget, no reserve). In practice this
        # rarely matters: a no-host cloud capability (ADR 30) has no node either, so the
        # `not nodes` check just below declines it regardless.
        capability = resolve_capability(
            fleet, store, capability=None, task_class=task_class,
            min_ability=min_ability, privacy=privacy, urgency="necessary",
        )
    except NoCapableArtifact as e:
        # Admission control is exactly where an unmeetable need should surface, and the
        # protocol already has a shape for it (§8: confirmed | counter | declined).
        return Admission("declined", reason=str(e))
    spec = fleet.capabilities.get(capability)
    nodes = fleet.nodes_for(capability)
    if spec is None or not nodes:
        return Admission("declined", reason=f"no node serves {capability}")

    starts = window_start_epoch(window_start, now)
    warm_by = max(now, starts - lead_s)
    ends = starts + duration_min * 60
    return Admission(
        "confirmed", capability=capability, node=nodes[0].id, artifact=spec.model,
        warm_by=warm_by, starts=starts, ends=ends,
    )


def iso(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


async def reservation_tick(
    store: Store, wake: WakeCoordinator, *, now: float | None = None
) -> None:
    """Advance each active reservation's lifecycle based on stored absolute times.

    Side-effects (wake + pre-load) fire exactly once, on the scheduled→warming edge,
    because the transition is gated on the current state — the tick is idempotent.
    """
    now = time.time() if now is None else now
    for r in store.active_reservations():
        state, rid = r.state, r.id

        if state == "scheduled":
            if now >= r.ends:
                store.set_reservation_state(rid, "closed")  # window missed entirely
            elif now >= r.warm_by:
                store.set_reservation_state(rid, "warming")
                await wake.maybe_wake(r.capability, reason=f"reservation:{rid}")
                _log.info(
                    "reservation %s warming: pre-load %s on %s (stub)",
                    rid, r.artifact, r.node,
                )
        elif state == "warming":
            if now >= r.ends:
                store.set_reservation_state(rid, "draining")
            elif now >= r.starts:
                store.set_reservation_state(rid, "open")
        elif state == "open":
            if now >= r.ends:
                store.set_reservation_state(rid, "draining")
        elif state == "draining":
            # Thin pass-through for M2b (no real drain/idle-timeout yet).
            store.set_reservation_state(rid, "closed")
