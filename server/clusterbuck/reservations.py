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
on — that's intra-queue ordering, ADR 24). So is the **pre-load**, which still only logs
what it would warm.

**Re-planning is deferred too, and is smaller than it sounds.** A reservation wakes and
warms; it does not dispatch. The `node` it names is advisory, because jobs address a
capability and any capable node may claim one — so "re-plan to another node" changes
nothing a job could observe, and cloud is a per-job routing decision rather than a
per-booking one. What a booking genuinely owes its holder is a machine awake when the
window opens; the wake retry and the opened-onto-nothing warning below are that promise
kept, rather than the dispatch machinery design.md once implied.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .fleet import Fleet
from .routing import NoCapableArtifact, resolve_capability
from .store import Store
from .wake import WakeCoordinator

_log = logging.getLogger("clusterbuck.reservations")

# Lead time: wake + pre-load this long before the window opens, so the cold-load cost is
# paid off the critical path. A FALLBACK, not the rule — the real figure is each node's
# measured `load_s` (heartbeat `stats.load_s`), because bringing a model up is storage
# speed times model size and runs from a few seconds on NVMe to minutes on a slow disk.
# This constant stood in for that measurement while nothing took it, and it still applies
# to a node whose model server cannot report which models are resident, where coldness
# cannot be proven and so is never sampled.
DEFAULT_WARM_LEAD_S = 300

# Measured load times are a median of a handful of cold starts, and a window that opens
# onto a model still loading has failed at the one thing pre-warming exists to do. Erring
# early costs a node some idle time; erring late costs the reservation.
WARM_LEAD_SAFETY = 1.5


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
    lead_s: float | None = None,
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
        # `etas={}`: a booking is for a window that has not started, so which machine is
        # idle NOW says nothing about it. Routing's speed-and-idleness order is for work
        # that runs on submission; a reservation keeps the ability-then-price order.
        capability = resolve_capability(
            fleet, store, capability=None, task_class=task_class,
            min_ability=min_ability, privacy=privacy, urgency="necessary", etas={},
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
    # An explicit lead wins; otherwise take the slowest cold load measured among the nodes
    # serving this capability, since the reservation does not get to choose which one takes
    # the window.
    if lead_s is None:
        lead_s = WARM_LEAD_SAFETY * store.warm_lead_for(capability, DEFAULT_WARM_LEAD_S)
    warm_by = max(now, starts - lead_s)
    ends = starts + duration_min * 60
    return Admission(
        "confirmed", capability=capability, node=nodes[0].id, artifact=spec.model,
        warm_by=warm_by, starts=starts, ends=ends,
    )


def iso(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC).isoformat().replace("+00:00", "Z")


async def reservation_tick(
    store: Store, wake: WakeCoordinator, *, now: float | None = None
) -> None:
    """Advance each active reservation's lifecycle based on stored absolute times.

    State transitions fire exactly once, because each is gated on the current state.
    The **wake does not**: it is re-offered on every tick of the warming window, for the
    same reason job wakes are retried (`wake.wake_reconcile_scan`). Wake-on-LAN is
    unacknowledged UDP, so a single packet on the scheduled→warming edge was the whole
    of a reservation's chance — lose it and the window opened onto a machine still
    asleep, which is precisely the outcome booking one is meant to prevent. Retrying is
    nearly free: `maybe_wake` short-circuits the moment anything is serving, and
    coalesces to one packet per capability per cooldown either way.

    And if the window opens onto nothing regardless, it says so. That was silent: the
    states advanced on the clock alone, so a reservation whose node never came up looked
    exactly like one that worked.
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
                # The one moment the booking is meant to have paid off. Checked rather
                # than assumed, because everything above this line is clock arithmetic
                # and none of it knows whether a machine actually woke.
                if not await wake.has_live_consumer(r.capability):
                    _log.warning(
                        "reservation %s opened with nothing serving %s — the window is "
                        "live but no node answered the wake", rid, r.capability,
                    )
            else:
                # Still warming: keep offering the wake rather than resting on the one
                # packet sent at warm_by, which nothing acknowledges.
                await wake.maybe_wake(r.capability, reason=f"reservation:{rid}")
        elif state == "open":
            if now >= r.ends:
                store.set_reservation_state(rid, "draining")
        elif state == "draining":
            # Thin pass-through for M2b (no real drain/idle-timeout yet).
            store.set_reservation_state(rid, "closed")
