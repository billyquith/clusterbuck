"""The coordinator's background loop: one periodic tick over stored state.

Every scan here derives what should happen from SQLite + Redis rather than from live
timers, so they survive restarts and can be driven directly in tests. This is the single
place the coordinator gathers its periodic work.

**Each tick is isolated.** The loop used to run the latency-sensitive scans inside one
shared `try`, with `observe_tick` broken out into a second one. That gave the wrong
grouping twice over: an exception in the first scan skipped every later scan in the same
block *and* left `ticks` un-incremented, stalling the slow-cadence work behind it — while
the one tick that had its own handler was the purely observational one that mattered
least. Now every tick gets the same treatment, and a failure costs only its own turn.

Cadences, fastest to slowest: escalation / reservations / usage / observe run every tick;
the wake reconciler on `wake_every`, whose default is chosen to match the DEFAULT wake
cooldown — `maybe_wake` coalesces to at most one wake per capability per
`wake_cooldown_s`, so scanning faster than that window can only burn queries. Nothing
derives one from the other, so moving `CBK_WAKE_COOLDOWN_S` or `CBK_ESCALATION_INTERVAL_S`
away from their defaults breaks the correspondence: the cost is wasted queries or a slower
retry, never a missed wake. The reaper and the terminating
backstops run on `reaper_every` because the thresholds they enforce are measured in
minutes; evals on `eval_every`; the planner on `planner_every`, because it is advisory and
compares slow-moving state.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from datetime import UTC, datetime

from .backstop import backstop_scan
from .catalog import scan_all
from .config import settings
from .escalation import escalation_scan
from .eval_runner import eval_tick
from .fleet import Fleet
from .model_health import ModelHealth
from .observe import observe_tick
from .queue import Queue
from .reaper import reaper_scan
from .reservations import reservation_tick
from .store import Store
from .usage import usage_scan
from .wake import WakeCoordinator, wake_reconcile_scan

_log = logging.getLogger("clusterbuck.coordinator")


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


@contextmanager
def _isolated(name: str):
    """Run one tick; log and swallow its failure so the others still get their turn."""
    try:
        yield
    except Exception:  # a tick failure must not kill the loop, or skip its siblings
        _log.exception("coordinator tick %r failed", name)


async def coordinator_loop(
    store: Store,
    queue: Queue,
    wake: WakeCoordinator,
    fleet: Fleet | None,
    *,
    interval_s: float,
    stop: asyncio.Event,
    planner_every: int = 30,
    eval_every: int = 5,
    reaper_every: int = 6,
    wake_every: int = 6,
    model_health: ModelHealth | None = None,
    probe_every: int = 3,
) -> None:
    """Tick escalation + reservations + usage + observation + evals + the planner."""
    ticks = 0
    while not stop.is_set():
        ticks += 1

        with _isolated("escalation"):
            await escalation_scan(store, queue, wake, fleet=fleet)
        with _isolated("reservation"):
            await reservation_tick(store, wake)
        with _isolated("usage"):
            await usage_scan(store, queue, fleet)

        if ticks % wake_every == 0:
            # Retry a wake that was owed and may simply not have landed: WoL is
            # unacknowledged UDP, and the two demand-side callers — submit and escalation
            # — each fire once on an edge. Without this a lost packet left an urgent job
            # queued with no terminal state and nothing to try again. (Reservations retry
            # their own warm-up wake for the length of the warming window; this scan
            # covers queued JOBS, which is a booking's blind spot and vice versa.)
            with _isolated("wake-reconcile"):
                await wake_reconcile_scan(store, queue, wake)

        if ticks % reaper_every == 0:
            # Recover jobs abandoned by a worker that died mid-run (ADR 20): XAUTOCLAIM
            # over the pending-entries list.
            with _isolated("reaper"):
                await reaper_scan(
                    store, queue, group=settings.consumer_group,
                    min_idle_ms=settings.reaper_min_idle_ms,
                )
            # Same slow cadence, and for the same reason — thresholds in minutes — but a
            # separate mechanism, not a variant of the reaper: these scan SQLite for jobs
            # that never entered the pending list at all, which is precisely what
            # XAUTOCLAIM cannot see (see backstop.py's header).
            with _isolated("backstop"):
                await backstop_scan(
                    store, queue, fleet,
                    orphan_grace_s=settings.orphan_grace_s,
                    max_queue_age_s=settings.max_queue_age_s,
                    group=settings.consumer_group,
                )

        if ticks % eval_every == 0:
            with _isolated("eval"):
                await eval_tick(store, queue, now=_now(), fleet=fleet)

        if ticks % planner_every == 0:
            with _isolated("planner"):
                scan_all(store, now=_now())

        if model_health is not None and (ticks == 1 or ticks % probe_every == 0):
            # On the first tick too: `/fleet` answering `unknown` for a whole interval
            # after every restart is exactly the gap this measurement exists to close.
            with _isolated("model-probe"):
                await model_health.probe_all()

        # Purely observational — nothing routes on it — so it runs last.
        with _isolated("observe"):
            await observe_tick(store, queue, group=settings.consumer_group)

        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except TimeoutError:
            pass
