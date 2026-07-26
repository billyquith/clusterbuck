"""The coordinator's background loop: one periodic tick over stored state.

Runs the escalation scan (ADR 18) and the reservation reconciler (ADR 17) on a shared
cadence. Both derive what should happen from SQLite + Redis rather than from live timers,
so they survive restarts and are driven directly in tests. This is the single place a
future unified coordinator gathers its periodic work.
"""

from __future__ import annotations

import asyncio
import logging

from datetime import datetime, timezone

from .attention import attention_tick
from .catalog import scan_all
from .config import settings
from .escalation import escalation_scan
from .eval_runner import eval_tick
from .fleet import Fleet
from .queue import Queue
from .reaper import reaper_scan
from .reservations import reservation_tick
from .store import Store
from .usage import usage_scan
from .wake import WakeCoordinator

_log = logging.getLogger("clusterbuck.coordinator")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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
) -> None:
    """Tick escalation + reservations + attention + usage + evals + the planner."""
    ticks = 0
    while not stop.is_set():
        try:
            await escalation_scan(store, queue, wake)
            await reservation_tick(store, wake)
            await attention_tick(store, queue)
            await usage_scan(store, queue, fleet)
            ticks += 1
            # Recover jobs abandoned by a worker that died mid-run (ADR 20). Runs on a slow
            # cadence because the idle threshold it enforces is measured in minutes.
            if ticks % reaper_every == 0:
                await reaper_scan(
                    store, queue, group=settings.consumer_group,
                    min_idle_ms=settings.reaper_min_idle_ms,
                )
            # Measuring an unmeasured artifact is background work: collect finished eval
            # jobs and dispatch new ones on a slower cadence than the live ticks.
            if ticks % eval_every == 0:
                await eval_tick(store, queue, now=_now())
            # The planner is advisory and compares slow-moving state, so it runs far less
            # often than the latency-sensitive ticks above.
            if ticks % planner_every == 0:
                scan_all(store, now=_now())
        except Exception:  # a tick failure must not kill the loop
            _log.exception("coordinator tick failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass
