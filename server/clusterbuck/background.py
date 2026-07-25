"""The coordinator's background loop: one periodic tick over stored state.

Runs the escalation scan (ADR 18) and the reservation reconciler (ADR 17) on a shared
cadence. Both derive what should happen from SQLite + Redis rather than from live timers,
so they survive restarts and are driven directly in tests. This is the single place a
future unified coordinator gathers its periodic work.
"""

from __future__ import annotations

import asyncio
import logging

from .escalation import escalation_scan
from .queue import Queue
from .reservations import reservation_tick
from .store import Store
from .wake import WakeCoordinator

_log = logging.getLogger("clusterbuck.coordinator")


async def coordinator_loop(
    store: Store,
    queue: Queue,
    wake: WakeCoordinator,
    *,
    interval_s: float,
    stop: asyncio.Event,
) -> None:
    """Tick escalation + reservations every interval_s until stopped."""
    while not stop.is_set():
        try:
            await escalation_scan(store, queue, wake)
            await reservation_tick(store, wake)
        except Exception:  # a tick failure must not kill the loop
            _log.exception("coordinator tick failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass
