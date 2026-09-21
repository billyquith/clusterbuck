"""The escalation engine (ADR 18; fleet-management.md → Urgency, escalation).

Urgency is a trajectory: a `waitable(N)` job that is still unserved N minutes after
submission promotes to `necessary` and thereby gains wake rights. This module is the
age-based trigger; a backlog-watermark trigger is later work.

Doneness is judged against the Redis result blob, never SQLite's `status` (which is
refreshed lazily on poll) — otherwise a completed-but-unpolled job would be wrongly
promoted and could wake a machine for nothing.
"""

from __future__ import annotations

import logging
import time

from .config import settings
from .queue import Queue
from .store import Store
from .tiering import move_to_urgent_tier, tiering_ready
from .wake import WakeCoordinator

_log = logging.getLogger("clusterbuck.escalation")


async def escalation_scan(
    store: Store,
    queue: Queue,
    wake: WakeCoordinator,
    *,
    now: float | None = None,
    fleet=None,
) -> list[str]:
    """Promote due waitable jobs to necessary. Returns the promoted job ids."""
    now = time.time() if now is None else now
    promoted: list[str] = []
    for row in store.due_for_escalation(now):
        if await queue.read_result(row.result_key) is not None:
            continue  # already served — don't promote or wake
        store.mark_escalated(row.id)
        promoted.append(row.id)
        _log.info("escalate %s [%s] waitable → necessary", row.id, row.capability)
        # Promotion has to move the queued entry, not just the row: urgency now decides
        # which stream a job sits on (ADR 34), and a promoted job left on the base stream
        # gains wake rights but no better place in line.
        if tiering_ready(store, fleet, row.capability, mode=settings.urgent_streams):
            row = store.get(row.id) or row  # pick up the delivery recorded at submit
            if await move_to_urgent_tier(
                store, queue, row, group=settings.consumer_group
            ):
                _log.info("moved %s to the urgent tier", row.id)
        await wake.maybe_wake(row.capability, reason="escalation")
    return promoted
