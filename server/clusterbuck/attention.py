"""Client attention as a lease (ADR 18 / protocols §9).

A client posts attention when its user becomes active; its waitable backlog (optionally
scoped by task class) promotes to necessary so results are fresh shortly after the user
sits down. The lease is TTL'd — on expiry the coordinator demotes the still-unstarted
promotions back to waitable, and the fabric returns to lazy. Symmetric with worker
presence (owner active → small model): user active → hot work.
"""

from __future__ import annotations

import logging
import time

from .queue import Queue
from .store import Store

_log = logging.getLogger("clusterbuck.attention")


async def attention_tick(store: Store, queue: Queue, *, now: float | None = None) -> int:
    """Expire lapsed leases, demoting unstarted attention-promotions. Returns demoted count."""
    now = time.time() if now is None else now
    demoted = 0
    for lease in store.expired_attention_leases(now):
        client_key = lease["client_key"]
        for job in store.attention_promoted_jobs(client_key):
            # Only demote work that hasn't been served — started/finished stays as-is.
            if await queue.read_result(job.result_key) is None:
                store.demote_job(job.id)
                demoted += 1
        store.delete_attention_lease(client_key)
        _log.info("attention lease for %s lapsed; demoted unstarted work", client_key)
    return demoted
