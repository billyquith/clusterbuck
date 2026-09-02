"""Observe what the queue already knows, so a client can see its own wait.

`GET /jobs/{id}` used to report no timestamps and no claiming node until a job was
*finished*, because the only sources were the SQLite row (created at submit) and the
result blob (written at the end). A polling client therefore could not distinguish
"queued" from "running", and had to time jobs from its own submit — lost on a restart.

Redis already holds the missing facts. A worker passes its own id as the `XREADGROUP`
consumer name, so the pending-entries list knows **which node claimed an entry** and
**how long ago**, before any result exists. This tick copies that into the job row.

Two properties worth keeping in mind when reading this:

* The stamp is `now - idle_ms`, the *true* delivery instant — not the tick's clock. A
  tick running late still records an accurate `started_at`, which is why the read path
  needs no `XPENDING` call of its own.
* PEL rows disappear on `XACK`. A job claimed and acked between two ticks is never
  observed here, so it ends up with `finished_at` set and `started_at` NULL. That is
  expected. `started_at IS NULL` means "no claim was observed", never "not started".
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from .queue import REAPER_CONSUMER, Queue
from .store import Store

_log = logging.getLogger("clusterbuck.observe")


async def observe_tick(
    store: Store, queue: Queue, *, group: str, capabilities: list[str] | None = None
) -> dict[str, int]:
    """Stamp `started_at`/`claimed_by` for jobs a consumer is currently holding.

    Returns {"observed": n} — the number of jobs whose claim was recorded by this call
    (already-stamped jobs are not counted again).
    """
    observed = 0
    caps = capabilities if capabilities is not None else await queue.known_capabilities()

    for capability in caps:
        claims = await queue.claims(capability, group)
        # The reaper's own claims are bookkeeping, not work starting: XAUTOCLAIM takes an
        # entry so it can requeue or dead-letter it within the same scan.
        claims = [c for c in claims if c["consumer"] != REAPER_CONSUMER]
        if not claims:
            continue

        rows = store.jobs_by_entry_ids([c["entry_id"] for c in claims])
        for claim in claims:
            row = rows.get(claim["entry_id"])
            if row is None:
                continue  # not ours, or requeued under a new entry id since
            at = _delivered_at(claim["idle_ms"])
            if store.mark_started(row.id, at=at, claimed_by=claim["consumer"]):
                observed += 1

    if observed:
        _log.info("observed %d claim(s)", observed)
    return {"observed": observed}


def _delivered_at(idle_ms: int) -> str:
    """The instant an entry was last delivered, from how long it has been idle."""
    when = datetime.now(UTC) - timedelta(milliseconds=max(idle_ms, 0))
    return when.isoformat().replace("+00:00", "Z")
