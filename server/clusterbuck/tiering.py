"""When it is safe to use the urgency tiers (ADR 34).

Tiering splits each capability into `q:<cap>:urgent` and `q:<cap>`, and the urgent stream
is only useful if something reads it. A worker built before tiering reads the base stream
only, so writing an urgent job to the urgent stream during a rollout would **strand** it —
precisely the failure the tier exists to prevent.

The gate is therefore evidence-based rather than version-based: a node reports the streams
it reads in its heartbeat `queues`, so a node listing a `:urgent` stream is demonstrably
able to serve one. That is a stronger signal than a version string, which only says what
build is installed, not what it does.

Two details that decide correctness:

* It gates on **enrolled** nodes, not live ones, and requires **at least one**. "Every
  live node is tier-aware" is vacuously true on a cold fleet with nothing awake — and the
  asleep node is exactly the one that will wake up and claim the job.
* Cloud-only capabilities are always tier-aware: their only consumer is the coordinator's
  own executor, which ships with this code.
"""

from __future__ import annotations

from .fleet import Fleet
from .queue import URGENT_TIER, stream_key
from .store import Store, json_list

_URGENT_SUFFIX = f":{URGENT_TIER}"


def tiering_ready(
    store: Store, fleet: Fleet | None, capability: str, *, mode: str = "auto"
) -> bool:
    """Whether `capability` can safely use the urgent tier."""
    if mode == "off":
        return False
    if mode == "on":
        return True

    spec = fleet.capabilities.get(capability) if fleet else None
    if spec is not None and getattr(spec, "cloud", False):
        return True  # drained in-process by this build's own executor

    serving = [n for n in store.list_nodes() if _serves(n, capability)]
    if not serving:
        return False  # nothing enrolled: no evidence, so do not tier
    return all(_tier_aware(n) for n in serving)


def _serves(node, capability: str) -> bool:
    return capability in json_list(node.capabilities)


def _tier_aware(node) -> bool:
    """True if this node's last heartbeat listed an urgent stream among its queues."""
    return any(q.endswith(_URGENT_SUFFIX) for q in json_list(node.queues))


async def move_to_urgent_tier(store: Store, queue, row, *, group: str) -> bool:
    """Move a promoted job's queued entry onto the urgent tier. True if it moved.

    Escalation changes a job's urgency in SQLite only, so without this a
    promoted job keeps its place on the base stream and the promotion buys it nothing on a
    worker that is already awake — the exact complaint that reopened ADR 24.

    Read the payload FIRST: it lives on the stream, not in SQLite, so removing the entry
    before reading would destroy the job. `claimed`, `gone`, or no recorded delivery all
    mean leave it alone — a copy on the urgent stream would run the job twice, which is
    much worse than serving it once in submission order.

    Two things make the move safe to be interrupted, and both replace an earlier sequence
    that could lose a job outright:

    * **One atomic step in Redis** (`queue.move_if_unclaimed`), not `withdraw` then
      `enqueue`. The payload exists only on the stream, so a coordinator that died between
      those two calls destroyed the job — and left it invisible to every recovery path at
      once, since it was in no pending list and its row still named the deleted entry.
      That primitive also refuses to delete an entry a consumer holds, where `withdraw`
      deletes first by design; deleting a running job's entry orphans its PEL row, which
      Redis 7's XAUTOCLAIM drops (see `orm/job.py` on `cancel_requested`).
    * **The recorded delivery is cleared before the move, not after.** The remaining window
      is between the successful move and re-recording it, and clearing first turns that
      into a self-healing state rather than a stuck one: `entry_id IS NULL` is what the
      orphan sweep selects, and it looks for the entry before concluding anything, so it
      finds the entry on the urgent stream and adopts it. Restored only on `claimed`,
      where the original entry is still there and still valid; on `gone` the row is left
      cleared deliberately, so the sweep can report honestly that the entry was trimmed
      away unserved instead of pointing at one that no longer exists.
    """
    if not row.entry_id or not row.stream:
        return False
    if row.stream.endswith(_URGENT_SUFFIX):
        return False  # already there

    payload = await queue.read_entry(row.stream, row.entry_id)
    if payload is None:
        return False  # trimmed away, or unparseable — nothing safe to move

    dst = stream_key(row.capability, URGENT_TIER)
    store.clear_delivery(row.id)
    outcome = await queue.move_if_unclaimed(
        row.stream, dst, row.entry_id, payload, group=group
    )
    if outcome == "claimed":
        store.record_delivery(row.id, stream=row.stream, entry_id=row.entry_id)
        return False
    if outcome == "gone":
        return False

    store.record_delivery(row.id, stream=dst, entry_id=outcome)
    return True
