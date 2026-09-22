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

    Read the payload FIRST: it lives on the stream, not in SQLite, so withdrawing before
    reading would destroy the job. Then withdraw, and only re-add when the withdrawal
    proved the entry was unclaimed. `claimed`, `gone`, or no recorded delivery all mean
    leave it alone — a copy on the urgent stream would run the job twice, which is much
    worse than serving it once in submission order.
    """
    if not row.entry_id or not row.stream:
        return False
    if row.stream.endswith(_URGENT_SUFFIX):
        return False  # already there

    payload = await queue.read_entry(row.stream, row.entry_id)
    if payload is None:
        return False  # trimmed away, or unparseable — nothing safe to move

    if await queue.withdraw(row.stream, row.entry_id, group=group) != "deleted":
        return False

    entry_id = await queue.enqueue(payload, tier=URGENT_TIER)
    store.record_delivery(
        row.id, stream=stream_key(row.capability, URGENT_TIER), entry_id=entry_id
    )
    return True
