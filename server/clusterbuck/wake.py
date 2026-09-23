"""The wake coordinator (DESIGN.md → Availability & wake; ADR 2).

Turns "asleep" into "available". When a job that has wake rights (`urgent` or `necessary`
— directly submitted or escalated) needs capacity and *no worker is currently serving*
that capability, it sends Wake-on-LAN to the capable nodes' MACs from fleet.yaml.

**Liveness is read from two signals, and needs both.**

* **Heartbeat** — a node says, every 10s from its own asyncio task, which streams it is
  claiming from. Accurate, because that task is never blocked by a long inference, and
  precise, because a paused node reports an empty `queues` and so stops counting the
  moment its owner evicts it. Only enrolled nodes have one.
* **Consumer idle time** on the capability's stream/group. Empty `XREADGROUP >` reads
  refresh `idle` (verified on Redis 7.4), so a polling-but-jobless worker reads as alive
  — where `active-time` would falsely read as gone. Covers the env-configured worker that
  never enrolled and therefore never heartbeats.

The second signal alone was the whole of it, and it is wrong in one direction that
matters: a worker mid-generation is not polling, so a node serving a job longer than
`worker_dead_ms` read as *dead* and had WoL broadcast at its sleeping peers every cooldown
window — in a fleet whose entire resting state is asleep. The heartbeat is what corrects
that, so the two are OR'd: alive on either is alive.

Wakes coalesce: at most one wake per capability per cooldown window, so an escalation
burst never stampedes. And they are **retried**, by `wake_reconcile_scan` — see its
docstring for why a wake fired once is not a wake delivered.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime

import redis.asyncio as redis

from .fleet import Fleet
from .queue import (
    CLOUD_EXECUTOR_CONSUMER,
    TIER_ORDER,
    Queue,
    live_worker_consumers,
    stream_key,
)
from .store import Store, json_list
from .wol import send_magic_packet

_log = logging.getLogger("clusterbuck.wake")


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    # Stamps are written UTC-aware, but a hand-edited or migrated row may be naive.
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def heartbeat_age_s(node, *, now: datetime) -> float | None:
    """Seconds since this node last spoke, or None if it has no readable stamp at all.

    Falls back to `enrolled_at`, because `Store.enroll_node` does not set
    `last_heartbeat` — a node that enrolled and never heartbeated has been silent since it
    enrolled, and reading that as "no information" would exempt precisely the nodes that
    never came up.

    Lives here rather than in `web.py`, where it started, because it is now load-bearing
    for a routing-adjacent decision (whether to wake a machine) and not only for a pill on
    the dashboard. `web.py` imports it from here.
    """
    at = _parse_iso(node.last_heartbeat) or _parse_iso(node.enrolled_at)
    if at is None:
        return None
    return max(0.0, (now - at).total_seconds())


def nodes_serving(
    nodes, capability: str, *, silent_s: float, now: datetime | None = None
) -> list[str]:
    """Node ids whose last heartbeat both is RECENT and says they are claiming `capability`.

    Two conditions, and the second is what makes this usable as a liveness signal rather
    than a declaration. `mode` is what a node last said about itself and nothing expires
    it; `queues` is what it says it is *reading right now*, rebuilt from the presence
    ladder on every beat. A paused node's ladder yields no capabilities, so it heartbeats
    an empty `queues` and correctly stops counting as someone serving — while still being
    demonstrably awake, which is a different question this function does not answer.

    Matched against the full stream names for both tiers rather than by substring: a
    capability named as a prefix of another (`8b-extract` / `8b-extract-v2`) would
    otherwise have one node's heartbeat vouch for the other's queue.

    A node with no readable `queues` — enrolled but not yet heartbeated, or a client that
    omits the field — simply does not count. That is the safe direction: the cost is one
    redundant magic packet at a machine that is already awake, where the opposite error
    is a job that waits for a wake nobody sends.
    """
    now = now or datetime.now(UTC)
    wanted = {stream_key(capability, tier) for tier in TIER_ORDER}
    serving = []
    for node in nodes:
        age = heartbeat_age_s(node, now=now)
        if age is None or age > silent_s:
            continue
        if wanted & set(json_list(node.queues)):
            serving.append(node.node_id)
    return serving


class WakeCoordinator:
    def __init__(
        self,
        fleet: Fleet | None,
        client: redis.Redis,
        *,
        group: str,
        dead_ms: int = 60_000,
        cooldown_s: float = 60.0,
        broadcast: str = "255.255.255.255",
        port: int = 9,
        send: Callable[..., None] = send_magic_packet,
        clock: Callable[[], float] = time.monotonic,
        store: Store | None = None,
        silent_s: float = 60.0,
    ) -> None:
        self._fleet = fleet
        self._r = client
        self._group = group
        self._dead_ms = dead_ms
        self._cooldown_s = cooldown_s
        self._broadcast = broadcast
        self._port = port
        self._send = send
        self._clock = clock
        # Optional so the stream signal remains usable on its own — that is the whole of
        # what a pre-enrollment fleet has, and several tests drive exactly that.
        self._store = store
        self._silent_s = silent_s
        self._last_wake: dict[str, float] = {}
        # Capabilities already reported as having nothing wakeable. The reconciler retries
        # forever by design, so the warning has to fire on the TRANSITION or it becomes
        # per-minute noise — the same dedupe `api.py` keeps for capability warnings.
        self._warned_unwakeable: set[str] = set()

    async def has_live_consumer(self, capability: str) -> bool:
        """True if something that can actually serve this capability is up right now.

        Either signal suffices (see the module docstring for why neither does alone).

        The stream read covers **every tier**, not just the base one. A worker reads the
        urgent stream first and breaks out of the tier loop as soon as it takes a job, so
        a capability under sustained urgent load never refreshes its *base*-group idle
        time — and reading only the base stream therefore reported the busiest capability
        in the fleet as unserved.

        The cloud executor counts — on a cloud-only capability it is the only consumer
        there will ever be, and if it is draining the queue then no machine needs waking.

        Called on the submit path as well as the tick, so both reads have to stay cheap:
        the heartbeat one is a scan of a table with a handful of rows, the same shape (and
        the same bet) as the tiering gate `api.py` already runs on that request. If either
        shows up in a profile, recompute it on heartbeat rather than caching it here.

        The reaper deliberately does NOT count, and that exclusion is the point: its
        `XAUTOCLAIM` registers `cbk-reaper` on the group and refreshes its idle time on
        every scan (~60s, i.e. within `dead_ms`), so counting it made the coordinator's
        own bookkeeping look like a live worker and suppress the wake an urgent job was
        entitled to.
        """
        if self._store is not None and nodes_serving(
            self._store.list_nodes(), capability, silent_s=self._silent_s
        ):
            return True

        for tier in TIER_ORDER:
            try:
                consumers = await self._r.xinfo_consumers(
                    stream_key(capability, tier), self._group
                )
            except redis.ResponseError:
                continue  # no group/stream on this tier; the other one may still have it
            if live_worker_consumers(
                consumers, dead_ms=self._dead_ms, include=(CLOUD_EXECUTOR_CONSUMER,)
            ):
                return True
        return False

    async def maybe_wake(self, capability: str, *, reason: str) -> list[str]:
        """Wake capable sleeping nodes for a capability. Returns the node ids woken."""
        if self._fleet is None:
            return []
        if await self.has_live_consumer(capability):
            return []  # a worker is up; it will drain — no wake needed

        now = self._clock()
        last = self._last_wake.get(capability)
        if last is not None and (now - last) < self._cooldown_s:
            return []  # coalesce: already woke this capability recently

        woken: list[str] = []
        for node in self._fleet.nodes_for(capability):
            if not node.mac:
                continue
            try:
                self._send(node.mac, broadcast=self._broadcast, port=self._port)
                woken.append(node.id)
            except Exception as e:  # a bad MAC shouldn't sink the others
                _log.warning("WoL to %s (%s) failed: %s", node.id, node.mac, e)

        if woken:
            self._last_wake[capability] = now
            self._warned_unwakeable.discard(capability)
            _log.info(
                "wake %s → %s (reason=%s)", capability, ", ".join(woken), reason
            )
        elif capability not in self._warned_unwakeable:
            # Nothing serving it and nothing that can be woken into serving it: every
            # capable node either is absent from fleet.yaml or has no MAC there (a node
            # reachable only over a tunnel has none to give). The job is not stuck for any
            # reason the coordinator can fix, and without this it is stuck SILENTLY —
            # the reconciler retries on every scan and each attempt logs nothing.
            self._warned_unwakeable.add(capability)
            _log.warning(
                "%s has work and nothing serving it, but no capable node can be woken "
                "(no MAC in the registry, or no node serves it at all) — reason=%s",
                capability, reason,
            )
        return woken


async def wake_reconcile_scan(
    store: Store, queue: Queue, wake: WakeCoordinator, *, now: float | None = None
) -> list[str]:
    """Re-attempt the wake an unserved job with wake rights is owed. Returns capabilities woken.

    **A wake fired is not a wake delivered.** The two demand-side callers of `maybe_wake`
    are edge-triggered — one burst at submit, one on escalation — and a magic packet is
    fire-and-forget UDP with no acknowledgement by construction (`wol.py`). (Reservations
    are the third caller and retry for themselves across the warming window, which is the
    same lesson applied where this scan cannot reach: a booking warms before any job
    exists, so it is invisible to the queued-job population below.)
    It is lost for entirely ordinary reasons: a sleeping machine
    on Wi-Fi that never registered with a sleep proxy, a node on another subnet (the
    broadcast does not cross one), a switch that aged out the MAC, a node reachable only
    over a tunnel and so holding no MAC to try at all.

    Without this scan the consequence was silent and terminal. An `urgent` job got exactly
    one attempt: escalation could never revisit it (`due_for_escalation` selects
    `waitable` rows only, and promotes each once), nothing else re-examined it, and with
    `CBK_MAX_QUEUE_AGE_S` unset — the default, and deliberately so — the job had no
    terminal state at all. Not orphaned, its entry exists; not reaped, it never entered
    the pending list; not escalated. It read `queued` until `MAXLEN ~` trimmed the entry
    out from under it, and the client was still told `queued` about a job that provably
    could not run.

    This is **not** the backlog-watermark escalation trigger (design.md → "Urgency is a
    trajectory"), which is still designed-not-built. Nothing is promoted here and no
    urgency changes: this only retries a wake already owed to a job that has the right to
    one. Hence `reason="unserved"` in the log, not `backlog`.

    Cheap by construction, and it has to be, because it runs forever against a fleet that
    is *supposed* to be asleep. `maybe_wake` short-circuits on liveness and then on its
    per-capability cooldown, so the steady state of a healthy fleet is one SQL scan of a
    small table and nothing else. Doneness is confirmed against the Redis result blob and
    never SQLite's `status`, the same discipline `escalation_scan` follows and for the
    same reason: a completed-but-unpolled job must not wake a machine.
    """
    now = time.time() if now is None else now
    woken: list[str] = []
    decided: set[str] = set()
    for row in store.jobs_awaiting_capacity(now):
        if row.capability in decided:
            continue  # one wake decision per capability per scan, not per job
        if await queue.read_result(row.result_key) is not None:
            continue  # served already; its row is just waiting on the usage tick
        decided.add(row.capability)
        if await wake.maybe_wake(row.capability, reason="unserved"):
            woken.append(row.capability)
    if woken:
        _log.info("wake reconcile: retried a wake for %s", ", ".join(woken))
    return woken
