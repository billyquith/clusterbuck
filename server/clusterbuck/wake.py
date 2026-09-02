"""The wake coordinator (DESIGN.md → Availability & wake; ADR 2).

Turns "asleep" into "available". When a job that has wake rights (`urgent` or `necessary`
— directly submitted or escalated) needs capacity and *no worker is currently serving*
that capability, it sends Wake-on-LAN to the capable nodes' MACs from fleet.yaml.

Liveness signal: a consumer's `idle` time on the capability's stream/group. Empty
`XREADGROUP >` reads refresh `idle` (verified on Redis 7.4), so a polling-but-jobless
worker reads as alive — where `active-time` would falsely read as gone. The dead
threshold must therefore exceed the longest inference (a busy worker isn't polling); this
is an M2 approximation that real heartbeats (M4) supersede.

Wakes coalesce: at most one wake per capability per cooldown window, so an escalation
burst never stampedes.
"""

from __future__ import annotations

import logging
import time
from typing import Awaitable, Callable

import redis.asyncio as redis

from .fleet import Fleet
from .queue import CLOUD_EXECUTOR_CONSUMER, live_worker_consumers, stream_key
from .wol import send_magic_packet

_log = logging.getLogger("clusterbuck.wake")


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
        self._last_wake: dict[str, float] = {}

    async def has_live_consumer(self, capability: str) -> bool:
        """True if something that can actually serve this capability polled recently.

        Uses the shared `live_worker_consumers` filter so "live" means one thing across
        the coordinator. The cloud executor counts — on a cloud-only capability it is the
        only consumer there will ever be, and if it is draining the queue then no machine
        needs waking.

        The reaper deliberately does NOT count, and that exclusion is the point: its
        `XAUTOCLAIM` registers `cbk-reaper` on the group and refreshes its idle time on
        every scan (~60s, i.e. within `dead_ms`), so counting it made the coordinator's
        own bookkeeping look like a live worker and suppress the wake an urgent job was
        entitled to.
        """
        try:
            consumers = await self._r.xinfo_consumers(
                stream_key(capability), self._group
            )
        except redis.ResponseError:
            return False  # no group/stream ⇒ nobody home
        return bool(live_worker_consumers(
            consumers, dead_ms=self._dead_ms, include=(CLOUD_EXECUTOR_CONSUMER,)
        ))

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
            _log.info(
                "wake %s → %s (reason=%s)", capability, ", ".join(woken), reason
            )
        return woken
