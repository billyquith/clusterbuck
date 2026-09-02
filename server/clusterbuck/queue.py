"""The Redis queue contract (protocols.md §2), on Redis Streams + consumer groups (ADR 20).

One stream per capability, `q:<capability>`. The server `XADD`s jobs; workers read via
`XREADGROUP` under a shared consumer group so each job goes to exactly one worker; the
worker `XACK`s after writing the result. The pending-entries list + `XAUTOCLAIM` give the
visibility-timeout / reaper semantics natively — that is the whole reason ADR 20 chose
Streams over `BLPOP` lists, and `reaper.py` is where it is applied.

Streams are trimmed on write (`MAXLEN ~`): an unbounded stream keeps every prompt ever
submitted resident in Redis, and `XACK` does not remove entries.

Result blobs are written by the worker to a plain Redis key (`result_key`) with a TTL.
"""

from __future__ import annotations

import json
from typing import Any

import redis.asyncio as redis

from .config import settings


def stream_key(capability: str) -> str:
    return f"q:{capability}"


# Consumers the COORDINATOR registers on the shared group. They appear in
# XINFO CONSUMERS exactly like a worker does, which is why an unfiltered consumer count
# reports "2 consumers" on a fleet with one machine — the reaper's XAUTOCLAIM registers
# `cbk-reaper` the first time it runs.
REAPER_CONSUMER = "cbk-reaper"
CLOUD_EXECUTOR_CONSUMER = "cbk-cloud-executor"

# How many entries ahead of a job we are willing to count. An XRANGE returns each entry's
# whole `job` field, so an uncapped scan would ship every prompt ahead of the caller on
# every poll. Past this the answer is reported as "at least N".
POSITION_SCAN_CAP = 100


def live_worker_consumers(
    consumers: list[dict[str, Any]], *, dead_ms: int, include: tuple[str, ...] = ()
) -> list[dict[str, Any]]:
    """Consumers that are actually workers and actually alive.

    Two filters, both load-bearing:

    * **idle < dead_ms** — nothing ever calls `XGROUP DELCONSUMER`, so a worker that died
      months ago is still listed. An unfiltered count reports a fleet that is not there.
    * **not a coordinator consumer** — `cbk-reaper` is bookkeeping, never work being done.
      `cbk-cloud-executor` is *sometimes* real work (it is the only consumer a cloud-only
      capability will ever have), so it is not excluded here; callers that want it pass it
      in `include` and decide how to present it.
    """
    excluded = {REAPER_CONSUMER, CLOUD_EXECUTOR_CONSUMER} - set(include)
    return [
        c for c in consumers
        if c.get("name") not in excluded
        and int(c.get("idle", dead_ms + 1)) < dead_ms
    ]


def _entry_le(a: str, b: str) -> bool:
    """Stream-id ordering: `a <= b`. Ids are `<ms>-<seq>`, both integers."""
    def parts(x: str) -> tuple[int, int]:
        ms, _, seq = x.partition("-")
        return (int(ms), int(seq or 0))
    return parts(a) <= parts(b)


class Queue:
    def __init__(self, client: redis.Redis) -> None:
        self._r = client

    @classmethod
    def from_url(cls, url: str | None = None) -> "Queue":
        return cls(redis.from_url(url or settings.redis_url, decode_responses=True))

    @property
    def client(self) -> redis.Redis:
        """The underlying Redis client (for coordinator reads like XINFO)."""
        return self._r

    async def ensure_group(self, capability: str) -> None:
        """Create the consumer group (and stream) if absent. Idempotent."""
        key = stream_key(capability)
        try:
            await self._r.xgroup_create(
                name=key, groupname=settings.consumer_group, id="$", mkstream=True
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def enqueue(self, job: dict[str, Any]) -> str:
        """Ensure the group exists, then XADD the job. Returns the stream entry id.

        Trimmed with an approximate MAXLEN so the broker cannot grow without bound; the cap
        is far above any sane in-flight depth, so it only ever discards long-acked history.
        """
        capability = job["capability"]
        await self.ensure_group(capability)
        return await self._r.xadd(
            stream_key(capability), {"job": json.dumps(job)},
            maxlen=settings.stream_maxlen, approximate=True,
        )

    async def reclaim_stale(
        self, capability: str, group: str, *, min_idle_ms: int, consumer: str = "cbk-reaper",
        count: int = 50,
    ) -> list[tuple[str, dict[str, Any]]]:
        """XAUTOCLAIM entries whose claiming worker has gone quiet.

        Returns [(entry_id, job)] for entries now owned by `consumer`. `min_idle_ms` must
        exceed the longest plausible inference, or a worker that is simply busy would have
        its job stolen and re-run.
        """
        key = stream_key(capability)
        try:
            _next, entries, _deleted = await self._r.xautoclaim(
                name=key, groupname=group, consumername=consumer,
                min_idle_time=min_idle_ms, count=count,
            )
        except redis.ResponseError:
            return []  # no such stream/group
        out: list[tuple[str, dict[str, Any]]] = []
        for entry_id, fields in entries or []:
            raw = fields.get("job") if fields else None
            if raw is None:
                # Unparseable/empty entry: ack it away rather than reclaim it forever.
                await self._r.xack(key, group, entry_id)
                continue
            try:
                out.append((entry_id, json.loads(raw)))
            except ValueError:
                await self._r.xack(key, group, entry_id)
        return out

    async def ack(self, capability: str, group: str, entry_id: str) -> None:
        await self._r.xack(stream_key(capability), group, entry_id)

    async def read_one(
        self, capability: str, group: str, consumer: str
    ) -> tuple[str, dict[str, Any]] | None:
        """XREADGROUP one new entry for `capability`, or None if there isn't one.

        Used by the coordinator's own cloud executor (ADR 30) — the same consumption
        primitive a worker uses over the wire (worker/src/cbk_worker/work_loop.py), just
        called in-process because that executor lives in this Python process too.
        """
        entries = await self._r.xreadgroup(group, consumer, {stream_key(capability): ">"}, count=1)
        for _stream, messages in entries or []:
            for entry_id, fields in messages:
                raw = fields.get("job")
                if raw is None:
                    await self._r.xack(stream_key(capability), group, entry_id)
                    continue
                return entry_id, json.loads(raw)
        return None

    async def write_result(self, result_key: str, result: dict[str, Any]) -> None:
        await self._r.set(result_key, json.dumps(result), ex=settings.result_ttl_s)

    async def known_capabilities(self) -> list[str]:
        """Capabilities with a live stream, discovered from Redis rather than config.

        The reaper must cover streams that exist because a client submitted to them, not
        only those named in fleet.yaml.
        """
        keys = [k async for k in self._r.scan_iter(match="q:*", count=100)]
        return sorted(k[2:] for k in keys)

    async def read_result(self, result_key: str) -> dict[str, Any] | None:
        raw = await self._r.get(result_key)
        return json.loads(raw) if raw is not None else None

    async def claims(
        self, capability: str, group: str, *, count: int = 200
    ) -> list[dict[str, Any]]:
        """Who is holding what, from the pending-entries list.

        The worker passes its own id as the XREADGROUP consumer name, so the PEL is where
        the coordinator can learn *which node claimed a job* and *when* — before any
        result exists. `idle_ms` is time since that entry was last delivered, so
        `now - idle_ms` is the true claim instant regardless of how late this is called.

        PEL rows vanish on XACK, so this drives a LIVE view only; it can never backfill a
        job that has already finished.
        """
        try:
            rows = await self._r.xpending_range(
                stream_key(capability), group, min="-", max="+", count=count
            )
        except redis.ResponseError:
            return []  # no group yet
        return [
            {
                "entry_id": r["message_id"],
                "consumer": r["consumer"],
                "idle_ms": int(r["time_since_delivered"]),
                "delivered": int(r["times_delivered"]),
            }
            for r in rows or []
        ]

    async def group_info(self, capability: str, group: str) -> dict[str, Any] | None:
        """This capability's consumer-group record, or None if the group doesn't exist."""
        try:
            groups = await self._r.xinfo_groups(stream_key(capability))
        except redis.ResponseError:
            return None
        for g in groups or []:
            if g.get("name") == group:
                return g
        return None

    async def undelivered(
        self,
        capability: str,
        group: str,
        *,
        before_entry_id: str | None = None,
        count: int = POSITION_SCAN_CAP,
    ) -> tuple[int, bool]:
        """Count entries no consumer has ever been handed — the real backlog.

        A group delivers `>` strictly in id order, so every entry at or below
        `last-delivered-id` has been handed out at least once and everything strictly
        above it never has. Counting that range is therefore exactly "queued and untouched
        by any worker" — which is what a client means by "how many are ahead of me", and
        what neither XLEN (retained history) nor the pending count (already claimed) can
        answer.

        `before_entry_id` bounds the count at the caller's own entry, giving its position.
        Returns `(count, capped)`; when `capped` the true figure is "at least count".
        """
        info = await self.group_info(capability, group)
        if info is None:
            return (0, False)
        last = info.get("last-delivered-id") or "0-0"

        # A caller at or below last-delivered-id has been delivered, so it is not queued
        # and has no position. Decided by comparing ids, never inferred from an empty
        # range: XRANGE with an inverted range also returns [], which would read as
        # "nobody ahead of you" for a job that is already running.
        if before_entry_id is not None and _entry_le(before_entry_id, last):
            return (0, False)

        upper = f"({before_entry_id}" if before_entry_id is not None else "+"
        entries = await self._r.xrange(
            stream_key(capability), min=f"({last}", max=upper, count=count + 1
        )
        found = len(entries or [])
        return (min(found, count), found > count)

    async def depth(self, capability: str, group: str) -> dict[str, int]:
        """Per-capability queue stats.

        Four numbers that are routinely conflated, so each says exactly one thing:

        * `backlog` — entries never delivered to anyone. **This is the backlog.**
        * `pending` — claimed but unacked, i.e. work in flight right now.
        * `depth` — `XLEN`: retained history, acked entries included, until `MAXLEN ~`
          trims them. Useful for capacity, useless as a backlog.
        * `consumers` / `executors` — live workers, and live coordinator-side executors.

        The distinction is not academic: never-delivered work appears in neither `pending`
        nor (usefully) `depth`, so a queue full of permanently stuck jobs reads as healthy
        on those two alone.
        """
        key = stream_key(capability)
        entries = await self._r.xlen(key)
        pending = 0
        consumers: list[dict[str, Any]] = []
        try:
            summary = await self._r.xpending(key, group)
            pending = int(summary["pending"]) if summary else 0
            consumers = await self._r.xinfo_consumers(key, group)
        except redis.ResponseError:
            pass  # no group yet
        backlog, _capped = await self.undelivered(capability, group)
        live = live_worker_consumers(consumers, dead_ms=settings.worker_dead_ms)
        executors = live_worker_consumers(
            consumers, dead_ms=settings.worker_dead_ms,
            include=(CLOUD_EXECUTOR_CONSUMER,),
        )
        return {
            "depth": entries,
            "pending": pending,
            "backlog": backlog,
            "consumers": len(live),
            "executors": len([c for c in executors
                              if c.get("name") == CLOUD_EXECUTOR_CONSUMER]),
        }

    async def aclose(self) -> None:
        await self._r.aclose()
