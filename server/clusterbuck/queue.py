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

    async def depth(self, capability: str, group: str) -> dict[str, int]:
        """Per-capability queue stats: total entries, pending (claimed, unacked), consumers."""
        key = stream_key(capability)
        entries = await self._r.xlen(key)
        pending, consumers = 0, 0
        try:
            summary = await self._r.xpending(key, group)
            pending = int(summary["pending"]) if summary else 0
            consumers = len(await self._r.xinfo_consumers(key, group))
        except redis.ResponseError:
            pass  # no group yet
        return {"depth": entries, "pending": pending, "consumers": consumers}

    async def aclose(self) -> None:
        await self._r.aclose()
