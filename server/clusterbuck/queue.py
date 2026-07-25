"""The Redis queue contract (protocols.md §2), on Redis Streams + consumer groups (ADR 20).

One stream per capability, `q:<capability>`. The server `XADD`s jobs; workers read via
`XREADGROUP` under a shared consumer group so each job goes to exactly one worker; the
worker `XACK`s after writing the result. The pending-entries list + `XAUTOCLAIM` give the
visibility-timeout / reaper semantics natively (the reaper itself lands in M2).

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
        """Ensure the group exists, then XADD the job. Returns the stream entry id."""
        capability = job["capability"]
        await self.ensure_group(capability)
        return await self._r.xadd(
            stream_key(capability), {"job": json.dumps(job)}
        )

    async def read_result(self, result_key: str) -> dict[str, Any] | None:
        raw = await self._r.get(result_key)
        return json.loads(raw) if raw is not None else None

    async def aclose(self) -> None:
        await self._r.aclose()
