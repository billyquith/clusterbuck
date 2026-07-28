"""The pull-based worker loop (protocols.md §2, ADR 2/20).

Reads jobs from each capability stream under the shared consumer group, runs them on the
local model server, writes the terminal result to the result store, and acknowledges.

A job abandoned mid-run (worker died before XACK) is recovered by the coordinator's
XAUTOCLAIM reaper (server/clusterbuck/reaper.py); this worker acks on success *and* on
failure, because a failed job has a terminal result and must not be redelivered.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .config import WorkerConfig
from .model_client import ModelClient
from .models import Job, Result


def stream_key(capability: str) -> str:
    return f"q:{capability}"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class WorkLoop:
    def __init__(self, redis: Redis, model: ModelClient, cfg: WorkerConfig,
                 log: Callable[[str], None] | None = None) -> None:
        self._redis = redis
        self._model = model
        self._cfg = cfg
        self._log = log or print
        self._capabilities: tuple[str, ...] = tuple(cfg.capabilities)
        # Set by the heartbeat task when the owner is present or the coordinator quarantines
        # this build. In-flight jobs already claimed still finish; nothing new is claimed.
        self.paused = False
        self.jobs_done = 0

    @property
    def capabilities(self) -> tuple[str, ...]:
        return self._capabilities

    async def set_capabilities(self, caps: Iterable[str]) -> None:
        """Swap the served capability set, ensuring groups for any new ones."""
        caps = tuple(caps)
        for cap in caps:
            await self._ensure_group(cap)
        self._capabilities = caps

    async def ensure_groups(self) -> None:
        for cap in self._capabilities:
            await self._ensure_group(cap)

    async def _ensure_group(self, cap: str) -> None:
        """Create the consumer group (and stream) for a capability. Idempotent."""
        try:
            await self._redis.xgroup_create(
                stream_key(cap), self._cfg.consumer_group, id="$", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def poll_once(self) -> bool:
        """Read at most one job per capability and process it. True if it did work."""
        if self.paused:
            return False
        did_work = False
        # One job per capability per pass, rather than draining a stream before looking at
        # the next: that ordering is what keeps a busy small-model queue from starving the
        # big-model queue this node also serves.
        for cap in self._capabilities:
            entries = await self._redis.xreadgroup(
                self._cfg.consumer_group, self._cfg.worker_id,
                {stream_key(cap): ">"}, count=1)
            for _stream, messages in entries or []:
                for entry_id, fields in messages:
                    did_work = True
                    await self._process(cap, entry_id, fields)
        return did_work

    async def run(self, stop: asyncio.Event) -> None:
        await self.ensure_groups()
        self._log(f"cbk worker {self._cfg.worker_id} serving "
                  f"[{', '.join(self._capabilities)}] → model {self._cfg.model_name} "
                  f"@ {self._cfg.model_server_url}")
        while not stop.is_set():
            did_work = await self.poll_once()
            if did_work:
                continue
            # Idle: wait out the poll interval, but wake immediately on shutdown.
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._cfg.poll_s)
            except TimeoutError:
                pass

    async def _process(self, capability: str, entry_id: str,
                       fields: dict[str, str]) -> None:
        raw = fields.get("job")
        if raw is None:
            await self._redis.xack(stream_key(capability), self._cfg.consumer_group, entry_id)
            return

        job = Job.from_wire(json.loads(raw))
        now = _now_iso()
        try:
            completion, usage = await self._model.complete(job)
            result = Result(job_id=job.id, status="done", worker=self._cfg.worker_id,
                            completed_at=now, completion=completion, usage=usage)
            self.jobs_done += 1
            self._log(f"done  {job.id} [{capability}]")
        # Any failure becomes a terminal `failed` result rather than an exception that kills
        # the loop: the caller is waiting on a result key and deserves an answer either way.
        except Exception as e:
            result = Result(job_id=job.id, status="failed", worker=self._cfg.worker_id,
                            completed_at=now, error=str(e) or e.__class__.__name__)
            self._log(f"fail  {job.id} [{capability}]: {e}")

        # Result first, ack second. Acking first would let a crash in between drop the job
        # from the pending list with no result anywhere — invisible to the reaper.
        await self._redis.set(job.result_key, json.dumps(result.to_wire()),
                              ex=self._cfg.result_ttl_s)
        await self._redis.xack(stream_key(capability), self._cfg.consumer_group, entry_id)


def queue_names(capabilities: Sequence[str]) -> list[str]:
    return [stream_key(c) for c in capabilities]
