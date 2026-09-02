"""In-process execution of cloud-backed jobs (ADR 30).

A provider account (Anthropic, OpenAI, …) is registered on the coordinator with an API key
held by the gateway (model-evaluation.md → Provider accounts, budget, and the cost-quality
loop) — never a worker. So a job routed to a no-host cloud capability (`cloud: true`,
`model_server: null`, fleet.py) is never handed to a worker's queue at all: the coordinator
drains that capability's stream itself and calls the provider directly via LiteLLM, exactly
the "coordinator-side proxy worker that pulls from queues on the endpoint's behalf" shape
ADR 12 already established for attached endpoints. A cloud provider is that shape exactly —
no fabric code, no host node.

This mirrors worker/src/cbk_worker/work_loop.py's WorkLoop closely on purpose: same stream
contract (XREADGROUP under the shared consumer group, write-result-then-ack), same reaper
recovery if this process dies mid-call (server/clusterbuck/reaper.py — a missed ack here is
recovered exactly like an abandoned worker job, ADR 20). The two are not shared code (ADR 7:
server and worker meet only at the queue contract + HTTP) — this is a second, independent
implementation of the same protocol, in the language that already holds the keys.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime

import litellm

from .fleet import Fleet, resolve_api_key
from .models import JobRecord
from .queue import Queue

_log = logging.getLogger("clusterbuck.cloud")

# A fixed consumer name is fine: XREADGROUP identifies a consumer by (stream, group, name),
# and this executor never runs more than one instance per coordinator process.
CONSUMER_ID = "cbk-cloud-executor"

# Mirrors worker/src/cbk_worker/model_client.py's _PARAMS_NOT_FORWARDED: `stream` and
# `messages` are the request envelope, not inference parameters. `api_key`/`api_base` are
# excluded so a job's `params` (client- or eval-harness-supplied) can never redirect this
# call to a different endpoint or override which key it authenticates with — the key is
# resolved from the capability's own `api_key_env` alone, per job, never from the wire.
_PARAMS_NOT_FORWARDED = {"stream", "messages", "response_format", "api_key", "api_base"}


def cloud_capabilities(fleet: Fleet | None) -> list[str]:
    """No-host cloud capabilities (fleet-management.md: "no host node") this executor
    drains — the registered provider accounts, as opposed to a hosted OpenAI-compatible
    `cloud: true` capability that still names a `model_server` a real worker can call."""
    if fleet is None:
        return []
    return [name for name, spec in fleet.capabilities.items()
            if spec.cloud and spec.model_server is None]


def provider_of(model: str) -> str:
    """The provider name from a LiteLLM "<provider>/<model>" identifier, for the usage
    row's `node` column (orm/usage.py: "worker id, or 'cloud:<provider>'")."""
    return model.split("/", 1)[0] if "/" in model else "cloud"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class CloudExecutor:
    """Drains cloud-backed capability streams and calls the provider via LiteLLM.

    Deliberately shaped like WorkLoop (same run/poll_once/ensure_groups split) so the two
    read as one pattern applied twice, not two unrelated designs.
    """

    def __init__(
        self, queue: Queue, fleet: Fleet, *, consumer_group: str,
        poll_s: float = 1.0, log: Callable[[str], None] | None = None,
    ) -> None:
        self._queue = queue
        self._fleet = fleet
        self._group = consumer_group
        self._poll_s = poll_s
        self._log = log or _log.info

    @property
    def capabilities(self) -> list[str]:
        return cloud_capabilities(self._fleet)

    async def ensure_groups(self) -> None:
        for cap in self.capabilities:
            await self._queue.ensure_group(cap)

    async def poll_once(self) -> bool:
        """Read at most one job per cloud capability and process it. True if it did work."""
        did_work = False
        for cap in self.capabilities:
            entry = await self._queue.read_one(cap, self._group, CONSUMER_ID)
            if entry is None:
                continue
            did_work = True
            entry_id, raw = entry
            await self._process(cap, entry_id, raw)
        return did_work

    async def run(self, stop: asyncio.Event) -> None:
        await self.ensure_groups()
        if self.capabilities:
            self._log(f"cloud executor draining [{', '.join(self.capabilities)}]")
        while not stop.is_set():
            if await self.poll_once():
                continue
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._poll_s)
            except asyncio.TimeoutError:
                pass

    async def _process(self, capability: str, entry_id: str, raw: dict) -> None:
        job = JobRecord.from_wire(raw)
        spec = self._fleet.capabilities[capability]
        model = (job.params or {}).get("model") or spec.model
        provider = provider_of(model)
        # Around the call, not before it — same correction as the worker's loop.
        started_at = _now_iso()

        try:
            api_key = resolve_api_key(spec)
            if not api_key:
                raise RuntimeError(
                    f"no API key configured for {capability!r} "
                    f"(set {spec.api_key_env or '?'})"
                )
            if job.messages:
                messages = [{"role": m.role, "content": m.content} for m in job.messages]
            else:
                messages = [{"role": "user", "content": job.prompt or ""}]

            # `model` set first, `params` applied after — a job may pin the exact artifact
            # under test (the eval harness does, model-evaluation.md), and that pin must
            # win over this capability's own default (model_client.py's identical ordering).
            request: dict = {"model": model, "messages": messages, "stream": False}
            for name, value in (job.params or {}).items():
                if name in _PARAMS_NOT_FORWARDED:
                    continue
                request[name] = value

            resp = await litellm.acompletion(api_key=api_key, **request)
            body = resp.model_dump()
            result = {
                "job_id": job.id, "status": "done", "worker": f"cloud:{provider}",
                "started_at": started_at, "finished_at": _now_iso(),
                "completed_at": started_at,  # deprecated alias, see result.schema.json
                "completion": body, "usage": body.get("usage"),
            }
        except Exception as e:  # a terminal `failed` result beats killing this loop
            result = {
                "job_id": job.id, "status": "failed", "worker": f"cloud:{provider}",
                "started_at": started_at, "finished_at": _now_iso(),
                "completed_at": started_at,  # deprecated alias, see result.schema.json
                "error": str(e) or e.__class__.__name__,
            }
            self._log(f"cloud fail {job.id} [{capability}]: {e}")

        # Result first, ack second — identical ordering to work_loop.py, for the identical
        # reason: acking first would let a crash in between drop the job from the pending
        # list with a result nowhere, invisible to the reaper.
        await self._queue.write_result(job.result_key, result)
        await self._queue.ack(capability, self._group, entry_id)
