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
import statistics
import time
from collections import deque
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .config import WorkerConfig
from .model_client import ModelClient
from .models import Job, Result

# Urgency tiers, mirroring the coordinator's queue.py (ADR 34). The urgent stream is read
# ahead of the base one, so demanding work no longer waits behind a patient backlog on a
# worker that is already awake. `tier=None` is the historical name, so a coordinator that
# does not tier keeps addressing exactly the stream it always did.
URGENT_TIER = "urgent"
TIER_ORDER: tuple[str | None, ...] = (URGENT_TIER, None)


# How many recent completions the tokens/sec figure is taken over. Long enough to shrug
# off one slow generation, short enough to track a model swap or a machine under load.
_TPS_WINDOW = 20


def stream_key(capability: str, tier: str | None = None) -> str:
    return f"q:{capability}:{URGENT_TIER}" if tier == URGENT_TIER else f"q:{capability}"


def artifact_aliases(name: str) -> set[str]:
    """Spellings that mean the same artifact to a model server.

    Ollama reports an explicit tag on `/v1/models` while a registry commonly omits it, so
    `llama3.2` and `llama3.2:latest` are one artifact. Mirrors the coordinator's own
    normalisation (server fleet.py) — the two must agree, or a pin the coordinator believes
    valid is refused here.
    """
    name = (name or "").strip()
    base = name.split(":", 1)[0]
    return {name, base, f"{base}:latest"}


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
        # What the local model server actually has, refreshed by the heartbeat task from
        # the same inventory it reports. Empty means UNKNOWN (the server did not answer),
        # not empty — so a pin is only ever refused on positive evidence.
        self.installed: frozenset[str] = frozenset()
        self.jobs_done = 0
        # Output tokens/sec, sampled per completed job. A short window rather than a
        # lifetime mean: the first job after a cold model load runs dramatically slower
        # than steady state, and an average would carry that outlier forever.
        self._tps_samples: deque[float] = deque(maxlen=_TPS_WINDOW)

    @property
    def tps(self) -> float | None:
        """Median output tokens/sec over recent jobs; None until the first sample.

        Reported on the heartbeat as `stats.tps`, which the coordinator already persists.
        This measures the (model, machine) PAIRING, which no model-level ability score can
        express: the same artifact on a GPU box and a CPU box scores identically on
        ability and performs nothing alike. Median, not mean, so one cold start or one
        unusually long generation does not define the node.
        """
        if not self._tps_samples:
            return None
        return round(statistics.median(self._tps_samples), 1)

    def set_installed(self, artifacts: Iterable[str]) -> None:
        """Record the local inventory, expanded to every spelling of each artifact."""
        seen: set[str] = set()
        for artifact in artifacts:
            seen |= artifact_aliases(artifact)
        self.installed = frozenset(seen)

    def _refuse_reason(self, job: Job) -> str | None:
        """Why this job's pinned artifact cannot be honoured here, or None.

        The coordinator pins the artifact whose ability cleared the job's `min_ability`
        bar. Running it on a different model would answer the job at a quality nobody
        checked while reporting success — the silent under-serve the pin exists to end. So
        a pin this node cannot serve becomes a FAILED result with a reason, which is
        visible, instead of a plausible answer, which is not.

        Silent when the inventory is unknown: a model server that did not answer must not
        take the whole node offline.
        """
        pinned = (job.params or {}).get("model")
        if not pinned or not self.installed:
            return None
        if artifact_aliases(str(pinned)) & self.installed:
            return None
        return (f"job pinned artifact {pinned!r}, which is not installed on this node "
                f"(serving {self._cfg.model_name!r}). Refusing to answer with a different "
                f"model than the one its ability bar was checked against.")

    def _record_tps(self, usage: dict | None, elapsed_s: float) -> None:
        """Sample tokens/sec when the model server reported enough to compute it.

        OUTPUT tokens only. Prompt tokens are consumed by a prefill pass whose cost has
        little to do with generation speed, so counting them would flatter a long-prompt
        job and make the figure describe the workload rather than the machine.
        """
        if not usage or elapsed_s <= 0:
            return
        out = usage.get("completion_tokens")
        if not isinstance(out, (int, float)) or isinstance(out, bool) or out <= 0:
            return
        self._tps_samples.append(out / elapsed_s)

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
        """Create the consumer group (and stream) for a capability, both tiers. Idempotent."""
        for tier in TIER_ORDER:
            try:
                await self._redis.xgroup_create(
                    stream_key(cap, tier), self._cfg.consumer_group, id="$", mkstream=True)
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
            # Urgent tier first, base only if it had nothing. Note this stays INSIDE the
            # one-job-per-capability discipline: the tier loop breaks as soon as it takes
            # a job, so a busy urgent stream cannot drain to empty while another
            # capability this node serves waits.
            for tier in TIER_ORDER:
                entries = await self._redis.xreadgroup(
                    self._cfg.consumer_group, self._cfg.worker_id,
                    {stream_key(cap, tier): ">"}, count=1)
                took = False
                for _stream, messages in entries or []:
                    for entry_id, fields in messages:
                        did_work = took = True
                        await self._process(cap, entry_id, fields, tier=tier)
                if took:
                    break
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
                       fields: dict[str, str], *, tier: str | None = None) -> None:
        raw = fields.get("job")
        if raw is None:
            await self._redis.xack(
                stream_key(capability, tier), self._cfg.consumer_group, entry_id)
            return

        job = Job.from_wire(json.loads(raw))
        # Stamped around the model call, not once before it. The single timestamp this
        # replaced was taken here and written as `completed_at`, so it was really a
        # processing-START time — wrong by a whole inference, and the only completion
        # time the system had.
        started_at = _now_iso()
        # Monotonic: wall-clock strings would be corrupted by an NTP step or a DST change
        # mid-inference, and a negative elapsed would poison the sample.
        t0 = time.monotonic()
        try:
            refusal = self._refuse_reason(job)
            if refusal is not None:
                raise RuntimeError(refusal)
            completion, usage = await self._model.complete(job)
            result = Result(job_id=job.id, status="done", worker=self._cfg.worker_id,
                            started_at=started_at, finished_at=_now_iso(),
                            completion=completion, usage=usage)
            self.jobs_done += 1
            self._record_tps(usage, time.monotonic() - t0)
            self._log(f"done  {job.id} [{capability}]")
        # Any failure becomes a terminal `failed` result rather than an exception that kills
        # the loop: the caller is waiting on a result key and deserves an answer either way.
        except Exception as e:
            result = Result(job_id=job.id, status="failed", worker=self._cfg.worker_id,
                            started_at=started_at, finished_at=_now_iso(),
                            error=str(e) or e.__class__.__name__)
            self._log(f"fail  {job.id} [{capability}]: {e}")

        # Result first, ack second. Acking first would let a crash in between drop the job
        # from the pending list with no result anywhere — invisible to the reaper.
        await self._redis.set(job.result_key, json.dumps(result.to_wire()),
                              ex=self._cfg.result_ttl_s)
        await self._redis.xack(
            stream_key(capability, tier), self._cfg.consumer_group, entry_id)


def queue_names(capabilities: Sequence[str]) -> list[str]:
    """Every stream this worker reads, both tiers.

    Reported on the heartbeat, where it doubles as the coordinator's rollout evidence: a
    node listing a `:urgent` stream is demonstrably able to serve one, which is what lets
    the coordinator start tiering without stranding an older worker's jobs.
    """
    return [stream_key(c, t) for c in capabilities for t in TIER_ORDER]
