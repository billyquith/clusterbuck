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
from redis.exceptions import RedisError, ResponseError

from .config import WorkerConfig
from .model_client import ModelClient
from .models import Job, Result
from .naming import artifact_aliases

# Urgency tiers, mirroring the coordinator's queue.py (ADR 34). The urgent stream is read
# ahead of the base one, so demanding work no longer waits behind a patient backlog on a
# worker that is already awake. `tier=None` is the historical name, so a coordinator that
# does not tier keeps addressing exactly the stream it always did.
URGENT_TIER = "urgent"
TIER_ORDER: tuple[str | None, ...] = (URGENT_TIER, None)


# How many recent completions the tokens/sec figure is taken over. Long enough to shrug
# off one slow generation, short enough to track a model swap or a machine under load.
_TPS_WINDOW = 20

# Cold loads are rare by nature — a model comes up once and serves many jobs — so the
# window is short, and a median of three still beats one unlucky sample.
_LOAD_WINDOW = 5


def stream_key(capability: str, tier: str | None = None) -> str:
    return f"q:{capability}:{URGENT_TIER}" if tier == URGENT_TIER else f"q:{capability}"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class Evicted(BaseException):
    """The owner took the machine back while this job was running (ADR 10).

    Derived from `BaseException`, not `Exception`, and that is the whole point of the
    class. `_process` turns any `Exception` into a terminal `failed` result — correct for
    a job that genuinely cannot be served, and catastrophic for this one: an evicted job
    has not failed, it has been interrupted, and it must go back to its queue. Sharing an
    ancestor with the handler that would answer it is a single misplaced `except` away
    from telling a client their job failed because someone sat down at a laptop.

    `asyncio.CancelledError` is `BaseException` for exactly this reason, and this is the
    same signal wearing a name that says which cancellation it was.
    """


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
        # Which model those samples describe. A window that survives a model swap reports
        # the OLD model's speed as the new one's, which is wrong in both directions: it
        # flatters a big model that just landed and libels a small one that replaced it.
        self._tps_model: str | None = None
        # Seconds to bring a model up from cold, recovered from a cold job's wall time.
        self._load_samples: deque[float] = deque(maxlen=_LOAD_WINDOW)
        # Models the inventory last reported RESIDENT. Empty means the adapter could not
        # say (LM Studio, llama.cpp), which is unknown, not "nothing loaded" — and unknown
        # is never evidence that a job was a cold start.
        self._resident: frozenset[str] | None = None
        # The model calls currently in flight, and the ones `evict` cancelled. Held as
        # TASK OBJECTS rather than a boolean flag so the two can be compared by identity:
        # a flag set just as a job finished would still be standing when the next job
        # started, and that job would then read a completion of its own as an eviction.
        #
        # Sets rather than single slots because a node may run more than one job at a
        # time (`max_concurrent_jobs`). An eviction is the owner taking the machine back,
        # so it takes ALL of them — cancelling only the most recent would leave the
        # person waiting on whatever else happened to be generating.
        self._inflight: set[asyncio.Task] = set()
        self._evicted_tasks: set[asyncio.Task] = set()
        # The per-job processing tasks, which is what slot accounting counts. Distinct
        # from `_inflight`: a job holds a slot for its whole life, including the result
        # write and the ack, while it is only in `_inflight` during the model call.
        self._running: set[asyncio.Task] = set()
        # Whether the broker is answering. Reported on the heartbeat by emptying `queues`
        # — a node that cannot reach Redis is claiming from nothing, and saying otherwise
        # would have the coordinator suppress a wake on this node's behalf.
        self.broker_ok = True
        # Whose machine this is, and whether they are at it. Drives `effective_limit`.
        # Defaults are the cautious pair: unknown profile, owner present.
        self._mode: str = "active"
        self._profile: str | None = None

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
        median = statistics.median(self._tps_samples)
        # Two decimals, and never a zero. Rounding a genuinely slow node's rate to one
        # decimal produced 0.0 on a live fleet — which every consumer then reads as a
        # MEASUREMENT of zero rather than the absence of one. A node that cannot be shown
        # to generate anything has no rate, which is the None case.
        return round(median, 2) or None

    @property
    def load_s(self) -> float | None:
        """Median seconds to bring this node's model up from cold; None until sampled.

        Reported as `stats.load_s`, and what the coordinator's reservation pre-warm needs:
        the lead time before a window is storage speed times model size, which no probe of
        either alone can predict. Absent on any model server that cannot report what is
        resident — coldness has to be PROVEN, not assumed, or every job on such a node
        would be recorded as a cold start.
        """
        if not self._load_samples:
            return None
        return round(statistics.median(self._load_samples), 1)

    def set_resident(self, loaded: Iterable[str]) -> None:
        """Record which models the inventory reports warm. Empty stays `unknown`."""
        seen: set[str] = set()
        for artifact in loaded:
            seen |= artifact_aliases(artifact)
        self._resident = frozenset(seen) if seen else None

    def _was_cold(self, model: str) -> bool:
        """Positive evidence this model was NOT resident when the job arrived."""
        if self._resident is None:
            return False  # the adapter cannot say; absence of evidence is not evidence
        return not (artifact_aliases(model) & self._resident)

    def _record_load(self, usage: dict | None, elapsed_s: float) -> None:
        """Recover the load share of a cold job's wall time.

        A cold completion is load plus generation on one clock, and the API gives no way
        to separate them — so subtract the generation this node's own measured throughput
        accounts for. That needs an EXISTING tps for the same model: the very first job
        after a swap has none, and using the previous model's median would attribute its
        speed to a different artifact, which is precisely the case this measurement exists
        for. Such a sample is skipped rather than guessed.
        """
        steady = self.tps
        if steady is None or not usage or elapsed_s <= 0:
            return
        out = usage.get("completion_tokens")
        if not isinstance(out, (int, float)) or isinstance(out, bool) or out <= 0:
            return
        load = elapsed_s - (out / steady)
        # A cold job faster than steady state means the model was already warm and the
        # inventory was stale. Recording ~0 would drag the median toward "instant" and
        # under-warm every future reservation, so drop it.
        if load > 0:
            self._load_samples.append(load)

    def set_installed(self, artifacts: Iterable[str]) -> None:
        """Record the local inventory, expanded to every spelling of each artifact."""
        seen: set[str] = set()
        for artifact in artifacts:
            seen |= artifact_aliases(artifact)
        self.installed = frozenset(seen)

    def _refuse_reason(self, job: Job) -> str | None:
        """Why this job cannot be honoured here, or None.

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

    def evict(self) -> bool:
        """Stop the job running right now. True if there was one to stop.

        The owner's half of `cbk pause` (ADR 10): pausing already stops this node CLAIMING
        new work, but a 30B generation started a minute ago would otherwise hold the
        machine for minutes more, which is not what a person reaching for their own laptop
        means by "pause".

        Deliberately leaves the entry **unacked with no result**, so the job returns to its
        queue through the visibility timeout exactly as an abandoned one does — that path
        is already built and already tested, and writing a `failed` result here would turn
        an interruption into a terminal answer the client cannot retry.

        What this does NOT promise: that the model server stops generating. Cancelling
        closes the connection (verified: the server sees the client hang up mid-response),
        so the worker stops waiting immediately and the job is released — but whether the
        server abandons the generation behind it is the server's business. Freeing the RAM
        is the separate job of `ModelManager.unload`.
        """
        live = {t for t in self._inflight if not t.done()}
        if not live:
            return False
        # ALL of them. An eviction is a person reaching for their own laptop; leaving one
        # generation running because it was not the most recent would hold the GPU for
        # exactly as long as the one we did stop.
        self._evicted_tasks |= live
        for task in live:
            task.cancel()
        return True

    async def _complete(self, job: Job) -> tuple[dict, dict | None]:
        """Run the model call as a task, so `evict` has something to cancel.

        Translates a cancellation WE caused into `Evicted`, and lets any other one through
        untouched: a genuine shutdown must stay a `CancelledError` or the process would
        stop exiting when asked. Membership of `_evicted_tasks` is what distinguishes
        them, by task identity — see the note on those sets in `__init__`.
        """
        task = asyncio.ensure_future(self._model.complete(job))
        self._inflight.add(task)
        try:
            return await task
        except asyncio.CancelledError:
            if task in self._evicted_tasks:
                raise Evicted() from None
            raise
        finally:
            self._inflight.discard(task)
            self._evicted_tasks.discard(task)

    def set_presence(self, mode: str | None, profile: str | None) -> None:
        """Tell the loop whose machine this is and whether they are at it.

        Fed by the heartbeat task from the ladder's EFFECTIVE mode (already damped by the
        presence hysteresis) and the node's enrolled profile.
        """
        self._mode = mode or "active"
        self._profile = profile

    @property
    def effective_limit(self) -> int:
        """How many jobs this node may run at once, right now.

        `max_concurrent_jobs` is a ceiling the operator sets for the hardware; this is
        what the machine's situation allows of it. A flat number cannot be right for both
        a dedicated box that exists to serve and a laptop somebody is using, and the
        profile is exactly the distinction the rest of the system already draws for model
        pulls and for eviction (design.md §12, `commands.yields_to_a_person`). This is
        that same line applied to a third resource: attention.

        * **dedicated** — nobody is waiting for this machine, so it runs the full
          ceiling. Holding slots back would be the same pure loss as dropping its weights
          on a pause.
        * **shared / background / unknown** — the machine is somebody's. At `away` it
          looks idle and the ladder is already climbing to heavier models, so take the
          ceiling; at `active` a person is using it and the node takes one job at a time,
          which is what it always did. Unknown profile yields, for the reason
          `yields_to_a_person` gives: being wrong that way costs throughput, the other
          way costs somebody their laptop.
        * **paused** — nothing, either way. Claiming stops; what is in hand finishes or
          is evicted, depending on the profile.

        No hysteresis of its own, deliberately. The mode arriving here has already been
        damped by the presence ladder, which exists precisely so a coffee break does not
        thrash an expensive decision; adding a second damper on top would make the node
        slow to give the machine back, which is the one direction that must stay
        immediate.
        """
        ceiling = max(1, self._cfg.max_concurrent_jobs)
        if self._mode == "paused":
            return 0
        if self._profile == "dedicated":
            return ceiling
        return ceiling if self._mode == "away" else 1

    @property
    def free_slots(self) -> int:
        """How many more jobs this node may take right now."""
        return max(0, self.effective_limit - len(self._running))

    def _spawn(self, cap: str, entry_id: str, fields: dict, *, tier: str | None) -> None:
        """Run one job as its own task, holding a slot until it is completely done.

        The slot covers the result write and the ack, not just the model call: a job
        whose generation has finished is still occupying memory and still owes an
        answer, so releasing its slot early would let the node over-commit.
        """
        task = asyncio.ensure_future(self._process(cap, entry_id, fields, tier=tier))
        self._running.add(task)
        task.add_done_callback(self._running.discard)

    async def poll_once(self) -> bool:
        """Claim what this node has room for and start it. True if it claimed anything.

        Jobs are STARTED here, not awaited. `_process` used to be awaited inline, which
        made the node strictly serial across every capability it served — so a slow job
        on one capability blocked even the URGENT tier of another, defeating the tier on
        the one node that was awake. At `max_concurrent_jobs = 1` the behaviour is
        unchanged, because `run` waits for the slot before polling again.
        """
        if self.paused or self.effective_limit <= 0:
            return False
        did_work = False
        # One job per capability per pass, rather than draining a stream before looking at
        # the next: that ordering is what keeps a busy small-model queue from starving the
        # big-model queue this node also serves.
        for cap in self._capabilities:
            # Out of slots, but capabilities still unvisited: WAIT for one rather than
            # abandoning the pass. Breaking here instead would quietly undo the fairness
            # the loop above exists for — at a limit of 1 the first capability would take
            # the only slot every pass and a second capability's queue would never be
            # read at all. Waiting reproduces exactly what awaiting each job inline used
            # to do: claim one, run it, move to the next capability.
            if self.free_slots <= 0:
                await self._wait_for_slot()
                if self.paused:
                    break
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
                        self._spawn(cap, entry_id, fields, tier=tier)
                if took:
                    break
        return did_work

    async def _wait_for_slot(self) -> None:
        """Block until at least one running job finishes, or return if none are."""
        if not self._running:
            return
        await asyncio.wait(tuple(self._running), return_when=asyncio.FIRST_COMPLETED)

    async def drain(self) -> None:
        """Wait for every job in flight to finish. For shutdown and for tests."""
        while self._running:
            await asyncio.gather(*tuple(self._running), return_exceptions=True)

    async def run(self, stop: asyncio.Event) -> None:
        await self.ensure_groups()
        self._log(f"cbk worker {self._cfg.worker_id} serving "
                  f"[{', '.join(self._capabilities)}] → model {self._cfg.model_name} "
                  f"@ {self._cfg.model_server_url}")
        while not stop.is_set():
            # A broker outage no longer kills this loop, because the heartbeat can now
            # tell the truth about it.
            #
            # It used to, deliberately: the coordinator treats a recent heartbeat naming
            # a capability's streams as proof somebody is serving them and suppresses the
            # wake a queued job is owed (server wake.py, `nodes_serving`). A worker that
            # survived a dead broker kept heartbeating over HTTP — a different connection,
            # still healthy — while claiming nothing, and so vouched for a queue it could
            # not read. Exiting was what took the heartbeat down with it, and silent
            # starvation is worse than a restart loop.
            #
            # `broker_ok` closes that instead, without the restart: the heartbeat reports
            # an empty `queues` while this is false, which is the literal truth (a node
            # that cannot reach Redis is claiming from nothing) and which the coordinator
            # already reads as "not serving". Staying up is strictly better — the node
            # remains visible on the dashboard, its `mode` and inventory keep flowing, and
            # it resumes the instant the broker returns rather than after a supervisor
            # backoff.
            #
            # Only a genuine outage reaches here: `broker.connect` absorbs a transient
            # failure inside the command, ten retries over roughly ten seconds.
            try:
                did_work = await self.poll_once()
            except RedisError as e:
                if self.broker_ok:          # log the edge, not every retry
                    self._log(f"broker unreachable: {e} — not claiming, and saying so "
                              f"on the next heartbeat")
                self.broker_ok = False
                did_work = False
            else:
                if not self.broker_ok:
                    self._log("broker is back — claiming again")
                self.broker_ok = True
            if did_work and self.free_slots > 0:
                continue
            # Nothing to claim, or no room to put it. Either way, wait — but wake on
            # whichever comes first: shutdown, the poll interval, or a running job
            # finishing and freeing its slot.
            #
            # That last one is what keeps a full node responsive. Sleeping out the poll
            # interval regardless would add up to `poll_s` of idle GPU to every job on a
            # saturated node; spinning instead would burn a core polling Redis. At
            # `max_concurrent_jobs = 1` this reduces to the old behaviour: claim one,
            # wait for it, repeat.
            waiters = [asyncio.ensure_future(stop.wait())]
            if self._running and self.free_slots <= 0:
                waiters.append(asyncio.ensure_future(
                    asyncio.wait(tuple(self._running),
                                 return_when=asyncio.FIRST_COMPLETED)))
            try:
                await asyncio.wait(waiters, timeout=self._cfg.poll_s,
                                   return_when=asyncio.FIRST_COMPLETED)
            finally:
                for w in waiters:
                    w.cancel()

        # Shutting down: the jobs already claimed keep their slots until they answer, so
        # they are not silently abandoned to the reaper on a graceful stop.
        await self.drain()

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
        # Which artifact this job actually runs on — the pin if it carries one, else this
        # node's configured model. Decided BEFORE the call, because both measurements below
        # belong to that artifact and the answer changes when a job pins something else.
        served = str((job.params or {}).get("model") or self._cfg.model_name)
        cold = self._was_cold(served)
        # Whether this job has the machine to itself for its whole run. Sampled at both
        # ends: one job in flight now, and still only this one when it finishes.
        solo_at_start = len(self._running) <= 1
        if served != self._tps_model:
            # A throughput window that survives a model swap reports the old model's speed
            # as the new one's — flattering a big model that just landed, libelling a small
            # one that replaced it, and poisoning the load estimate that subtracts it.
            self._tps_samples.clear()
            self._tps_model = served
        try:
            refusal = self._refuse_reason(job)
            if refusal is not None:
                raise RuntimeError(refusal)
            completion, usage = await self._complete(job)
            elapsed = time.monotonic() - t0
            solo = solo_at_start and len(self._running) <= 1
            result = Result(job_id=job.id, status="done", worker=self._cfg.worker_id,
                            started_at=started_at, finished_at=_now_iso(),
                            completion=completion, usage=usage)
            self.jobs_done += 1
            # Only a job that ran ALONE is a measurement.
            #
            # Both figures are wall-clock divided by tokens, so a job that shared the
            # accelerator with another reads as slower than the node is — and `load_s`
            # is derived by subtracting generation from wall time using `tps`, so a
            # deflated tps inflates every load estimate built on it, which in turn
            # over-warms every reservation. Under concurrency the honest answer is fewer
            # samples, not faster-looking ones. At a limit of 1 this is always true and
            # nothing changes.
            #
            # `solo` is captured before the awaits above, because by now other jobs may
            # have started or finished; what matters is whether this one had the machine
            # to itself while it ran.
            if not solo:
                pass
            elif cold:
                # Load first: it needs the throughput measured BEFORE this job, since
                # this job's own rate includes the load it is trying to isolate.
                self._record_load(usage, elapsed)
            else:
                self._record_tps(usage, elapsed)
            self._log(f"done  {job.id} [{capability}]")
        # Evicted FIRST, and it is not an `Exception` at all, so the handler below cannot
        # reach it however this is later edited. Nothing is written and nothing is acked:
        # the entry stays in the pending list and the coordinator's reaper requeues it,
        # which is precisely the recovery path a worker that died mid-job already uses.
        except Evicted:
            self._log(f"evict {job.id} [{capability}]: the owner took the machine back — "
                      f"returned to its queue for another node")
            return
        # Any failure becomes a terminal `failed` result rather than an exception that kills
        # the loop: the caller is waiting on a result key and deserves an answer either way.
        except Exception as e:
            result = Result(job_id=job.id, status="failed", worker=self._cfg.worker_id,
                            started_at=started_at, finished_at=_now_iso(),
                            error=str(e) or e.__class__.__name__)
            self._log(f"fail  {job.id} [{capability}]: {e}")

        # Result first, ack second. Acking first would let a crash in between drop the job
        # from the pending list with no result anywhere — invisible to the reaper.
        #
        # And FIRST WRITER WINS (`nx`), because this node may no longer be the one serving
        # this job. A machine that sleeps mid-inference is not dead: the coordinator's
        # reaper reclaims the entry after `reaper_min_idle_ms` and another worker answers,
        # and then this one wakes hours later, finishes the generation it was in the middle
        # of, and writes. Unconditionally, that overwrote a terminal answer a client had
        # already read — or replaced a dead-letter with a `done`, making a job the client
        # was told had failed silently succeed. Both copies are valid answers to the same
        # job, so the tie goes to whichever landed first and terminal stays terminal.
        wrote = await self._redis.set(job.result_key, json.dumps(result.to_wire()),
                                      ex=self._cfg.result_ttl_s, nx=True)
        if not wrote:
            self._log(f"stale {job.id} [{capability}]: already answered elsewhere while "
                      f"this node was away — discarding this copy")
        # Acked either way. The ack is very likely a no-op (the reclaiming reaper acked the
        # old entry when it requeued), but leaving the entry pending if it is not would
        # give the reaper something to churn on for a job that already has an answer.
        await self._redis.xack(
            stream_key(capability, tier), self._cfg.consumer_group, entry_id)


def queue_names(capabilities: Sequence[str]) -> list[str]:
    """Every stream this worker reads, both tiers.

    Reported on the heartbeat, where it doubles as the coordinator's rollout evidence: a
    node listing a `:urgent` stream is demonstrably able to serve one, which is what lets
    the coordinator start tiering without stranding an older worker's jobs.
    """
    return [stream_key(c, t) for c in capabilities for t in TIER_ORDER]
