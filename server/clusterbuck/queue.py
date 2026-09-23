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

# Urgency tiers (ADR 34, implementing ADR 24's deferral). Two streams per capability:
# `q:<cap>:urgent` is drained ahead of `q:<cap>`, so once a worker is awake, demanding work
# no longer waits behind a patient backlog. Read order is exported so the coordinator, the
# worker and the cloud executor cannot disagree about it.
URGENT_TIER = "urgent"
TIER_ORDER: tuple[str | None, ...] = (URGENT_TIER, None)
_TIER_SUFFIX = f":{URGENT_TIER}"


def stream_key(capability: str, tier: str | None = None) -> str:
    """The stream a capability's work of a given tier lives on.

    `tier=None` is the base stream and the historical name, so every existing caller and
    every deployed worker keeps addressing exactly the stream it always did.
    """
    return f"q:{capability}{_TIER_SUFFIX}" if tier == URGENT_TIER else f"q:{capability}"


def tier_for(urgency: str | None) -> str | None:
    """Which tier a job belongs on, from its urgency alone.

    `urgent` and `necessary` both demand capacity (ADR 18), so both go to the urgent tier;
    `waitable` — and anything unrecognised — takes the base stream. Keeping this a pure
    function of urgency means every enqueue path agrees without duplicating the rule.
    """
    return URGENT_TIER if urgency in ("urgent", "necessary") else None


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

    async def ensure_group(self, capability: str, tier: str | None = None) -> None:
        """Create the consumer group (and stream) if absent. Idempotent."""
        key = stream_key(capability, tier)
        try:
            await self._r.xgroup_create(
                name=key, groupname=settings.consumer_group, id="$", mkstream=True
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def enqueue(self, job: dict[str, Any], *, tier: str | None = None) -> str:
        """Ensure the group exists, then XADD the job. Returns the stream entry id.

        Trimmed with an approximate MAXLEN so the broker cannot grow without bound; the cap
        is far above any sane in-flight depth, so it only ever discards long-acked history.

        `tier` defaults to None — the base stream — rather than being derived from the
        job's urgency here, because whether tiering is safe depends on the *fleet*: a
        worker that predates it reads only the base stream, so an urgent-tier write during
        a rollout would strand the job. The caller consults that gate and passes the answer.
        """
        capability = job["capability"]
        await self.ensure_group(capability, tier)
        return await self._r.xadd(
            stream_key(capability, tier), {"job": json.dumps(job)},
            maxlen=settings.stream_maxlen, approximate=True,
        )

    async def reclaim_stale(
        self, capability: str, group: str, *, min_idle_ms: int, consumer: str = "cbk-reaper",
        count: int = 50, tier: str | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """XAUTOCLAIM entries whose claiming worker has gone quiet.

        Returns [(entry_id, job)] for entries now owned by `consumer`. `min_idle_ms` must
        exceed the longest plausible inference, or a worker that is simply busy would have
        its job stolen and re-run.
        """
        key = stream_key(capability, tier)
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

    async def ack(
        self, capability: str, group: str, entry_id: str, *, tier: str | None = None
    ) -> None:
        await self._r.xack(stream_key(capability, tier), group, entry_id)

    async def read_one(
        self, capability: str, group: str, consumer: str, *, tier: str | None = None
    ) -> tuple[str, dict[str, Any]] | None:
        """XREADGROUP one new entry for `capability`, or None if there isn't one.

        Used by the coordinator's own cloud executor (ADR 30) — the same consumption
        primitive a worker uses over the wire (worker/src/cbk_worker/work_loop.py), just
        called in-process because that executor lives in this Python process too.
        """
        key = stream_key(capability, tier)
        try:
            entries = await self._r.xreadgroup(group, consumer, {key: ">"}, count=1)
        except redis.ResponseError:
            # No such stream or group. Normal rather than exceptional now that there are
            # two tiers: a capability may have a base stream and no urgent one (nothing
            # urgent has ever been submitted), and reading the absent tier must simply
            # find nothing — the same tolerance `reclaim_stale` has.
            return None
        for _stream, messages in entries or []:
            for entry_id, fields in messages:
                raw = fields.get("job")
                if raw is None:
                    await self._r.xack(key, group, entry_id)
                    continue
                return entry_id, json.loads(raw)
        return None

    async def write_result(
        self, result_key: str, result: dict[str, Any], *, only_if_absent: bool = False
    ) -> bool:
        """Write a job's terminal result. True if this call is the one that wrote it.

        `only_if_absent` makes it first-writer-wins, for an EXECUTOR: a job can legitimately
        be running twice (a worker that stalled long enough for the reaper to requeue it
        still finishes its own copy when it comes back), and the loser must not overwrite an
        answer a client may already have read. Terminal should mean terminal.

        The coordinator's own terminalising paths — the reaper's dead-letter and the
        backstops — deliberately pass False. The reaper already checks `read_result` first,
        and the backstops exist precisely to write an answer where nothing else will.
        """
        wrote = await self._r.set(
            result_key, json.dumps(result), ex=settings.result_ttl_s,
            **({"nx": True} if only_if_absent else {}),
        )
        return bool(wrote)

    async def known_capabilities(self) -> list[str]:
        """Capabilities with a live stream, discovered from Redis rather than config.

        The reaper must cover streams that exist because a client submitted to them, not
        only those named in fleet.yaml.

        Tier suffixes are stripped and the result deduped. Untaught, this would return
        `8b-extract:urgent` as if it were a capability of its own — and the reaper, which
        iterates what this returns, would then reclaim those entries under a bogus
        capability name and requeue them onto the *base* stream, silently demoting the
        escalated work the tier exists to prioritise.
        """
        keys = [k async for k in self._r.scan_iter(match="q:*", count=100)]
        names = set()
        for key in keys:
            name = key[2:]
            name = name.removesuffix(_TIER_SUFFIX)
            names.add(name)
        return sorted(names)

    async def read_result(self, result_key: str) -> dict[str, Any] | None:
        raw = await self._r.get(result_key)
        return json.loads(raw) if raw is not None else None

    async def find_entry_for_job(
        self, capability: str, job_id: str, *, count: int = 10_000
    ) -> tuple[str, str] | None:
        """Locate a job's stream entry by scanning, returning `(stream, entry_id)`.

        The slow path, and only used where the fast one is unavailable: a row whose
        delivery predates delivery tracking has `entry_id IS NULL`, which is *unknown*,
        not *never enqueued*. Without this, every job in flight when the coordinator is
        upgraded would be swept away as an orphan — the deploy itself would fail live work.

        Bounded by the stream cap (`CBK_STREAM_MAXLEN`) and normally run against nothing:
        candidate rows are usually zero.
        """
        for tier in TIER_ORDER:
            stream = stream_key(capability, tier)
            entries = await self._r.xrange(stream, count=count)
            for entry_id, fields in entries or []:
                raw = (fields or {}).get("job")
                if not raw:
                    continue
                try:
                    if json.loads(raw).get("id") == job_id:
                        return (stream, entry_id)
                except ValueError:
                    continue
        return None

    # Move a queued entry between streams, atomically, and only if no consumer holds it.
    #
    # A script rather than three round-trips because the entry's payload lives ONLY on the
    # stream — SQLite has no `messages`/`prompt`/`params` columns — so a coordinator that
    # died between the XDEL and the XADD destroyed the job outright. It was then invisible
    # to every recovery path at once: not in any pending list (so the reaper cannot see
    # it), and not `entry_id IS NULL` (so the orphan sweep does not select it either).
    #
    # The pending check is inside the same script for a second reason. The obvious
    # primitive, `withdraw`, deletes FIRST and probes after, deliberately — for a
    # cancellation, guaranteeing non-delivery is the whole point. A move wants the
    # opposite: if the entry cannot be proven free, leave it completely alone, deletion
    # included. Probing first from Python cannot deliver that (a worker can claim the
    # entry in the gap, and then the delete orphans a running job's PEL row, which Redis
    # 7's XAUTOCLAIM drops — see orm/job.py's note on `cancel_requested`); probing first
    # inside Lua can, because nothing else runs in between.
    #
    # Returns the new entry id, or the literal "claimed" / "gone". Ids are `<ms>-<seq>`,
    # so neither sentinel can be mistaken for one.
    _MOVE_IF_UNCLAIMED = """
    local held = redis.pcall('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
    if type(held) == 'table' and held.err == nil and #held > 0 then return 'claimed' end
    if redis.call('XDEL', KEYS[1], ARGV[2]) ~= 1 then return 'gone' end
    return redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[3], '*', 'job', ARGV[4])
    """

    async def move_if_unclaimed(
        self, src: str, dst: str, entry_id: str, payload: dict[str, Any], *, group: str
    ) -> str:
        """Move one entry from `src` to `dst` in a single atomic step.

        Returns the new entry id on success, or `"claimed"` (a consumer holds it, so it
        will run where it is) or `"gone"` (already trimmed, or never there). Both
        non-success outcomes leave the source stream untouched.

        The destination group is ensured first: XADD creates the stream but not the group,
        and an entry written to a stream nobody is grouped on would be delivered to nobody.
        """
        try:
            await self._r.xgroup_create(
                name=dst, groupname=group, id="$", mkstream=True
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        return await self._r.eval(
            self._MOVE_IF_UNCLAIMED, 2, src, dst,
            group, entry_id, str(settings.stream_maxlen), json.dumps(payload),
        )

    async def read_entry(self, stream: str, entry_id: str) -> dict[str, Any] | None:
        """The job payload of one specific stream entry, or None if it is not there.

        Needed to MOVE an entry between tiers: the payload is not stored in SQLite
        (`messages`/`prompt`/`params` are not columns), so it has to be read off the
        stream before the entry goes anywhere — `move_if_unclaimed` is handed the payload
        this returns, because the move deletes the source entry in the same atomic step.
        """
        entries = await self._r.xrange(stream, min=entry_id, max=entry_id, count=1)
        for _id, fields in entries or []:
            raw = (fields or {}).get("job")
            if raw is None:
                return None
            try:
                return json.loads(raw)
            except ValueError:
                return None
        return None

    async def withdraw(self, stream: str, entry_id: str, *, group: str) -> str:
        """Try to take a queued entry back off a stream. Never lies about the outcome.

        Returns:
          * `"deleted"` — the entry was removed and no consumer holds it, so it is
            **provably** never going to run.
          * `"claimed"` — a consumer already has it; it will finish whatever we do.
          * `"gone"`    — no such entry (already trimmed by `MAXLEN ~`, or never there).

        XDEL first, then probe the pending list, and that order matters: deleting first
        makes any *future* delivery impossible, so the probe afterwards cannot be beaten
        by a worker reading the entry in the gap.

        The distinction cannot be made from XDEL alone. `XACK` does not remove an entry
        from a stream, so a job that already ran and was acked looks exactly like one
        never delivered — which is why callers must check the result blob before trusting
        `"deleted"`.
        """
        removed = await self._r.xdel(stream, entry_id)
        try:
            held = await self._r.xpending_range(
                stream, group, min=entry_id, max=entry_id, count=1
            )
        except redis.ResponseError:
            held = []  # no group ⇒ nobody can be holding it
        if held:
            return "claimed"
        return "deleted" if int(removed or 0) else "gone"

    async def claims(
        self, capability: str, group: str, *, count: int = 200, tier: str | None = None
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
                stream_key(capability, tier), group, min="-", max="+", count=count
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

    async def group_info(
        self, capability: str, group: str, *, tier: str | None = None
    ) -> dict[str, Any] | None:
        """This capability's consumer-group record, or None if the group doesn't exist."""
        try:
            groups = await self._r.xinfo_groups(stream_key(capability, tier))
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
        tier: str | None = None,
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
        info = await self.group_info(capability, group, tier=tier)
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
            stream_key(capability, tier), min=f"({last}", max=upper, count=count + 1
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

        Aggregation across the urgency tiers is per field, not uniform: work counts
        **sum**, but `consumers` takes the **max**. One worker is a consumer on both
        groups, so summing would double-count the fleet and re-create the inflated count
        this reporting exists to fix.
        """
        totals = {"depth": 0, "pending": 0, "backlog": 0}
        live_names: set[str] = set()
        executor_names: set[str] = set()

        for tier in TIER_ORDER:
            key = stream_key(capability, tier)
            totals["depth"] += await self._r.xlen(key)
            consumers: list[dict[str, Any]] = []
            try:
                summary = await self._r.xpending(key, group)
                totals["pending"] += int(summary["pending"]) if summary else 0
                consumers = await self._r.xinfo_consumers(key, group)
            except redis.ResponseError:
                pass  # no group on this tier yet
            backlog, _capped = await self.undelivered(capability, group, tier=tier)
            totals["backlog"] += backlog
            # Counted by NAME across tiers, which is what makes this a max rather than a
            # sum: the same worker appears on both groups under the same id.
            live_names |= {
                c["name"] for c in
                live_worker_consumers(consumers, dead_ms=settings.worker_dead_ms)
            }
            executor_names |= {
                c["name"] for c in live_worker_consumers(
                    consumers, dead_ms=settings.worker_dead_ms,
                    include=(CLOUD_EXECUTOR_CONSUMER,))
                if c.get("name") == CLOUD_EXECUTOR_CONSUMER
            }

        return {
            **totals,
            "consumers": len(live_names),
            "executors": len(executor_names),
        }

    async def aclose(self) -> None:
        await self._r.aclose()
