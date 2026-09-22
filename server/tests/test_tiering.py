"""Urgency-tiered streams (ADR 34) — implementing what ADR 24 deferred.

Urgency used to govern only *whether capacity gets created* (whether a machine is woken),
never position within a capability's stream. Meanwhile the documentation promised that
`necessary` gets "head of the async queues" — so a client whose urgent job sat behind a
patient backlog on a warm worker was seeing documented behaviour that did not exist.

The risk in fixing it is the rollout: a worker built before tiering reads only the base
stream, so an urgent-tier write while such a node is enrolled would strand the job. Most
of these tests are about that.
"""

from __future__ import annotations

import json

import pytest

from clusterbuck.queue import (
    TIER_ORDER,
    URGENT_TIER,
    Queue,
    stream_key,
    tier_for,
)
from clusterbuck.store import Store
from clusterbuck.tiering import move_to_urgent_tier, tiering_ready

CAP = "8b-extract"
GROUP = "cbk-workers"


@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(str(tmp_path / "tier.db"))


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


class _Hw:
    ram_gb, accelerator, vram_gb, disk_free_gb, bench_tps_small = 16.0, "cpu", None, 100.0, None


class _Req:
    """The shape `Store.enroll_node` reads off an enrolment request."""

    def __init__(self, hostname: str) -> None:
        self.hostname, self.os, self.arch, self.profile = hostname, "linux", "arm64", "shared"
        self.hw = _Hw()


def _enrol(store: Store, node_id: str, *, tier_aware: bool,
           capabilities: list[str] | None = None) -> None:
    """Enrol a node and give it a heartbeat, through the real store API.

    `queues` is the evidence the gate reads, so it is what distinguishes an old worker
    from a new one here — exactly as it does in production.
    """
    caps = capabilities if capabilities is not None else [CAP]
    store.enroll_node(node_id=node_id, node_key=f"k-{node_id}", req=_Req(node_id),
                      capabilities=json.dumps(caps), enrolled_at="t")
    queues = [stream_key(c, t) for c in caps for t in TIER_ORDER] if tier_aware \
        else [stream_key(c) for c in caps]
    store.record_heartbeat(
        node_id=node_id, mode="active", installed="[]", loaded="[]",
        queues=json.dumps(queues), jobs_done=0, tps=None, last_heartbeat="t",
    )


# --- the topology ------------------------------------------------------------------


def test_tier_is_a_pure_function_of_urgency():
    """Both classes that may DEMAND capacity share the urgent tier (ADR 18); waitable —
    and anything unrecognised — takes the base stream."""
    assert tier_for("urgent") == URGENT_TIER
    assert tier_for("necessary") == URGENT_TIER
    assert tier_for("waitable") is None
    assert tier_for(None) is None
    assert tier_for("something-new") is None


def test_the_base_stream_keeps_its_historical_name():
    """A deployed worker addresses `q:<cap>`; renaming it would strand every job."""
    assert stream_key(CAP) == "q:8b-extract"
    assert stream_key(CAP, None) == "q:8b-extract"
    assert stream_key(CAP, URGENT_TIER) == "q:8b-extract:urgent"


async def test_capability_discovery_strips_the_tier_suffix(queue):
    """Untaught, this returns `8b-extract:urgent` as a capability of its own — and the
    reaper, which iterates it, would reclaim those entries under a bogus name and requeue
    them onto the BASE stream, silently demoting the escalated work."""
    await queue.ensure_group(CAP, None)
    await queue.ensure_group(CAP, URGENT_TIER)
    assert await queue.known_capabilities() == [CAP]


async def test_depth_sums_work_but_does_not_double_count_workers(queue):
    """One worker is a consumer on BOTH groups, so summing consumers would report a fleet
    twice its real size — re-creating the inflated count this reporting exists to fix."""
    for tier in TIER_ORDER:
        await queue.ensure_group(CAP, tier)
        await queue.enqueue({
            "id": f"job_{tier}", "created_at": "t", "capability": CAP, "prompt": "x",
            "params": {}, "urgency": "waitable", "privacy": "local_only",
            "result_key": f"res_{tier}", "attempts": 0, "max_attempts": 3,
        }, tier=tier)
        await queue.client.xreadgroup(
            GROUP, "node-alpha", {stream_key(CAP, tier): ">"}, count=1)

    stats = await queue.depth(CAP, GROUP)
    assert stats["depth"] == 2, "work sums across tiers"
    assert stats["pending"] == 2
    assert stats["consumers"] == 1, "the same node, seen on two groups"


# --- the rollout gate --------------------------------------------------------------


def test_no_enrolled_node_means_no_tiering(store):
    """Vacuous truth is the trap: "every live node is tier-aware" is trivially true on a
    cold fleet with nothing awake — and the asleep node is exactly the one that will wake
    up and claim the job."""
    assert tiering_ready(store, None, CAP) is False


def test_a_tier_aware_node_opens_the_gate(store):
    _enrol(store, "node-new", tier_aware=True)
    assert tiering_ready(store, None, CAP) is True


def test_one_old_node_keeps_the_gate_shut(store):
    """The compatibility case. A node that does not read the urgent stream would never
    see the job, so a single one is enough to hold tiering back for that capability."""
    _enrol(store, "node-new", tier_aware=True)
    _enrol(store, "node-old", tier_aware=False)
    assert tiering_ready(store, None, CAP) is False


def test_the_gate_is_per_capability(store):
    """An old node serving something else must not hold back a capability it cannot run."""
    _enrol(store, "node-new", tier_aware=True)
    _enrol(store, "node-other", tier_aware=False, capabilities=["32b-reason"])
    assert tiering_ready(store, None, CAP) is True


def test_the_gate_can_be_forced_either_way(store):
    _enrol(store, "node-old", tier_aware=False)
    assert tiering_ready(store, None, CAP, mode="on") is True
    _enrol(store, "node-new", tier_aware=True)
    assert tiering_ready(store, None, CAP, mode="off") is False


def test_a_node_that_has_never_heartbeated_is_not_tier_aware(store):
    """Enrolment alone proves nothing about what the worker reads — the evidence is the
    `queues` it reports, and a node that has not reported any has given none."""
    store.enroll_node(node_id="node-quiet", node_key="k", req=_Req("quiet"),
                      capabilities=json.dumps([CAP]), enrolled_at="t")
    assert tiering_ready(store, None, CAP) is False


# --- moving a promoted job ----------------------------------------------------------


async def test_an_unclaimed_promoted_entry_moves_tier(store, queue):
    entry = await queue.enqueue({
        "id": "job_move", "created_at": "t", "capability": CAP, "prompt": "x",
        "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": "res_move", "attempts": 0, "max_attempts": 3,
    })
    store.insert(id="job_move", result_key="res_move", capability=CAP, created_at="t")
    store.record_delivery("job_move", stream=stream_key(CAP), entry_id=entry)

    moved = await move_to_urgent_tier(store, queue, store.get("job_move"), group=GROUP)
    assert moved is True

    assert await queue.client.xlen(stream_key(CAP)) == 0, "gone from the base stream"
    assert await queue.client.xlen(stream_key(CAP, URGENT_TIER)) == 1
    row = store.get("job_move")
    assert row.stream == stream_key(CAP, URGENT_TIER)
    assert row.entry_id is not None


async def test_the_payload_survives_the_move(store, queue):
    """The payload lives on the stream, not in SQLite, so the entry has to be read before
    it is withdrawn — withdrawing first would destroy the job."""
    original = {
        "id": "job_p", "created_at": "t", "capability": CAP,
        "messages": [{"role": "user", "content": "keep me"}],
        "params": {"temperature": 0.2}, "urgency": "necessary",
        "privacy": "local_only", "result_key": "res_p", "attempts": 0, "max_attempts": 3,
    }
    entry = await queue.enqueue(original)
    store.insert(id="job_p", result_key="res_p", capability=CAP, created_at="t")
    store.record_delivery("job_p", stream=stream_key(CAP), entry_id=entry)

    await move_to_urgent_tier(store, queue, store.get("job_p"), group=GROUP)

    entries = await queue.client.xrange(stream_key(CAP, URGENT_TIER))
    assert json.loads(entries[0][1]["job"]) == original


async def test_a_claimed_entry_is_never_duplicated(store, queue):
    """A worker is already running it. Copying it to the urgent tier would run the job
    twice — far worse than serving it once in submission order."""
    await queue.ensure_group(CAP)
    entry = await queue.enqueue({
        "id": "job_held", "created_at": "t", "capability": CAP, "prompt": "x",
        "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": "res_held", "attempts": 0, "max_attempts": 3,
    })
    store.insert(id="job_held", result_key="res_held", capability=CAP, created_at="t")
    store.record_delivery("job_held", stream=stream_key(CAP), entry_id=entry)
    await queue.client.xreadgroup(GROUP, "node-alpha", {stream_key(CAP): ">"}, count=1)

    assert await move_to_urgent_tier(
        store, queue, store.get("job_held"), group=GROUP) is False
    assert await queue.client.xlen(stream_key(CAP, URGENT_TIER)) == 0


async def test_a_job_with_no_recorded_delivery_is_left_alone(store, queue):
    store.insert(id="job_nod", result_key="res_nod", capability=CAP, created_at="t")
    assert await move_to_urgent_tier(
        store, queue, store.get("job_nod"), group=GROUP) is False


async def test_moving_twice_is_a_no_op(store, queue):
    entry = await queue.enqueue({
        "id": "job_twice", "created_at": "t", "capability": CAP, "prompt": "x",
        "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": "res_twice", "attempts": 0, "max_attempts": 3,
    })
    store.insert(id="job_twice", result_key="res_twice", capability=CAP, created_at="t")
    store.record_delivery("job_twice", stream=stream_key(CAP), entry_id=entry)

    assert await move_to_urgent_tier(
        store, queue, store.get("job_twice"), group=GROUP) is True
    assert await move_to_urgent_tier(
        store, queue, store.get("job_twice"), group=GROUP) is False
    assert await queue.client.xlen(stream_key(CAP, URGENT_TIER)) == 1


# --- the move is atomic, and refuses to touch a claimed entry (A3) -------------------


def _wire(job_id: str) -> dict:
    """A minimal job payload, matching contract/job.schema.json's required fields."""
    return {
        "id": job_id, "created_at": "t", "capability": CAP, "prompt": "x",
        "params": {}, "urgency": "necessary", "privacy": "local_only",
        "result_key": f"res_{job_id.removeprefix('job_')}", "attempts": 0,
        "max_attempts": 3,
    }


async def test_a_claimed_entry_is_left_completely_alone(store, queue):
    """Not just "not moved" — not DELETED either, which is the part that could lose a job.

    Escalation fires on any due `waitable` whose result blob is absent, and
    `due_for_escalation` excludes only terminal statuses — `running` is not one. So a job a
    worker is actively generating is reachable here. The obvious primitive, `withdraw`,
    deletes first and probes after (correct for a cancellation, where guaranteeing
    non-delivery is the point): that removed a running job's stream entry, and Redis 7's
    XAUTOCLAIM drops a pending row whose entry is gone, so losing that worker afterwards
    left the job with no blob, no status and nothing to recover it.
    """
    store.insert(id="job_a", result_key="res_a", capability=CAP, created_at="t",
                 urgency="necessary")
    entry_id = await queue.enqueue(_wire("job_a"))
    store.record_delivery("job_a", stream=stream_key(CAP), entry_id=entry_id)
    # A worker claims it and is now mid-inference.
    await queue.client.xreadgroup(GROUP, "node-busy", {stream_key(CAP): ">"}, count=1)

    row = store.get("job_a")
    assert await move_to_urgent_tier(store, queue, row, group=GROUP) is False

    # The entry is still there, still deliverable, still claimable by the reaper.
    assert await queue.read_entry(stream_key(CAP), entry_id) is not None
    assert [c["entry_id"] for c in await queue.claims(CAP, GROUP)] == [entry_id]
    # And the row still points at it, so cancellation and queue position still work.
    row = store.get("job_a")
    assert (row.stream, row.entry_id) == (stream_key(CAP), entry_id)
    # Nothing was copied to the urgent tier.
    assert await queue.client.xlen(stream_key(CAP, URGENT_TIER)) == 0


async def test_the_delivery_is_cleared_before_the_entry_moves(store, queue):
    """Ordering, because it is the whole of what makes a crash mid-move recoverable.

    The move is one atomic step in Redis now, so the entry cannot vanish between two
    streams. What remains is the gap between a successful move and re-recording it in
    SQLite — and `entry_id IS NULL` is what the orphan sweep selects, which it resolves by
    LOOKING for the entry and adopting it. Recording the clear afterwards instead would
    leave the row naming a deleted entry: a state no sweep selects and nothing repairs.

    Observed by watching the row at the instant the move is issued, which is the only way
    to pin an ordering without actually killing the process.
    """
    store.insert(id="job_a", result_key="res_a", capability=CAP, created_at="t",
                 urgency="necessary")
    entry_id = await queue.enqueue(_wire("job_a"))
    store.record_delivery("job_a", stream=stream_key(CAP), entry_id=entry_id)

    seen: dict = {}
    real = queue.move_if_unclaimed

    async def watching(src, dst, eid, payload, *, group):
        row = store.get("job_a")
        seen["entry_id"], seen["stream"] = row.entry_id, row.stream
        return await real(src, dst, eid, payload, group=group)

    queue.move_if_unclaimed = watching
    try:
        assert await move_to_urgent_tier(
            store, queue, store.get("job_a"), group=GROUP) is True
    finally:
        queue.move_if_unclaimed = real

    assert seen == {"entry_id": None, "stream": None}, \
        "the row must already be cleared when the move is issued, not after it returns"
    # And it is re-recorded once the move succeeds.
    row = store.get("job_a")
    assert row.stream == stream_key(CAP, URGENT_TIER)
    assert await queue.read_entry(row.stream, row.entry_id) is not None


async def test_a_move_that_finds_nothing_leaves_the_delivery_cleared(store, queue):
    """`gone` deliberately does NOT restore the old id.

    The entry really is not there — trimmed away unserved, or lost between the read and
    the move. Restoring the id would leave the row pointing at a deleted entry, which the
    orphan sweep does not select; cleared, the sweep looks for it, does not find it, and
    reports honestly that it cannot be recovered. `claimed` is the opposite case and does
    restore, because there the original entry is still live (asserted above).
    """
    store.insert(id="job_a", result_key="res_a", capability=CAP, created_at="t",
                 urgency="necessary")
    entry_id = await queue.enqueue(_wire("job_a"))
    store.record_delivery("job_a", stream=stream_key(CAP), entry_id=entry_id)

    async def vanished(src, dst, eid, payload, *, group):
        return "gone"

    queue.move_if_unclaimed = vanished
    assert await move_to_urgent_tier(
        store, queue, store.get("job_a"), group=GROUP) is False

    row = store.get("job_a")
    assert (row.entry_id, row.stream) == (None, None), \
        "a row naming a deleted entry is one nothing repairs; cleared, the sweep can act"


# --- a silent node abstains, it does not veto -----------------------------------------


def test_a_paused_node_does_not_switch_tiering_off_for_the_whole_capability(store):
    """`queues` is what a node says it is claiming from RIGHT NOW, so it is empty
    whenever the node is not claiming — which a paused one is not, and nor is one that
    cannot currently reach the broker. Reading that as "not tier-aware" meant `cbk pause`
    on a single modern worker silently switched urgent tiering off for its whole
    capability, and every urgent job then queued behind the patient backlog the tier
    exists to jump."""
    _enrol(store, "node-a", tier_aware=True)
    _enrol(store, "node-b", tier_aware=True)
    assert tiering_ready(store, None, CAP) is True

    store.record_heartbeat(node_id="node-b", mode="paused", installed="[]", loaded="[]",
                           queues=json.dumps([]), jobs_done=0, tps=None,
                           last_heartbeat="t")

    assert tiering_ready(store, None, CAP) is True, \
        "an abstaining node must not veto what the others have proved"


def test_an_old_worker_that_is_actually_claiming_still_vetoes(store):
    """The gate's real job is unchanged: a node demonstrably reading only the base
    stream would strand an urgent-tier job, so it still shuts the gate."""
    _enrol(store, "node-a", tier_aware=True)
    _enrol(store, "node-old", tier_aware=False)
    assert tiering_ready(store, None, CAP) is False


def test_a_capability_whose_nodes_are_all_silent_stays_untiered(store):
    """No evidence either way is the same answer as an empty fleet. The one node might
    be an old build, and nothing has said otherwise."""
    _enrol(store, "node-a", tier_aware=True)
    store.record_heartbeat(node_id="node-a", mode="paused", installed="[]", loaded="[]",
                           queues=json.dumps([]), jobs_done=0, tps=None,
                           last_heartbeat="t")
    assert tiering_ready(store, None, CAP) is False
