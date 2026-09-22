"""Wake coordinator: the decision to wake, against real Redis liveness. No packets sent."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
import redis.asyncio as aioredis

from clusterbuck.fleet import Fleet, NodeSpec
from clusterbuck.queue import (
    CLOUD_EXECUTOR_CONSUMER,
    REAPER_CONSUMER,
    URGENT_TIER,
    stream_key,
)
from clusterbuck.store import Store
from clusterbuck.wake import WakeCoordinator

CAP = "8b-extract"
GROUP = "cbk-workers"
MAC = "aa:bb:cc:dd:ee:ff"


def _fleet() -> Fleet:
    return Fleet(nodes=[NodeSpec(id="node-a", mac=MAC, capabilities=[CAP])])


@pytest.fixture()
async def rclient(redis_url):
    c = aioredis.from_url(redis_url, decode_responses=True)
    yield c
    await c.aclose()


async def test_wakes_when_no_live_consumer(rclient):
    sent: list[str] = []
    wake = WakeCoordinator(
        _fleet(), rclient, group=GROUP, send=lambda mac, **kw: sent.append(mac)
    )
    await rclient.xgroup_create(stream_key(CAP), GROUP, id="$", mkstream=True)

    woken = await wake.maybe_wake(CAP, reason="test")
    assert woken == ["node-a"]
    assert sent == [MAC]


async def test_no_wake_when_consumer_live(rclient):
    sent: list[str] = []
    wake = WakeCoordinator(
        _fleet(), rclient, group=GROUP, send=lambda mac, **kw: sent.append(mac)
    )
    await rclient.xgroup_create(stream_key(CAP), GROUP, id="$", mkstream=True)
    # An empty read registers a consumer with a fresh idle time (it's "polling").
    await rclient.xreadgroup(GROUP, "node-live", {stream_key(CAP): ">"}, count=1, block=10)

    assert await wake.maybe_wake(CAP, reason="test") == []
    assert sent == []


async def test_cooldown_coalesces(rclient):
    sent: list[str] = []
    t = {"now": 0.0}
    wake = WakeCoordinator(
        _fleet(), rclient, group=GROUP, cooldown_s=60,
        send=lambda mac, **kw: sent.append(mac), clock=lambda: t["now"],
    )
    await rclient.xgroup_create(stream_key(CAP), GROUP, id="$", mkstream=True)

    assert await wake.maybe_wake(CAP, reason="1") == ["node-a"]
    t["now"] = 30.0
    assert await wake.maybe_wake(CAP, reason="2") == []   # within cooldown → coalesced
    t["now"] = 90.0
    assert await wake.maybe_wake(CAP, reason="3") == ["node-a"]
    assert len(sent) == 2


async def test_no_fleet_no_wake(rclient):
    sent: list[str] = []
    wake = WakeCoordinator(None, rclient, group=GROUP, send=lambda mac, **kw: sent.append(mac))
    assert await wake.maybe_wake(CAP, reason="test") == []
    assert sent == []


async def test_the_coordinators_own_reaper_does_not_suppress_a_wake(rclient):
    """A latent bug this exclusion fixes, not a hypothetical.

    The reaper's `XAUTOCLAIM` registers `cbk-reaper` on the same consumer group and
    refreshes its idle time on every scan — which runs about once a minute, i.e. inside
    the default `dead_ms`. Treating any consumer as proof of life therefore let the
    coordinator's own bookkeeping stand in for a worker, and an urgent job that was
    entitled to wake a sleeping machine silently got nothing.
    """
    sent: list[str] = []
    wake = WakeCoordinator(
        _fleet(), rclient, group=GROUP, send=lambda mac, **kw: sent.append(mac)
    )
    await rclient.xgroup_create(stream_key(CAP), GROUP, id="$", mkstream=True)
    # Exactly what a reaper scan leaves behind: a registered consumer, freshly polled.
    await rclient.xreadgroup(
        GROUP, REAPER_CONSUMER, {stream_key(CAP): ">"}, count=1, block=10)

    assert await wake.maybe_wake(CAP, reason="test") == ["node-a"]
    assert sent == [MAC]


async def test_a_cloud_executor_does_count_as_someone_serving(rclient):
    """The mirror of the case above: on a cloud-backed capability the coordinator's
    executor is the only consumer there will ever be, so if it is draining the queue then
    waking a machine would be pointless."""
    sent: list[str] = []
    wake = WakeCoordinator(
        _fleet(), rclient, group=GROUP, send=lambda mac, **kw: sent.append(mac)
    )
    await rclient.xgroup_create(stream_key(CAP), GROUP, id="$", mkstream=True)
    await rclient.xreadgroup(
        GROUP, CLOUD_EXECUTOR_CONSUMER, {stream_key(CAP): ">"}, count=1, block=10)

    assert await wake.maybe_wake(CAP, reason="test") == []
    assert sent == []


# --- both liveness signals (B1) -------------------------------------------------------
#
# `has_live_consumer` used to read stream consumers on the BASE stream only, and nothing
# else. Two ways that reported a working fleet as absent, and both broadcast WoL at
# sleeping peers every cooldown window — in a design whose resting state is asleep.


class _Hw:
    ram_gb, accelerator, vram_gb, disk_free_gb, bench_tps_small = 16.0, "cpu", None, 100.0, None


class _Req:
    """The shape `Store.enroll_node` reads off an enrolment request."""

    def __init__(self, hostname: str) -> None:
        self.hostname, self.os, self.arch, self.profile = hostname, "linux", "arm64", "shared"
        self.hw = _Hw()


@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(str(tmp_path / "wake.db"))


def _iso(offset_s: float = 0.0) -> str:
    at = datetime.now(UTC) + timedelta(seconds=offset_s)
    return at.isoformat().replace("+00:00", "Z")


def _enrol(store: Store, node_id: str, *, queues: list[str], age_s: float = 0.0,
           capabilities: list[str] | None = None) -> None:
    """Enrol a node and give it one heartbeat, through the real store API."""
    store.enroll_node(node_id=node_id, node_key=f"k-{node_id}", req=_Req(node_id),
                      capabilities=json.dumps(capabilities or [CAP]), enrolled_at=_iso(-3600))
    store.record_heartbeat(
        node_id=node_id, mode="active", installed="[]", loaded="[]",
        queues=json.dumps(queues), jobs_done=0, tps=None, last_heartbeat=_iso(-age_s),
    )


async def test_a_consumer_on_the_urgent_tier_alone_counts_as_serving(rclient):
    """The tier half of the bug, and the one that hides the BUSIEST capability.

    A worker reads the urgent stream first and breaks out of the tier loop the moment it
    takes a job, so a capability under sustained urgent load never refreshes its
    base-group idle time. Reading only the base stream therefore reported exactly the
    capability that was working hardest as unserved.
    """
    sent: list[str] = []
    wake = WakeCoordinator(
        _fleet(), rclient, group=GROUP, send=lambda mac, **kw: sent.append(mac)
    )
    urgent = stream_key(CAP, URGENT_TIER)
    await rclient.xgroup_create(urgent, GROUP, id="$", mkstream=True)
    await rclient.xreadgroup(GROUP, "node-live", {urgent: ">"}, count=1, block=10)
    # The base stream exists with a group and NO live consumer — the state a worker busy
    # on the urgent tier leaves behind.
    await rclient.xgroup_create(stream_key(CAP), GROUP, id="$", mkstream=True)

    assert await wake.has_live_consumer(CAP) is True
    assert await wake.maybe_wake(CAP, reason="test") == []
    assert sent == []


async def test_a_heartbeat_alone_proves_a_node_is_serving(rclient, store):
    """The signal that survives a long inference.

    A worker mid-generation is not polling, so its consumer idle time climbs past
    `worker_dead_ms` (60s by default) while it is doing exactly the work the fleet exists
    for. Its heartbeat runs on its own task and is never blocked by the model call, so it
    keeps saying which streams it claims. There is deliberately no stream consumer at all
    here: the heartbeat has to carry this on its own.
    """
    sent: list[str] = []
    _enrol(store, "node-a", queues=[stream_key(CAP), stream_key(CAP, URGENT_TIER)])
    wake = WakeCoordinator(
        _fleet(), rclient, group=GROUP, store=store, silent_s=60,
        send=lambda mac, **kw: sent.append(mac),
    )

    assert await wake.has_live_consumer(CAP) is True
    assert await wake.maybe_wake(CAP, reason="test") == []
    assert sent == []


async def test_a_paused_node_is_awake_but_is_not_serving(rclient, store):
    """`queues`, not `mode`, and the distinction is the point.

    A paused node's ladder yields no capabilities, so it heartbeats an empty `queues`. It
    is plainly awake — but it will not claim anything, and suppressing the wake would
    leave the job waiting on a machine that has opted out. Some OTHER sleeping node is
    entitled to be woken for it.
    """
    sent: list[str] = []
    _enrol(store, "node-a", queues=[])
    wake = WakeCoordinator(
        _fleet(), rclient, group=GROUP, store=store, silent_s=60,
        send=lambda mac, **kw: sent.append(mac),
    )

    assert await wake.has_live_consumer(CAP) is False
    assert await wake.maybe_wake(CAP, reason="test") == ["node-a"]


async def test_a_stale_heartbeat_does_not_vouch_for_a_node(rclient, store):
    """The whole reason a registry row is not a liveness signal: nothing expires it."""
    sent: list[str] = []
    _enrol(store, "node-a", queues=[stream_key(CAP)], age_s=3600)
    wake = WakeCoordinator(
        _fleet(), rclient, group=GROUP, store=store, silent_s=60,
        send=lambda mac, **kw: sent.append(mac),
    )

    assert await wake.has_live_consumer(CAP) is False
    assert await wake.maybe_wake(CAP, reason="test") == ["node-a"]


async def test_a_heartbeat_for_a_neighbouring_capability_does_not_vouch(rclient, store):
    """Matched on the full stream name, so a capability that is a PREFIX of another
    cannot have its neighbour's heartbeat stand in for it."""
    sent: list[str] = []
    _enrol(store, "node-a", queues=[stream_key(f"{CAP}-v2")], capabilities=[f"{CAP}-v2"])
    wake = WakeCoordinator(
        _fleet(), rclient, group=GROUP, store=store, silent_s=60,
        send=lambda mac, **kw: sent.append(mac),
    )

    assert await wake.has_live_consumer(CAP) is False
    assert await wake.maybe_wake(CAP, reason="test") == ["node-a"]
