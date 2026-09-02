"""Wake coordinator: the decision to wake, against real Redis liveness. No packets sent."""

from __future__ import annotations

import pytest
import redis.asyncio as aioredis

from clusterbuck.fleet import Fleet, NodeSpec
from clusterbuck.queue import (
    CLOUD_EXECUTOR_CONSUMER,
    REAPER_CONSUMER,
    stream_key,
)
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
