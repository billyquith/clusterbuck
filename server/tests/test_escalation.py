"""Escalation engine: waitable → necessary on age, gated on the Redis result blob."""

from __future__ import annotations

import json
import time

import pytest
from clusterbuck.escalation import escalation_scan
from clusterbuck.fleet import Fleet, NodeSpec
from clusterbuck.queue import Queue
from clusterbuck.store import Store
from clusterbuck.wake import WakeCoordinator

CAP = "8b-extract"
GROUP = "cbk-workers"
MAC = "aa:bb:cc:dd:ee:ff"


@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(str(tmp_path / "esc.db"))


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


def _wake(queue: Queue, sent: list[str]) -> WakeCoordinator:
    fleet = Fleet(nodes=[NodeSpec(id="node-a", mac=MAC, capabilities=[CAP])])
    return WakeCoordinator(
        fleet, queue.client, group=GROUP, send=lambda mac, **kw: sent.append(mac)
    )


async def test_promotes_due_waitable_and_wakes(store, queue):
    sent: list[str] = []
    store.insert(
        id="job_a", result_key="res_a", capability=CAP, created_at="t",
        urgency="waitable", escalate_at=time.time() - 1,
    )
    promoted = await escalation_scan(store, queue, _wake(queue, sent))

    assert promoted == ["job_a"]
    row = store.get("job_a")
    assert row.urgency == "necessary"
    assert row.escalated == 1
    assert sent == [MAC]  # promotion granted wake rights


async def test_skips_already_served(store, queue):
    sent: list[str] = []
    store.insert(
        id="job_b", result_key="res_b", capability=CAP, created_at="t",
        urgency="waitable", escalate_at=time.time() - 1,
    )
    # A completed job whose SQLite status is still 'queued' (not yet polled) must NOT
    # be promoted — doneness is judged on the result blob.
    await queue.client.set("res_b", json.dumps({"job_id": "job_b", "status": "done"}))

    assert await escalation_scan(store, queue, _wake(queue, sent)) == []
    assert store.get("job_b").urgency == "waitable"
    assert sent == []


async def test_ignores_future_deadline(store, queue):
    sent: list[str] = []
    store.insert(
        id="job_c", result_key="res_c", capability=CAP, created_at="t",
        urgency="waitable", escalate_at=time.time() + 3600,
    )
    assert await escalation_scan(store, queue, _wake(queue, sent)) == []


async def test_waitable_without_deadline_never_escalates(store, queue):
    sent: list[str] = []
    store.insert(
        id="job_d", result_key="res_d", capability=CAP, created_at="t",
        urgency="waitable", escalate_at=None,
    )
    assert await escalation_scan(store, queue, _wake(queue, sent)) == []
