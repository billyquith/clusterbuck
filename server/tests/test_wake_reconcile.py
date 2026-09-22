"""The wake reconciler: a wake fired once is not a wake delivered.

Every other caller of `maybe_wake` is edge-triggered — one burst at submit, one on
escalation, one on a reservation's warming edge — and a magic packet is unacknowledged
UDP. Before this scan existed, a single lost packet left an `urgent` job with no further
attempt and, with `CBK_MAX_QUEUE_AGE_S` unset (the default), **no terminal state at all**:
not orphaned (its entry exists), not reaped (it never entered the pending list), not
escalable (escalation selects `waitable` rows and promotes each once). It read `queued`
forever.

These tests are about the retry and about what must NOT earn one.
"""

from __future__ import annotations

import json
import logging
import time

import pytest

from clusterbuck.fleet import Fleet, NodeSpec
from clusterbuck.queue import Queue
from clusterbuck.store import Store
from clusterbuck.wake import WakeCoordinator, wake_reconcile_scan

CAP = "8b-extract"
OTHER = "32b-reason"
GROUP = "cbk-workers"
MAC = "aa:bb:cc:dd:ee:ff"


@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(str(tmp_path / "reconcile.db"))


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


def _wake(queue: Queue, sent: list[str], *, clock=None) -> WakeCoordinator:
    fleet = Fleet(nodes=[
        NodeSpec(id="node-a", mac=MAC, capabilities=[CAP, OTHER]),
    ])
    return WakeCoordinator(
        fleet, queue.client, group=GROUP, send=lambda mac, **kw: sent.append(mac),
        **({"clock": clock} if clock else {}),
    )


def _job(store: Store, job_id: str, **kw) -> None:
    defaults = {
        "id": job_id, "result_key": f"res_{job_id}", "capability": CAP,
        "created_at": "t", "urgency": "necessary",
    }
    store.insert(**{**defaults, **kw})


# --- the retry itself -----------------------------------------------------------------


async def test_an_unserved_job_with_wake_rights_gets_another_wake(store, queue):
    """The A1 regression. Nothing here submits anything — the job is simply sitting
    queued, exactly as it would be after its submit-time packet went nowhere."""
    sent: list[str] = []
    _job(store, "job_a")

    assert await wake_reconcile_scan(store, queue, _wake(queue, sent)) == [CAP]
    assert sent == [MAC]


async def test_an_urgent_job_qualifies_too(store, queue):
    """Both classes that may demand capacity (ADR 18), not just the escalated ones."""
    sent: list[str] = []
    _job(store, "job_a", urgency="urgent")

    assert await wake_reconcile_scan(store, queue, _wake(queue, sent)) == [CAP]


async def test_the_retry_repeats_across_scans(store, queue):
    """One retry would be no better than one attempt: a machine that failed to wake at
    12:00 is no more likely to have woken by 12:01. Bounded only by the cooldown."""
    sent: list[str] = []
    t = {"now": 0.0}
    _job(store, "job_a")
    wake = _wake(queue, sent, clock=lambda: t["now"])

    assert await wake_reconcile_scan(store, queue, wake) == [CAP]
    t["now"] = 30.0
    assert await wake_reconcile_scan(store, queue, wake) == []   # inside the cooldown
    t["now"] = 90.0
    assert await wake_reconcile_scan(store, queue, wake) == [CAP]
    assert sent == [MAC, MAC]


async def test_one_wake_decision_per_capability_however_deep_the_backlog(store, queue):
    """Coalescing is the whole point (design.md → "Escalation coalesces, never
    stampedes"): a backlog of fifty jobs is one warm window, not fifty wakes."""
    sent: list[str] = []
    for i in range(50):
        _job(store, f"job_{i}")

    assert await wake_reconcile_scan(store, queue, _wake(queue, sent)) == [CAP]
    assert sent == [MAC]


async def test_each_capability_is_decided_on_its_own(store, queue):
    sent: list[str] = []
    _job(store, "job_a", capability=CAP)
    _job(store, "job_b", capability=OTHER)

    assert sorted(await wake_reconcile_scan(store, queue, _wake(queue, sent))) == \
        sorted([CAP, OTHER])
    assert sent == [MAC, MAC]


# --- what must not earn a wake ---------------------------------------------------------


async def test_a_waitable_job_never_creates_capacity(store, queue):
    """ADR 18, and the one rule this scan could most easily have broken: a patient job
    runs on warmth that already exists and never makes any."""
    sent: list[str] = []
    _job(store, "job_a", urgency="waitable", escalate_at=time.time() + 3600)

    assert await wake_reconcile_scan(store, queue, _wake(queue, sent)) == []
    assert sent == []


async def test_a_finished_but_unpolled_job_does_not_wake_a_machine(store, queue):
    """Doneness is judged on the Redis result blob, never SQLite's `status`, which is
    refreshed lazily. The same discipline `escalation_scan` follows, for the same reason."""
    sent: list[str] = []
    _job(store, "job_a")
    await queue.client.set("res_job_a", json.dumps({"job_id": "job_a", "status": "done"}))

    assert await wake_reconcile_scan(store, queue, _wake(queue, sent)) == []
    assert sent == []


async def test_a_job_being_worked_on_does_not_wake_a_machine(store, queue):
    """`mark_started` advances an observed claim to `running`, so the status column
    already means "nobody is known to have this" — no second mechanism needed."""
    sent: list[str] = []
    _job(store, "job_a")
    store.mark_started("job_a", at="t", claimed_by="node-a")

    assert store.get("job_a").status == "running"
    assert await wake_reconcile_scan(store, queue, _wake(queue, sent)) == []


async def test_a_cancelled_job_does_not_wake_a_machine(store, queue):
    """The worst outcome available in a cold-by-default fleet: booting a machine for work
    the client has already walked away from. `due_for_escalation` guards the same way."""
    sent: list[str] = []
    _job(store, "job_a")
    store.request_cancel("job_a")

    assert await wake_reconcile_scan(store, queue, _wake(queue, sent)) == []
    assert sent == []


async def test_a_job_past_its_deadline_does_not_wake_a_machine(store, queue):
    """Nobody can answer it in time, so the wake buys nothing. The usage scan expires
    these within a tick, but two scans in the same tick have no ordering to rely on."""
    sent: list[str] = []
    _job(store, "job_a", deadline_epoch=time.time() - 1)

    assert await wake_reconcile_scan(store, queue, _wake(queue, sent)) == []
    assert sent == []


async def test_a_terminal_job_does_not_wake_a_machine(store, queue):
    sent: list[str] = []
    _job(store, "job_a")
    store.set_status("job_a", "failed")

    assert await wake_reconcile_scan(store, queue, _wake(queue, sent)) == []


async def test_a_live_worker_means_no_wake_is_owed(store, queue):
    """The scan does not decide liveness itself — it hands every candidate to
    `maybe_wake`, which owns that rule so it cannot be stated in two places."""
    sent: list[str] = []
    _job(store, "job_a")
    await queue.ensure_group(CAP)
    await queue.client.xreadgroup(
        GROUP, "node-live", {f"q:{CAP}": ">"}, count=1, block=10)

    assert await wake_reconcile_scan(store, queue, _wake(queue, sent)) == []
    assert sent == []


async def test_a_capability_nothing_can_wake_is_reported_once(store, queue, caplog):
    """The state an operator most needs to see, and the one that was silent.

    A node reachable only over a tunnel has no MAC to broadcast to, so the job is stuck
    for a reason the coordinator cannot fix — and the reconciler, retrying forever, logged
    nothing on any of those attempts because the old code only spoke when it succeeded.
    Warned on the transition rather than per scan, or a permanent condition becomes
    per-minute noise.
    """
    sent: list[str] = []
    _job(store, "job_a")
    # A node that serves the capability but carries no MAC: enrolled over a tunnel.
    fleet = Fleet(nodes=[NodeSpec(id="node-tunnel", mac=None, capabilities=[CAP])])
    wake = WakeCoordinator(
        fleet, queue.client, group=GROUP, send=lambda mac, **kw: sent.append(mac)
    )

    with caplog.at_level(logging.WARNING, logger="clusterbuck.wake"):
        assert await wake_reconcile_scan(store, queue, wake) == []
        assert await wake_reconcile_scan(store, queue, wake) == []

    assert sent == []
    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warnings) == 1, "warned on the transition, not on every scan"
    assert CAP in warnings[0].getMessage()
