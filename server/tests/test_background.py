"""The coordinator loop's own behaviour: cadence, and failure isolation between ticks.

The individual scans have their own tests (test_escalation, test_reaper, test_backstop,
test_observe). Nothing covered the loop that drives them, which is where the ordering and
error-handling decisions actually live — so a scan raising could silently cost every later
scan its turn without a single test going red.
"""

from __future__ import annotations

import asyncio
import logging

import pytest
from clusterbuck import background


@pytest.fixture()
def tick_errors():
    """Capture `clusterbuck.coordinator` ERROR records straight off that logger.

    Not `caplog`: importing the sync plane pulls in LiteLLM, which reconfigures root
    logging, so caplog's root handler sees nothing once the full suite has run. Attaching
    here is independent of whatever else has happened to the root logger.
    """
    records: list[str] = []

    class _Sink(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    log = logging.getLogger("clusterbuck.coordinator")
    sink = _Sink(level=logging.ERROR)
    log.addHandler(sink)
    previous, log.level = log.level, logging.ERROR
    try:
        yield records
    finally:
        log.removeHandler(sink)
        log.level = previous


@pytest.fixture()
def calls(monkeypatch):
    """Replace every scan with a recorder. The loop never touches a real Store/Queue."""
    seen: dict[str, int] = {}

    def record(name, *, sync=False, raises=None):
        seen[name] = 0  # seeded, so "never ran" reads as 0 rather than a KeyError
        def bump(*_a, **_k):
            seen[name] += 1
            if raises is not None:
                raise raises
        async def abump(*_a, **_k):
            bump()
        monkeypatch.setattr(background, name, bump if sync else abump)

    record.seen = seen
    return record


def _run(stop_after: int, **kw):
    """Drive the loop for `stop_after` ticks with no sleeping."""
    stop = asyncio.Event()
    ticks = {"n": 0}

    async def go():
        real = background.observe_tick

        async def counting_observe(*a, **k):
            ticks["n"] += 1
            await real(*a, **k)
            if ticks["n"] >= stop_after:
                stop.set()

        background.observe_tick = counting_observe
        try:
            await background.coordinator_loop(
                None, None, None, None, interval_s=0, stop=stop, **kw)
        finally:
            background.observe_tick = real

    asyncio.run(go())


def test_a_failing_tick_does_not_cost_its_siblings_their_turn(calls, tick_errors):
    """The property this loop's structure exists for.

    Every scan used to share one `try`, so an exception in the first skipped all the rest
    for that tick — and left the tick counter un-incremented, stalling the slow-cadence
    work behind it too.
    """
    calls("escalation_scan", raises=RuntimeError("redis hiccup"))
    calls("reservation_tick")
    calls("usage_scan")
    calls("observe_tick")
    calls("eval_tick")
    calls("reaper_scan")
    calls("backstop_scan")
    calls("wake_reconcile_scan")
    calls("scan_all", sync=True)
    seen = calls.seen

    _run(stop_after=3, planner_every=100, eval_every=100, reaper_every=100)

    assert seen["escalation_scan"] == 3, "the failing tick must still be attempted"
    # The ones that used to be skipped:
    assert seen["reservation_tick"] == 3
    assert seen["usage_scan"] == 3
    assert seen["observe_tick"] == 3
    assert any("'escalation' failed" in m for m in tick_errors), tick_errors


def test_a_failing_tick_does_not_stall_the_slow_cadences(calls):
    """`ticks` increments before any scan runs, so a failure cannot freeze the modulo."""
    calls("escalation_scan", raises=RuntimeError("down"))
    for name in ("reservation_tick", "usage_scan", "observe_tick",
                 "eval_tick", "reaper_scan", "backstop_scan", "wake_reconcile_scan"):
        calls(name)
    calls("scan_all", sync=True)
    seen = calls.seen

    _run(stop_after=4, planner_every=100, eval_every=2, reaper_every=4)

    assert seen["eval_tick"] == 2, "eval must still fire on ticks 2 and 4"
    assert seen["reaper_scan"] == 1, "the reaper must still fire on tick 4"
    assert seen["backstop_scan"] == 1


def test_the_reaper_and_the_backstops_share_a_cadence_but_not_a_failure(calls, tick_errors):
    """They run together and are deliberately separate: XAUTOCLAIM cannot see what the
    SQLite sweeps look for, so one failing must not suppress the other."""
    for name in ("escalation_scan", "reservation_tick", "usage_scan",
                 "observe_tick", "eval_tick", "wake_reconcile_scan"):
        calls(name)
    calls("reaper_scan", raises=RuntimeError("PEL unavailable"))
    calls("backstop_scan")
    calls("scan_all", sync=True)
    seen = calls.seen

    _run(stop_after=2, planner_every=100, eval_every=100, reaper_every=1)

    assert seen["reaper_scan"] == 2
    assert seen["backstop_scan"] == 2, "the backstops must run even when the reaper fails"
    assert any("'reaper' failed" in m for m in tick_errors), tick_errors


def test_the_synchronous_planner_is_isolated_too(calls, tick_errors):
    """`scan_all` is the one non-async tick; it must get the same treatment."""
    for name in ("escalation_scan", "reservation_tick", "usage_scan", "observe_tick",
                 "eval_tick", "reaper_scan", "backstop_scan", "wake_reconcile_scan"):
        calls(name)
    calls("scan_all", sync=True, raises=RuntimeError("catalog broken"))
    seen = calls.seen

    _run(stop_after=2, planner_every=1, eval_every=100, reaper_every=100)

    assert seen["scan_all"] == 2
    assert seen["observe_tick"] == 2, "observe runs after the planner and must survive it"
    assert any("'planner' failed" in m for m in tick_errors), tick_errors


def test_the_wake_reconciler_runs_on_its_own_cadence(calls):
    """It has to actually be wired in, on a cadence of its own.

    The retry it provides is the only thing standing between a lost magic packet and a
    job that waits forever, and a scan that exists but is never called would leave that
    gap open while every unit test of the scan itself stayed green.

    Its cadence is tied to `wake_cooldown_s`, not chosen: `maybe_wake` coalesces to at
    most one wake per capability per cooldown window, so scanning faster can only burn
    queries.
    """
    for name in ("escalation_scan", "reservation_tick", "usage_scan", "observe_tick",
                 "eval_tick", "reaper_scan", "backstop_scan", "wake_reconcile_scan"):
        calls(name)
    calls("scan_all", sync=True)
    seen = calls.seen

    _run(stop_after=6, planner_every=100, eval_every=100, reaper_every=100, wake_every=2)

    assert seen["wake_reconcile_scan"] == 3, "ticks 2, 4 and 6"


def test_a_failing_wake_reconcile_does_not_cost_its_siblings_their_turn(calls, tick_errors):
    """It talks to both SQLite and Redis, so it has as many ways to fail as the reaper."""
    for name in ("escalation_scan", "reservation_tick", "usage_scan", "observe_tick",
                 "eval_tick", "reaper_scan", "backstop_scan"):
        calls(name)
    calls("wake_reconcile_scan", raises=RuntimeError("redis gone"))
    calls("scan_all", sync=True)
    seen = calls.seen

    _run(stop_after=2, planner_every=100, eval_every=100, reaper_every=1, wake_every=1)

    assert seen["wake_reconcile_scan"] == 2
    assert seen["reaper_scan"] == 2, "the reaper must still run when the reconciler fails"
    assert seen["observe_tick"] == 2
    assert any("'wake-reconcile' failed" in m for m in tick_errors), tick_errors


def test_the_model_probe_runs_at_once_then_on_its_cadence_and_is_isolated(calls, tick_errors):
    """First tick too — `/fleet` saying `unknown` for a whole interval after every restart is
    the gap the probe exists to close — and a failing probe costs the tick nothing else."""
    for name in ("escalation_scan", "reservation_tick", "usage_scan", "observe_tick",
                 "eval_tick", "reaper_scan", "backstop_scan", "wake_reconcile_scan"):
        calls(name)
    calls("scan_all", sync=True)
    probes = {"n": 0}

    class _Health:
        async def probe_all(self):
            probes["n"] += 1
            raise RuntimeError("probe blew up")

    _run(stop_after=6, planner_every=100, eval_every=100, reaper_every=100,
         model_health=_Health(), probe_every=3)

    assert probes["n"] == 3, "ticks 1, 3 and 6"
    assert calls.seen["observe_tick"] == 6
    assert any("'model-probe' failed" in m for m in tick_errors), tick_errors
