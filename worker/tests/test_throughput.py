"""Measured throughput — the `stats.tps` a worker reports on its heartbeat.

Ability (ADR 15) scores an ARTIFACT: how good a model is at a task class. It is deliberately
machine-independent, which means it can never answer "how fast is this here". The same
artifact scores identically on a 12 GB GPU and a CPU-only box and performs nothing alike.

So speed has to be measured on the node, from real jobs. No Redis needed here: this is
arithmetic over what the model server already told us, so it stays out of test_work_loop.py
and its broker fixture.
"""

from __future__ import annotations

from cbk_worker.config import WorkerConfig
from cbk_worker.work_loop import WorkLoop


def _loop() -> WorkLoop:
    # The throughput path touches neither redis nor the model client — passing None keeps
    # the test honest about that rather than wiring up fakes it never calls.
    return WorkLoop(None, None, WorkerConfig())


def test_tps_is_none_before_any_job():
    """A node that has served nothing has no speed, which is not the same as being slow.

    The contract types `stats.tps` as a number with no null, so there is nothing honest to
    send yet — the heartbeat omits the key entirely.
    """
    assert _loop().tps is None


def test_tps_counts_output_tokens_only():
    """Prompt tokens are consumed by a prefill pass whose cost has little to do with
    generation speed. Counting them would describe the workload, not the machine."""
    loop = _loop()
    loop._record_tps({"prompt_tokens": 9000, "completion_tokens": 100}, 2.0)
    assert loop.tps == 50.0


def test_a_cold_start_does_not_define_the_node():
    """The first job after a model loads is dramatically slower than steady state. A mean
    would carry that outlier for the life of the process; the median shrugs it off."""
    loop = _loop()
    loop._record_tps({"completion_tokens": 1}, 10.0)       # cold load: 0.1 tok/s
    for _ in range(4):
        loop._record_tps({"completion_tokens": 100}, 2.0)  # steady state: 50 tok/s
    assert loop.tps == 50.0


def test_the_window_forgets_so_a_slower_model_shows_up():
    """Swap in a bigger model and the reported figure has to follow it down, or the
    coordinator keeps routing on a speed this node no longer delivers."""
    from cbk_worker.work_loop import _TPS_WINDOW

    loop = _loop()
    for _ in range(_TPS_WINDOW):
        loop._record_tps({"completion_tokens": 100}, 1.0)   # 100 tok/s
    assert loop.tps == 100.0
    for _ in range(_TPS_WINDOW):
        loop._record_tps({"completion_tokens": 10}, 1.0)    # 10 tok/s
    assert loop.tps == 10.0


def test_samples_it_cannot_trust_are_dropped():
    """Every one of these would otherwise land a bogus number in the fleet's view of this
    node, and a wrong speed is worse than an absent one — absent is visibly unknown."""
    loop = _loop()
    loop._record_tps(None, 1.0)                        # server reported no usage block
    loop._record_tps({}, 1.0)                          # usage without token counts
    loop._record_tps({"completion_tokens": 0}, 1.0)    # nothing was generated
    loop._record_tps({"completion_tokens": 10}, 0.0)   # clock gave us no interval
    loop._record_tps({"completion_tokens": True}, 1.0)  # bool is an int subclass in Python
    assert loop.tps is None
