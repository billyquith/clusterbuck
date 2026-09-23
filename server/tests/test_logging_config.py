"""The coordinator's log configuration: ours at INFO, everyone else's at WARNING.

Two regressions this locks down, both of which were live and both of which are invisible
when broken — the process just stops saying things.

1. `clusterbuck.*` had no handler at all. Uvicorn configures only its own loggers, so our
   records fell through to Python's `lastResort`, which drops everything below WARNING.
2. Alembic's `fileConfig` ran with `disable_existing_loggers=True` on every `Store()`,
   setting `disabled = True` on all sixteen `clusterbuck.*` loggers (see
   test_migrations.py).
"""

from __future__ import annotations

import logging

import pytest
from clusterbuck.__main__ import (
    _HANDLER_MARK,
    DEFAULT_LOG_LEVEL,
    configure_logging,
)


@pytest.fixture(autouse=True)
def _restore_logging():
    """Put the logging tree back: these tests mutate global state by design."""
    root = logging.getLogger()
    before = (root.level, list(root.handlers), logging.getLogger("clusterbuck").level)
    yield
    root.level, root.handlers[:], logging.getLogger("clusterbuck").level = (
        before[0], before[1], before[2])


def _emit(name: str, level: int, msg: str) -> list[str]:
    """Log one record and return what a root handler would have emitted."""
    seen: list[str] = []

    class _Sink(logging.Handler):
        def emit(self, record):
            seen.append(self.format(record))

    sink = _Sink()
    logging.getLogger().addHandler(sink)
    try:
        logging.getLogger(name).log(level, msg)
    finally:
        logging.getLogger().removeHandler(sink)
    return seen


def test_our_info_is_emitted():
    """The seventeen INFO call sites — wake, reaper, escalation, the sweeps — must show."""
    configure_logging()
    assert _emit("clusterbuck.wake", logging.INFO, "woke a node") == ["woke a node"]


def test_third_party_info_is_not():
    """httpx, redis and sqlalchemy narrate every request at INFO. They stay quiet, or
    they bury the output this exists to surface."""
    configure_logging()
    for noisy in ("httpx", "redis", "sqlalchemy.engine", "LiteLLM", "openai"):
        assert _emit(noisy, logging.INFO, "chatter") == [], f"{noisy} leaked at INFO"


def test_third_party_warnings_still_get_through():
    """Quietening them must not mean silencing them — a redis warning still matters."""
    configure_logging()
    assert _emit("redis", logging.WARNING, "connection lost") == ["connection lost"]


def test_we_attach_our_own_root_handler():
    """The original defect: nothing attached a handler for `clusterbuck.*`, so Python's
    `lastResort` applied and dropped everything below WARNING whatever level we set.

    Asserts on OUR handler specifically. `assert root.handlers` would pass on pytest's
    own capture handler and catch nothing — it did, until this test was tightened.
    """
    configure_logging()
    ours = [h for h in logging.getLogger().handlers
            if getattr(h, _HANDLER_MARK, False)]
    assert ours, "configure_logging attached no handler of its own"


def test_repeat_calls_do_not_stack_handlers():
    """Idempotent: a second call replaces our handler rather than doubling every line."""
    configure_logging()
    configure_logging()
    configure_logging()
    ours = [h for h in logging.getLogger().handlers
            if getattr(h, _HANDLER_MARK, False)]
    assert len(ours) == 1, f"{len(ours)} handlers attached; log lines would repeat"


def test_the_level_is_overridable(monkeypatch):
    monkeypatch.setenv("CBK_LOG_LEVEL", "DEBUG")
    configure_logging()
    assert logging.getLogger("clusterbuck").level == logging.DEBUG
    assert _emit("clusterbuck.reaper", logging.DEBUG, "detail") == ["detail"]


def test_a_typo_falls_back_rather_than_muting_or_crashing(monkeypatch):
    """A bad value in a unit file must not stop the coordinator, nor silence it."""
    monkeypatch.setenv("CBK_LOG_LEVEL", "VERBOSE")
    configure_logging()  # must not raise
    assert logging.getLogger("clusterbuck").level == getattr(logging, DEFAULT_LOG_LEVEL)
    assert _emit("clusterbuck.wake", logging.INFO, "still heard") == ["still heard"]
