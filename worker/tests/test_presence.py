"""Presence-mode model ladder (ADR 10): asymmetric hysteresis."""

from __future__ import annotations

from cbk_worker.presence import PresenceLadder

LADDER = {"active": ["8b-extract"], "away": ["8b-extract", "32b-reason", "70b-reason"]}
ALL = ["8b-extract", "32b-reason", "70b-reason"]


class _Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_starts_active_and_serves_the_small_model():
    ladder = PresenceLadder(LADDER, ALL, hysteresis_s=120)
    assert ladder.effective_mode == "active"
    assert ladder.capabilities() == ["8b-extract"]


def test_climbing_to_away_is_damped_by_hysteresis():
    """Cold-loading a big model is expensive, so a brief absence must not trigger it."""
    clock = _Clock()
    ladder = PresenceLadder(LADDER, ALL, hysteresis_s=120, clock=clock)

    assert ladder.update("away") == "active"        # pending, not yet climbed
    clock.t = 119
    assert ladder.update("away") == "active"
    clock.t = 120
    assert ladder.update("away") == "away"          # stayed away long enough
    assert ladder.capabilities() == LADDER["away"]


def test_descending_and_pausing_are_immediate():
    """The owner's machine is the owner's: eviction is cheap and must not wait."""
    clock = _Clock()
    ladder = PresenceLadder(LADDER, ALL, hysteresis_s=120, clock=clock)
    clock.t = 500
    ladder.update("away")
    clock.t = 1000
    assert ladder.update("away") == "away"

    assert ladder.update("active") == "active"      # no delay
    assert ladder.capabilities() == ["8b-extract"]
    assert ladder.update("paused") == "paused"
    assert ladder.capabilities() == []              # paused ⇒ serve nothing


def test_a_flicker_back_to_active_restarts_the_climb():
    clock = _Clock()
    ladder = PresenceLadder(LADDER, ALL, hysteresis_s=120, clock=clock)
    clock.t = 0
    ladder.update("away")
    clock.t = 100
    ladder.update("active")        # owner came back; the pending climb must be abandoned
    clock.t = 101
    assert ladder.update("away") == "active", "climbed on a stale pending timestamp"
    clock.t = 221
    assert ladder.update("away") == "away"


def test_zero_hysteresis_climbs_at_once():
    """A dedicated box wants no damping at all."""
    ladder = PresenceLadder(LADDER, ALL, hysteresis_s=0, clock=_Clock())
    assert ladder.update("away") == "away"


def test_no_ladder_entry_serves_everything_the_node_can():
    ladder = PresenceLadder({}, ALL, hysteresis_s=0, clock=_Clock())
    assert ladder.capabilities() == ALL
    assert ladder.update("away") == "away"
    assert ladder.capabilities() == ALL
