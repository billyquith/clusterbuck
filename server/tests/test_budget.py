"""Cloud budget pacing + reserve enforcement (ADR 30)."""

from __future__ import annotations

import pytest

from clusterbuck.budget import check_cloud_budget
from clusterbuck.store import Store

JAN_1_2026 = 1767225600.0  # 2026-01-01T00:00:00Z


@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(str(tmp_path / "budget.db"))


def _spend(store: Store, cost: float, *, day: str = "2026-01-01", job_id: str = "j") -> None:
    store.record_usage(job_id=job_id, ts="t", capability="cloud-cap", model="m",
                       node="cloud:x", venue="cloud", tokens_in=0, tokens_out=0,
                       outcome="done", cost=cost, day=day)


def test_no_cap_is_unlimited(store):
    assert check_cloud_budget(store, monthly_cap=None, urgency="necessary").allowed


def test_necessary_allowed_within_paced_pool(store):
    # Day 1 of 31, $10 cap, 20% reserve → paced pool $8, allowed-by-now ~= 8/31 = $0.258.
    _spend(store, 0.10)
    d = check_cloud_budget(store, monthly_cap=10.0, urgency="necessary", now=JAN_1_2026)
    assert d.allowed


def test_necessary_blocked_once_paced_pool_is_exhausted(store):
    _spend(store, 5.0)  # already over the ~$0.26 allowed by day 1
    d = check_cloud_budget(store, monthly_cap=10.0, urgency="necessary", now=JAN_1_2026)
    assert not d.allowed
    assert "paced" in d.reason


def test_pacing_grows_with_elapsed_days(store):
    """The same spend that blocks on day 1 is fine on day 20 — pacing is cumulative, not a
    flat daily cap, so week one can't burn the month but later weeks get their share."""
    _spend(store, 5.0)
    day20 = JAN_1_2026 + 19 * 86400
    d = check_cloud_budget(store, monthly_cap=10.0, urgency="necessary", now=day20)
    assert d.allowed  # paced pool by day 20/31 is 8 * 20/31 ≈ 5.16


def test_urgent_may_spend_the_reserve_necessary_cannot(store):
    """Spend past the paced pool but still under the full monthly cap: `necessary` is
    blocked, `urgent` may still draw on the reserve."""
    _spend(store, 9.0)  # over the $8 paced pool, under the $10 full cap
    assert not check_cloud_budget(store, monthly_cap=10.0, urgency="necessary",
                                  now=JAN_1_2026).allowed
    assert check_cloud_budget(store, monthly_cap=10.0, urgency="urgent", now=JAN_1_2026).allowed


def test_urgent_blocked_once_the_full_cap_including_reserve_is_gone(store):
    _spend(store, 10.0)
    d = check_cloud_budget(store, monthly_cap=10.0, urgency="urgent", now=JAN_1_2026)
    assert not d.allowed
    assert "reserve" in d.reason


def test_reserve_fraction_is_configurable(store):
    _spend(store, 6.0)
    # With a 0% reserve, the whole $10 is paced: allowed-by-day-1 = 10/31 ≈ 0.32 — still
    # blocked. With a 90% reserve the paced pool is $1, also blocked by day 1. Check the
    # boundary the fraction actually controls: `necessary`'s ceiling scales with it.
    tiny_reserve = check_cloud_budget(store, monthly_cap=10.0, urgency="necessary",
                                      reserve_fraction=0.0, now=JAN_1_2026 + 20 * 86400)
    huge_reserve = check_cloud_budget(store, monthly_cap=10.0, urgency="necessary",
                                      reserve_fraction=0.9, now=JAN_1_2026 + 20 * 86400)
    assert tiny_reserve.allowed  # paced pool $10 * 21/31 ≈ 6.77 > $6 spent
    assert not huge_reserve.allowed  # paced pool $1 * 21/31 ≈ 0.68 < $6 spent
