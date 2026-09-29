"""Need-shaped routing weighs speed and idle machines, not price alone.

Cheapest-first on cloud-equivalent prices meant smallest-model-first: every need-shaped job
went to the slowest tier that cleared the bar while a far faster machine sat idle. These
tests pin the replacement order — local, then soonest answer, then price — and the cases
where the estimate must NOT be believed: a tier nobody is serving right now.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from clusterbuck.evaluation import SCALE_VERSION, TASK_CLASSES
from clusterbuck.fleet import CapabilitySpec, Fleet
from clusterbuck.orm.node import Node
from clusterbuck.routing import resolve, tier_eta
from clusterbuck.store import Store


def _fleet(*, big_is_cheaper: bool = False, **extra) -> Fleet:
    """`small` is cheaper by default, so the old order always chose it. Tests expecting
    `small` flip that, so price alone would pick `big` and only the estimate explains the
    answer — otherwise they would pass with speed ignored entirely."""
    cheap, dear = (0.0002, 0.0006), (0.0009, 0.0027)
    s_price, b_price = (dear, cheap) if big_is_cheaper else (cheap, dear)
    caps = {
        "small": CapabilitySpec(model_server="x", model="small:9b",
                                price_in_per_1k=s_price[0], price_out_per_1k=s_price[1]),
        "big": CapabilitySpec(model_server="x", model="big:30b",
                              price_in_per_1k=b_price[0], price_out_per_1k=b_price[1]),
    }
    caps.update(extra)
    return Fleet(capabilities=caps)


@pytest.fixture()
def store(tmp_path) -> Store:
    s = Store(str(tmp_path / "speed.db"))
    for artifact in ("small:9b", "big:30b", "cloud/x"):
        for tc in TASK_CLASSES:
            s.set_ability(artifact=artifact, task_class=tc, score=7.0,
                          scale_version=SCALE_VERSION, updated_at="t",
                          n_items=10, n_passed=10)
    return s


def _node(store, node_id, *, serves, tps, warm=(), age_s=5.0, load_s=None):
    with store._session() as s:
        s.add(Node(node_id=node_id, node_key="k", hostname=node_id, profile="shared",
                   capabilities=json.dumps([serves]), enrolled_at="2026-01-01T00:00:00Z",
                   auto_approve=0))
        s.commit()
    # Now, not at import: the suite runs for over a minute on a CI runner, and a module-level
    # clock made every "5 s old" heartbeat older than the silence threshold by the time
    # these tests ran, so every node read as silent there and nowhere else.
    stamp = (datetime.now(UTC) - timedelta(seconds=age_s)).isoformat().replace("+00:00", "Z")
    queues = [f"q:{serves}"] if serves else []
    store.record_heartbeat(node_id=node_id, mode="active", installed="[]",
                           loaded=json.dumps(list(warm)), queues=json.dumps(queues),
                           jobs_done=0, tps=tps, last_heartbeat=stamp, load_s=load_s)


def _open(store, capability, n):
    for i in range(n):
        store.insert(id=f"{capability}-{i}", result_key=f"r:{capability}-{i}",
                     capability=capability, created_at="2026-01-01T00:00:00Z")


def _route(fleet, store, **kw):
    return resolve(fleet, store, capability=None, task_class="extract", min_ability=5,
                   **kw)


def test_an_idle_fast_machine_beats_a_cheaper_slow_one(store):
    _node(store, "slow", serves="small", tps=10, warm=["small:9b"])
    _node(store, "fast", serves="big", tps=54, warm=["big:30b"])
    sel = _route(_fleet(), store)
    assert sel.capability == "big"
    assert sel.eta_s is not None


def test_a_backlog_sends_work_to_the_idle_slower_tier(store):
    """Idle beats fast once the fast tier has enough queued ahead of the new job."""
    _node(store, "slow", serves="small", tps=10, warm=["small:9b"])
    _node(store, "fast", serves="big", tps=54, warm=["big:30b"])
    _open(store, "big", 10)
    assert _route(_fleet(big_is_cheaper=True), store).capability == "small"


def test_a_tier_nobody_is_serving_is_never_preferred_over_a_live_one(store):
    """The fast machine's last heartbeat is old: it is asleep, and its speed is moot."""
    _node(store, "slow", serves="small", tps=10, warm=["small:9b"])
    _node(store, "fast", serves="big", tps=54, warm=["big:30b"], age_s=3600)
    assert _route(_fleet(big_is_cheaper=True), store).capability == "small"


def test_a_node_not_reading_the_queue_does_not_count(store):
    """Paused, or on a presence-ladder rung that excludes the tier: it heartbeats, but it
    is not draining the queue, so the tier has no live capacity."""
    _node(store, "slow", serves="small", tps=10, warm=["small:9b"])
    # Awake and warm — so only "is it reading the queue" can rule it out.
    _node(store, "fast", serves="", tps=54, warm=["big:30b"])
    with store._session() as s:
        n = s.get(Node, "fast")
        n.capabilities = json.dumps(["big"])
        s.add(n)
        s.commit()
    assert _route(_fleet(big_is_cheaper=True), store).capability == "small"


def test_with_no_liveness_at_all_price_decides_as_before(store):
    """No heartbeats anywhere (a fresh coordinator, or a test fleet): every estimate is
    unknown and the old cheapest-first order is exactly what comes back."""
    sel = _route(_fleet(), store)
    assert sel.capability == "small"
    assert sel.eta_s is None


def test_a_cold_load_counts_against_a_tier(store):
    """Faster decode does not win if the model must first be loaded for a long time."""
    _node(store, "slow", serves="small", tps=10, warm=["small:9b"])
    _node(store, "fast", serves="big", tps=54, load_s=600)  # cold, slow to load
    assert _route(_fleet(big_is_cheaper=True), store).capability == "small"


def test_capacity_adds_across_machines(store):
    _node(store, "a", serves="big", tps=20, warm=["big:30b"])
    _node(store, "b", serves="big", tps=20, warm=["big:30b"])
    eta = tier_eta(_fleet(), store)["big"]
    assert eta.live_nodes == 2
    assert eta.seconds == pytest.approx(700 / 40)


def test_local_still_comes_before_cloud_however_fast(store):
    """Speed reorders local tiers only; reaching for cloud stays a privacy and budget
    decision, never a latency one."""
    fleet = _fleet(cloudy=CapabilitySpec(model="cloud/x", cloud=True,
                                         price_in_per_1k=0.0001, price_out_per_1k=0.0001))
    _node(store, "slow", serves="small", tps=1, warm=["small:9b"])
    sel = _route(fleet, store, privacy="cloud_ok", urgency="urgent")
    assert sel.capability == "small"


def test_explicit_tier_addressing_is_untouched(store):
    _node(store, "fast", serves="big", tps=54, warm=["big:30b"])
    sel = resolve(_fleet(), store, capability="small", task_class=None, min_ability=None)
    assert sel.capability == "small"
