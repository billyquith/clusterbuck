"""Placement advice: which models each node should hold, and what the fleet is missing.

The advisor reads five store methods and nothing else, so most tests drive it from a
stand-in holding plain data — the cases worth pinning are about ranking, not SQL. The
demand query itself is tested against a real store, because excluding the harness's own
traffic is the part a stand-in would only pretend to check.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from clusterbuck.evaluation import SCALE_VERSION, TASK_CLASSES, TIER1_MAX_ABILITY
from clusterbuck.fleet import Fleet
from clusterbuck.orm.node import Node
from clusterbuck.placement import (
    DEMAND_PRIOR_JOBS,
    MIN_GAIN,
    advise,
    demand_weights,
    gaps,
)
from clusterbuck.store import Store

NOW = datetime(2026, 9, 1, tzinfo=UTC)


@dataclass
class _Ability:
    artifact: str
    task_class: str
    score: float
    provenance: str = "measured"


@dataclass
class _Cat:
    artifact: str
    size_gb: float
    min_ram_gb: float
    expected_ability: float | None = None
    params_b: float | None = None
    active_params_b: float | None = None
    context_tokens: int | None = None
    supports_tools: bool | None = None
    supports_vision: bool | None = None
    supports_json_schema: bool | None = None


class _Store:
    def __init__(self, *, nodes, catalog=(), ability=(), demand=None, throughput=None):
        self._nodes, self._catalog, self._ability = list(nodes), list(catalog), list(ability)
        self._demand = demand or {"total": 0, "by_class": {}, "by_capability": {},
                                  "by_model": {}, "max_tokens_in": 0}
        self._tp = throughput or {}

    def list_nodes(self):
        return self._nodes

    def list_catalog(self):
        return self._catalog

    def ability_matrix(self, _scale):
        return self._ability

    def real_demand(self, _since):
        return self._demand

    def model_throughput(self):
        return self._tp


def _node(node_id="node-a", *, installed=(), profile="dedicated", ram=64.0, vram=None,
          caps=("tier-a",), heartbeat="2026-09-01T00:00:00Z") -> Node:
    return Node(node_id=node_id, node_key="k", hostname=node_id, profile=profile,
                ram_gb=ram, vram_gb=vram, installed=json.dumps(list(installed)),
                capabilities=json.dumps(list(caps)), enrolled_at="2026-08-01T00:00:00Z",
                last_heartbeat=heartbeat, auto_approve=0)


def _flat(artifact, score, provenance="measured"):
    return [_Ability(artifact, tc, score, provenance) for tc in TASK_CLASSES]


def _node_advice(store, node_id="node-a"):
    return next(n for n in advise(store, now=NOW, scale_version=SCALE_VERSION)["nodes"]
                if n.node_id == node_id)


# --- demand ------------------------------------------------------------------------------

def test_a_thin_month_cannot_rule_a_class_out():
    """Early traffic is skewed; the prior keeps an unasked class in the mix."""
    w = demand_weights({"extract": 40})
    assert w["code"] > 0.1
    assert abs(sum(w.values()) - 1) < 1e-9


def test_real_volume_outweighs_the_prior():
    w = demand_weights({"extract": DEMAND_PRIOR_JOBS * 100})
    assert w["extract"] > 0.95


# --- ranking -----------------------------------------------------------------------------

def test_keep_what_you_have_when_nothing_beats_it_by_a_real_margin():
    """A candidate inside MIN_GAIN is noise; a download for it would be worse than none."""
    store = _Store(
        nodes=[_node(installed=["have:9b"])],
        catalog=[_Cat("have:9b", 6, 12), _Cat("new:14b", 9, 24, expected_ability=7.0)],
        ability=_flat("have:9b", 7.0 - MIN_GAIN / 2),
    )
    advice = _node_advice(store)
    assert advice.slate[0].candidate.artifact == "have:9b"
    assert advice.slate[0].action == "keep"
    assert "keep have:9b" in advice.summary


def test_an_equally_capable_model_that_fits_the_accelerator_displaces_one_that_spills():
    """Scores stop at the ceiling, so speed is what an equal-ability upgrade buys."""
    store = _Store(
        nodes=[_node(installed=["old:dense"], vram=24.0)],
        catalog=[_Cat("old:dense", 30, 40), _Cat("new:small", 10, 16)],
        ability=_flat("old:dense", 7.0) + _flat("new:small", 7.0),
    )
    advice = _node_advice(store)
    assert advice.slate[0].candidate.artifact == "new:small"
    assert "runs on the accelerator" in advice.slate[0].why


def test_an_equally_capable_model_with_far_fewer_active_parameters_displaces():
    store = _Store(
        nodes=[_node(installed=["old:32b"])],
        catalog=[_Cat("old:32b", 20, 40, params_b=32),
                 _Cat("new:moe", 18, 40, expected_ability=7.0, params_b=30,
                      active_params_b=3)],
        ability=_flat("old:32b", 7.0),
    )
    advice = _node_advice(store)
    assert advice.slate[0].candidate.artifact == "new:moe"
    assert "fewer active parameters" in advice.slate[0].why


def test_an_equally_capable_model_with_no_speed_edge_does_not_displace():
    """Same ability, same fit, similar size: the download buys nothing."""
    store = _Store(
        nodes=[_node(installed=["have:a"])],
        catalog=[_Cat("have:a", 10, 16, params_b=14), _Cat("other:b", 9, 16, params_b=12)],
        ability=_flat("have:a", 6.8) + _flat("other:b", 7.0),
    )
    assert _node_advice(store).slate[0].candidate.artifact == "have:a"


def test_a_clear_upgrade_is_recommended():
    store = _Store(
        nodes=[_node(installed=["old:7b"])],
        catalog=[_Cat("old:7b", 4, 8), _Cat("new:14b", 9, 24, expected_ability=7.0)],
        ability=_flat("old:7b", 5.0),
    )
    advice = _node_advice(store)
    assert advice.slate[0].candidate.artifact == "new:14b"
    assert advice.slate[0].action == "install"


def test_a_hint_is_clamped_to_the_instrument_ceiling():
    """The checker can never record above TIER1_MAX_ABILITY, so an unclamped 8 would win
    by a margin that cannot be verified — and star a 40 GB download for it."""
    store = _Store(
        nodes=[_node(installed=["have:30b"])],
        catalog=[_Cat("have:30b", 18, 40),
                 _Cat("big:70b", 40, 64, expected_ability=TIER1_MAX_ABILITY + 1)],
        ability=_flat("have:30b", TIER1_MAX_ABILITY - 0.2),
    )
    advice = _node_advice(store)
    big = next(c for c in advice.ranked if c.artifact == "big:70b")
    assert big.value == TIER1_MAX_ABILITY
    assert advice.slate[0].candidate.artifact == "have:30b"


def test_ability_measured_on_another_node_counts_here():
    """Ability belongs to the artifact. An empty node should be told to install the model
    measured elsewhere, not a catalog estimate of the same value."""
    store = _Store(
        nodes=[_node("node-a", installed=["moe:30b"]), _node("node-b", installed=[])],
        # The estimate is higher AND the smaller download, so only "evidence over a
        # promise" can put the measured model first.
        catalog=[_Cat("moe:30b", 18, 40),
                 _Cat("dense:32b", 16, 48, expected_ability=7.0)],
        ability=_flat("moe:30b", 6.8),
        throughput={("node-a", "moe:30b"): 50.0},
    )
    advice = _node_advice(store, "node-b")
    assert advice.slate[0].candidate.artifact == "moe:30b"
    assert "measured on node-a" in advice.slate[0].why


def test_a_missing_score_is_unknown_not_zero():
    """'vs best installed 0' was the old planner reading 'unmeasured' as 'useless'."""
    store = _Store(nodes=[_node(installed=["mystery:7b"])], catalog=[])
    advice = _node_advice(store)
    assert advice.ranked == [] and advice.slate == []


def test_a_candidate_that_does_not_fit_is_never_recommended():
    store = _Store(
        nodes=[_node(installed=[], ram=16.0)],
        catalog=[_Cat("huge:70b", 40, 64, expected_ability=7.0),
                 _Cat("small:8b", 5, 12, expected_ability=5.0)],
    )
    advice = _node_advice(store)
    assert [c.artifact for c in advice.ranked] == ["small:8b"]


# --- the slate ---------------------------------------------------------------------------

def test_a_shared_machine_gets_a_light_model_for_when_its_owner_is_busy():
    store = _Store(
        nodes=[_node(installed=["big:30b", "small:3b"], profile="shared")],
        catalog=[_Cat("big:30b", 18, 40), _Cat("small:3b", 2, 8)],
        ability=_flat("big:30b", 7.0) + _flat("small:3b", 6.0),
    )
    roles = {p.role: p.candidate.artifact for p in _node_advice(store).slate}
    assert roles == {"primary": "big:30b", "light": "small:3b"}


def test_an_installed_model_beats_a_download_for_the_light_slot():
    """No catalog entry means no known size — but it is already on disk."""
    store = _Store(
        nodes=[_node(installed=["big:30b", "have:7b"], profile="shared")],
        catalog=[_Cat("big:30b", 18, 40), _Cat("small:3b", 2, 8)],
        ability=_flat("big:30b", 7.0) + _flat("have:7b", 6.5) + _flat("small:3b", 6.0),
    )
    light = next(p for p in _node_advice(store).slate if p.role == "light")
    assert light.candidate.artifact == "have:7b" and light.action == "keep"


def test_a_dedicated_machine_does_not_spend_disk_on_a_light_model():
    store = _Store(
        nodes=[_node(installed=["big:30b"], profile="dedicated")],
        catalog=[_Cat("big:30b", 18, 40), _Cat("small:3b", 2, 8)],
        ability=_flat("big:30b", 7.0) + _flat("small:3b", 6.0),
    )
    assert [p.role for p in _node_advice(store).slate] == ["primary"]


def test_coverage_fills_a_class_the_primary_is_weak_at():
    extract_heavy = {"extract": 400}
    store = _Store(
        nodes=[_node(installed=[])],
        catalog=[_Cat("extractor", 5, 12), _Cat("coder", 5, 12)],
        ability=[_Ability("extractor", tc, 7.0 if tc != "code" else 3.0)
                 for tc in TASK_CLASSES]
        + [_Ability("coder", tc, 4.0 if tc != "code" else 7.0) for tc in TASK_CLASSES],
        demand={"total": 400, "by_class": extract_heavy, "by_capability": {},
                "by_model": {}, "max_tokens_in": 0},
    )
    slate = _node_advice(store).slate
    assert slate[0].candidate.artifact == "extractor"
    assert slate[1].role == "coverage" and slate[1].candidate.artifact == "coder"
    assert "covers code" in slate[1].why


def test_the_slate_respects_the_disk_quota():
    store = _Store(
        nodes=[_node(installed=[], profile="background")],  # 20 GB quota
        catalog=[_Cat("a", 15, 16), _Cat("b", 10, 16)],
        ability=[_Ability("a", tc, 7.0 if tc != "code" else 2.0) for tc in TASK_CLASSES]
        + [_Ability("b", tc, 2.0 if tc != "code" else 7.0) for tc in TASK_CLASSES],
    )
    total = sum(p.candidate.size_gb for p in _node_advice(store).slate)
    assert total <= 20


# --- the demand query, against a real store ----------------------------------------------

@pytest.fixture()
def real_store(tmp_path) -> Store:
    return Store(str(tmp_path / "p.db"))


def _job(db, jid, *, capability="tier-a", task_class="extract", client_key=None,
         created="2026-08-20T00:00:00Z"):
    c = sqlite3.connect(db)
    c.execute("INSERT INTO jobs (id, result_key, capability, status, created_at, "
              "task_class, client_key, urgency, escalated, attempts, cancel_requested) "
              "VALUES (?, ?, ?, 'done', ?, ?, ?, 'waitable', 0, 0, 0)",
              (jid, f"r:{jid}", capability, created, task_class, client_key))
    c.commit()
    c.close()


def test_harness_traffic_is_not_demand(real_store, tmp_path):
    """The eval harness spreads jobs evenly across classes; counted as demand, it would
    make the harness's own design look like what clients want."""
    db = str(tmp_path / "p.db")
    _job(db, "client-1")
    _job(db, "client-2", task_class="summarize")
    _job(db, "eval-1", task_class="code", client_key="cbk:eval")
    _job(db, "perf-new", task_class="reason", client_key="cbk:perf")
    _job(db, "perf-old", task_class="reason")  # predates the tag: known by its sample
    c = sqlite3.connect(db)
    c.execute("INSERT INTO perf_samples (run_id, job_id, category, task_class, min_ability, "
              "phase, submitted_at, outcome) VALUES ('r', 'perf-old', 'c', 'reason', 5, "
              "'p', 0, 'done')")
    c.commit()
    c.close()
    _job(db, "stale", created="2026-01-01T00:00:00Z")

    demand = real_store.real_demand("2026-08-01T00:00:00Z")
    assert demand["total"] == 2
    assert demand["by_class"] == {"extract": 1, "summarize": 1}


# --- gaps --------------------------------------------------------------------------------

def _fleet(**caps) -> Fleet:
    return Fleet(capabilities={k: {"model": v} for k, v in caps.items()})


def _gap_keys(store, fleet):
    return {g.key for g in gaps(store, fleet, now=NOW, scale_version=SCALE_VERSION)}


def test_gaps_name_a_load_bearing_tier_on_one_machine():
    store = _Store(nodes=[_node(caps=["tier-a"], installed=["m"])],
                   demand={"total": 10, "by_class": {}, "by_capability": {"tier-a": 10},
                           "by_model": {}, "max_tokens_in": 0})
    assert "single-node-tier" in _gap_keys(store, _fleet(**{"tier-a": "m"}))


def test_gaps_name_an_idle_tier_and_why():
    store = _Store(nodes=[_node(caps=["busy"], installed=["m"]),
                          _node("node-b", caps=["idle"], installed=["n"])],
                   demand={"total": 100, "by_class": {}, "by_capability": {"busy": 100},
                           "by_model": {}, "max_tokens_in": 0})
    found = gaps(store, _fleet(busy="m", idle="n"), now=NOW, scale_version=SCALE_VERSION)
    idle = next(g for g in found if g.key == "idle-tier")
    assert "speed is not weighed" in idle.detail


def test_gaps_name_a_busy_model_with_no_catalog_entry():
    store = _Store(nodes=[_node(installed=["m"])],
                   demand={"total": 5, "by_class": {}, "by_capability": {"tier-a": 5},
                           "by_model": {"m": 5}, "max_tokens_in": 9000})
    found = gaps(store, _fleet(**{"tier-a": "m"}), now=NOW, scale_version=SCALE_VERSION)
    g = next(g for g in found if g.key == "uncurated-model")
    assert "9,000 tokens" in g.detail


def test_gaps_name_an_offline_node_and_a_job_to_an_undefined_tier():
    store = _Store(nodes=[_node(installed=["m"], heartbeat="2026-08-25T00:00:00Z")],
                   demand={"total": 1, "by_class": {}, "by_capability": {"ghost": 1},
                           "by_model": {}, "max_tokens_in": 0})
    keys = _gap_keys(store, _fleet(**{"tier-a": "m"}))
    assert {"offline-node", "undefined-tier"} <= keys


def test_gaps_note_no_cloud_tier_until_there_is_one():
    store = _Store(nodes=[_node(installed=["m"])])
    assert "no-cloud" in _gap_keys(store, _fleet(**{"tier-a": "m"}))
    cloudy = Fleet(capabilities={"tier-a": {"model": "m"},
                                 "cloud-x": {"model": "p/x", "cloud": True,
                                             "supports_vision": True}})
    keys = _gap_keys(store, cloudy)
    assert "no-cloud" not in keys and "no-vision" not in keys
