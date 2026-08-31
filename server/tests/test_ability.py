"""Ability matrix + need-shaped routing (M5)."""

from __future__ import annotations

import pytest

from clusterbuck.evaluation import SCALE_VERSION, seed_ability
from clusterbuck.fleet import CapabilitySpec, Fleet, NodeSpec
from clusterbuck.routing import NoCapableArtifact, resolve_capability
from clusterbuck.store import Store


def _fleet() -> Fleet:
    return Fleet(
        nodes=[NodeSpec(id="node-a", capabilities=["8b-extract", "32b-reason", "70b-reason"])],
        capabilities={
            "8b-extract": CapabilitySpec(queue="q:8b-extract", model_server="x",
                                         model="llama3.2:3b", price_in_per_1k=0.0002,
                                         price_out_per_1k=0.0006),
            "32b-reason": CapabilitySpec(queue="q:32b-reason", model_server="x",
                                         model="qwen2.5:32b", price_in_per_1k=0.0009,
                                         price_out_per_1k=0.0027),
            "70b-reason": CapabilitySpec(queue="q:70b-reason", model_server="x",
                                         model="llama3.1:70b", price_in_per_1k=0.0027,
                                         price_out_per_1k=0.0035),
        },
    )


@pytest.fixture()
def seeded(tmp_path) -> Store:
    s = Store(str(tmp_path / "ability.db"))
    seed_ability(s, now="t")
    return s


def test_seed_populates_matrix(seeded):
    assert seeded.ability_count(SCALE_VERSION) == 15  # 3 artifacts × 5 task classes
    assert seeded.get_ability("llama3.2:3b", "reason", SCALE_VERSION) == 3.0
    assert seeded.get_ability("llama3.1:70b", "summarize", SCALE_VERSION) == 8.0


def test_seed_is_idempotent(tmp_path):
    s = Store(str(tmp_path / "a.db"))
    assert seed_ability(s, now="t") == 15
    assert seed_ability(s, now="t") == 0  # already populated


def test_routing_picks_cheapest_clearing_the_bar(seeded):
    f = _fleet()

    def route(min_ability):
        return resolve_capability(f, seeded, capability=None,
                                  task_class="summarize", min_ability=min_ability)

    assert route(4) == "8b-extract"   # 3b(4) clears, cheapest
    assert route(7) == "32b-reason"   # 3b out; 32b(7) & 70b(8) clear → cheaper 32b
    assert route(8) == "70b-reason"   # only 70b(8) clears


def test_routing_explicit_capability_wins(seeded):
    assert resolve_capability(_fleet(), seeded, capability="32b-reason",
                              task_class=None, min_ability=None) == "32b-reason"


def test_routing_fails_when_nothing_is_measured(tmp_path):
    """An empty matrix cannot promise an ability floor, so it must refuse rather than guess."""
    empty = Store(str(tmp_path / "empty.db"))  # not seeded
    with pytest.raises(NoCapableArtifact, match="none measured"):
        resolve_capability(_fleet(), empty, capability=None,
                           task_class="summarize", min_ability=6)


def test_routing_fails_explicitly_when_bar_unmeetable(seeded):
    """reason tops out at 7.5. Asking for 10 must fail, not quietly serve a weaker model —
    the client stated a floor and would otherwise never learn it was missed."""
    with pytest.raises(NoCapableArtifact, match="no artifact reaches ability 10"):
        resolve_capability(_fleet(), seeded, capability=None,
                           task_class="reason", min_ability=10)


def test_local_only_excludes_cloud_artifacts(seeded):
    """privacy=local_only must never select a cloud-backed capability, whatever its ability."""
    from clusterbuck.fleet import CapabilitySpec

    f = _fleet()
    f.capabilities["frontier"] = CapabilitySpec(
        queue="q:frontier", model_server="https://api.example.invalid/v1",
        model="frontier-x", cloud=True)
    seeded.set_ability(artifact="frontier-x", task_class="reason", score=10.0,
                       scale_version=SCALE_VERSION, updated_at="t")

    # cloud_ok can use it…
    assert resolve_capability(f, seeded, capability=None, task_class="reason",
                              min_ability=9, privacy="cloud_ok") == "frontier"
    # …local_only cannot, and says so.
    with pytest.raises(NoCapableArtifact, match="excluded by privacy"):
        resolve_capability(f, seeded, capability=None, task_class="reason",
                           min_ability=9, privacy="local_only")


def test_seeded_scores_are_labelled_as_seeds(seeded):
    """Seeds keep routing working, but must not masquerade as measurements."""
    assert seeded.ability_provenance("llama3.2:3b", "reason", SCALE_VERSION) == "seed"


def test_ability_endpoint(client):
    data = client.get("/ability").json()
    assert data["scale_version"] == SCALE_VERSION
    assert len(data["matrix"]) == 15
    assert data["headline"]["llama3.1:70b"] > data["headline"]["llama3.2:3b"]


def test_clear_ability_endpoint_drops_only_that_artifact(client):
    resp = client.post("/ability/clear", params={"artifact": "llama3.2:3b"})
    assert resp.json() == {"artifact": "llama3.2:3b", "cleared": 5}  # 5 task classes

    data = client.get("/ability").json()
    artifacts = {row["artifact"] for row in data["matrix"]}
    assert "llama3.2:3b" not in artifacts
    assert "llama3.1:70b" in artifacts  # untouched


def test_clear_ability_endpoint_unknown_artifact_is_a_noop(client):
    resp = client.post("/ability/clear", params={"artifact": "no-such-artifact"})
    assert resp.json() == {"artifact": "no-such-artifact", "cleared": 0}
