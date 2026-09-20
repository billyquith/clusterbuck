"""Ability matrix + need-shaped routing (M5)."""

from __future__ import annotations

import json

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
    assert seeded.ability_count(SCALE_VERSION) == 12  # 3 artifacts × 4 task classes
    assert seeded.get_ability("llama3.2:3b", "reason", SCALE_VERSION) == 3.0
    # The 70B sits exactly on its anchor (7 = "strong 70B-class local"), not above it:
    # nothing in a fresh install claims a band no instrument here can justify.
    assert seeded.get_ability("llama3.1:70b", "summarize", SCALE_VERSION) == 7.0


def test_seed_is_idempotent(tmp_path):
    s = Store(str(tmp_path / "a.db"))
    assert seed_ability(s, now="t") == 12
    assert seed_ability(s, now="t") == 0  # already populated


def test_routing_picks_cheapest_clearing_the_bar(seeded):
    f = _fleet()

    def route(min_ability):
        return resolve_capability(f, seeded, capability=None,
                                  task_class="summarize", min_ability=min_ability)

    assert route(4) == "8b-extract"   # 3b(4) clears, cheapest
    assert route(7) == "70b-reason"   # only the 70B seed reaches its 7 anchor


def test_a_floor_above_the_tier1_ceiling_fails_loudly(seeded):
    """8-10 is the judged band, and the judged tiers are deferred. Nothing in a default
    install has an instrument that can certify one, so asking for it fails explicitly
    rather than being quietly served by the best thing to hand — the same invariant that
    governs every other unmet floor."""
    with pytest.raises(NoCapableArtifact, match="no artifact reaches ability 9"):
        resolve_capability(_fleet(), seeded, capability=None,
                           task_class="summarize", min_ability=9)


def test_routing_explicit_capability_wins(seeded):
    assert resolve_capability(_fleet(), seeded, capability="32b-reason",
                              task_class=None, min_ability=None) == "32b-reason"


def test_routing_fails_explicitly_for_unknown_capability(seeded):
    """A typo'd capability must fail fast (422) rather than park in a stream no worker
    consumes — the same explicit-failure guarantee need-shaped addressing already gets."""
    with pytest.raises(NoCapableArtifact, match="unknown capability '70b-genius'"):
        resolve_capability(_fleet(), seeded, capability="70b-genius",
                           task_class=None, min_ability=None)


def test_routing_fails_explicitly_for_capability_with_no_fleet(seeded):
    with pytest.raises(NoCapableArtifact, match="no fleet registry is configured"):
        resolve_capability(None, seeded, capability="8b-extract",
                           task_class=None, min_ability=None)


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
    """privacy=local_only must never select a cloud-backed capability, whatever its ability.

    urgency="necessary" here isolates the privacy check from the separate urgency gate
    (ADR 18/30, tested below): a `waitable` job (the default) never reaches cloud at all,
    regardless of privacy, so this test picks the urgency that lets privacy be the only
    variable.
    """
    from clusterbuck.fleet import CapabilitySpec

    f = _fleet()
    f.capabilities["frontier"] = CapabilitySpec(
        queue="q:frontier", model_server="https://api.example.invalid/v1",
        model="frontier-x", cloud=True)
    seeded.set_ability(artifact="frontier-x", task_class="reason", score=10.0,
                       scale_version=SCALE_VERSION, updated_at="t")

    # cloud_ok + necessary can use it…
    assert resolve_capability(f, seeded, capability=None, task_class="reason",
                              min_ability=9, privacy="cloud_ok",
                              urgency="necessary") == "frontier"
    # …local_only cannot, and says so.
    with pytest.raises(NoCapableArtifact, match="excluded by privacy"):
        resolve_capability(f, seeded, capability=None, task_class="reason",
                           min_ability=9, privacy="local_only", urgency="necessary")


def test_waitable_never_reaches_cloud(seeded):
    """ADR 18/30: waitable never creates capacity for itself — no wake, no cloud, no
    demand — even when privacy would otherwise allow it and no local artifact clears the
    bar. This is a wake-rights question, checked before budget is ever considered."""
    from clusterbuck.fleet import CapabilitySpec

    f = _fleet()
    f.capabilities["frontier"] = CapabilitySpec(
        queue="q:frontier", model_server="https://api.example.invalid/v1",
        model="frontier-x", cloud=True)
    seeded.set_ability(artifact="frontier-x", task_class="reason", score=10.0,
                       scale_version=SCALE_VERSION, updated_at="t")

    with pytest.raises(NoCapableArtifact, match="no artifact reaches ability 9"):
        resolve_capability(f, seeded, capability=None, task_class="reason",
                           min_ability=9, privacy="cloud_ok", urgency="waitable")


def test_explicit_cloud_capability_still_enforces_privacy_and_urgency(seeded):
    """Explicit `capability` addressing is a power-user shortcut, not a bypass: naming a
    cloud capability directly must still respect ADR 14/18, matching fleet-management.md's
    "submission validation rejects incoherent combos... fast, at the API"."""
    from clusterbuck.fleet import CapabilitySpec

    f = _fleet()
    f.capabilities["frontier"] = CapabilitySpec(
        queue="q:frontier", model_server="https://api.example.invalid/v1",
        model="frontier-x", cloud=True)

    with pytest.raises(NoCapableArtifact, match="local_only"):
        resolve_capability(f, seeded, capability="frontier", task_class=None,
                           min_ability=None, privacy="local_only", urgency="necessary")
    with pytest.raises(NoCapableArtifact, match="waitable"):
        resolve_capability(f, seeded, capability="frontier", task_class=None,
                           min_ability=None, privacy="cloud_ok", urgency="waitable")
    # necessary + cloud_ok is the coherent combo — it still wins explicitly.
    assert resolve_capability(f, seeded, capability="frontier", task_class=None,
                              min_ability=None, privacy="cloud_ok",
                              urgency="necessary") == "frontier"


def test_local_preferred_over_cheaper_cloud(seeded):
    """ADR 16: prefer local, THEN cheapest — a cloud artifact that happens to be cheaper
    than a qualifying local one must still lose to it."""
    from clusterbuck.fleet import CapabilitySpec

    f = _fleet()
    # Cheaper than every local capability, and clears the bar.
    f.capabilities["bargain-cloud"] = CapabilitySpec(
        queue="q:bargain-cloud", model_server="https://api.example.invalid/v1",
        model="bargain-x", cloud=True, price_in_per_1k=0.00001, price_out_per_1k=0.00001)
    seeded.set_ability(artifact="bargain-x", task_class="summarize", score=9.0,
                       scale_version=SCALE_VERSION, updated_at="t")

    assert resolve_capability(f, seeded, capability=None, task_class="summarize",
                              min_ability=4, privacy="cloud_ok",
                              urgency="necessary") == "8b-extract"


def test_cloud_candidate_excluded_by_exhausted_budget(seeded):
    """A cloud candidate that would otherwise win must be excluded once the paced budget
    (necessary) is exhausted, and the failure names the budget rather than looking like a
    plain ability miss."""
    from clusterbuck.fleet import CapabilitySpec

    f = Fleet(capabilities={
        "frontier": CapabilitySpec(queue="q:frontier", model_server="x",
                                   model="frontier-x", cloud=True),
    })
    seeded.set_ability(artifact="frontier-x", task_class="reason", score=10.0,
                       scale_version=SCALE_VERSION, updated_at="t")
    seeded.record_usage(job_id="already-spent", ts="t", capability="frontier",
                        model="frontier-x", node="cloud:x", venue="cloud",
                        tokens_in=0, tokens_out=0, outcome="done", cost=100.0, day="2026-01-01")

    with pytest.raises(NoCapableArtifact, match="budget"):
        resolve_capability(f, seeded, capability=None, task_class="reason", min_ability=9,
                           privacy="cloud_ok", urgency="necessary",
                           cloud_budget_monthly=10.0, now=1767225600.0)  # 2026-01-01 UTC


def test_seeded_scores_are_labelled_as_seeds(seeded):
    """Seeds keep routing working, but must not masquerade as measurements."""
    assert seeded.ability_provenance("llama3.2:3b", "reason", SCALE_VERSION) == "seed"


def test_ability_endpoint(client):
    data = client.get("/ability").json()
    assert data["scale_version"] == SCALE_VERSION
    assert len(data["matrix"]) == 12
    assert data["headline"]["llama3.1:70b"] > data["headline"]["llama3.2:3b"]


def test_clear_ability_endpoint_drops_only_that_artifact(client):
    resp = client.post("/ability/clear", params={"artifact": "llama3.2:3b"})
    # `generation` alongside `cleared`: dropping the score is only half the reset, since
    # ability is recomputed from the artifact's eval_runs (ADR 15).
    assert resp.json() == {"artifact": "llama3.2:3b", "cleared": 4, "generation": 2}

    data = client.get("/ability").json()
    artifacts = {row["artifact"] for row in data["matrix"]}
    assert "llama3.2:3b" not in artifacts
    assert "llama3.1:70b" in artifacts  # untouched
    assert client.app.state.store.current_eval_generation("llama3.1:70b") == 1


def test_clear_ability_endpoint_unknown_artifact_is_a_noop(client):
    resp = client.post("/ability/clear", params={"artifact": "no-such-artifact"})
    assert resp.json() == {"artifact": "no-such-artifact", "cleared": 0, "generation": 2}


def test_clearing_twice_keeps_advancing_the_generation(client):
    """Each reset must be its own round: an operator who clears, sees a bad batch, and
    clears again must not have the second measurement blended with the first."""
    for expected in (2, 3, 4):
        body = client.post("/ability/clear", params={"artifact": "llama3.2:3b"}).json()
        assert body["generation"] == expected


# --- speed: the half of "need-shaped" that ability structurally cannot express ----------

def _node_row(node_id: str, caps: list[str], tps: float | None):
    from clusterbuck.orm.node import Node
    return Node(node_id=node_id, node_key="k", profile="shared",
                capabilities=json.dumps(caps), tps=tps, enrolled_at="t")


@pytest.fixture()
def seeded_with_nodes(seeded, monkeypatch):
    """Ability seeds plus measured speeds on every tier: a slow small-model box and a
    faster one carrying both larger tiers."""
    rows = [_node_row("slow", ["8b-extract"], 4.0),
            _node_row("fast", ["32b-reason", "70b-reason"], 55.0)]
    monkeypatch.setattr(seeded, "list_nodes", lambda: rows)
    return seeded


def test_a_capability_too_slow_for_the_floor_is_excluded(seeded_with_nodes):
    """The 3B clears ability 4 and is cheapest, but the only node serving it measures
    4 tok/s. A caller that asked for 20 would otherwise be silently handed it."""
    got = resolve_capability(_fleet(), seeded_with_nodes, capability=None,
                             task_class="summarize", min_ability=4, min_tps=20)
    assert got == "32b-reason", "fell back to the slow-but-cheap tier"


def test_the_failure_names_speed_separately_from_ability(seeded_with_nodes):
    """'not good enough' and 'not fast enough' need completely different fixes. A caller
    told only the first would go hunting for a better model it already has."""
    with pytest.raises(NoCapableArtifact, match="min_tps"):
        resolve_capability(_fleet(), seeded_with_nodes, capability=None,
                           task_class="summarize", min_ability=4, min_tps=500)


def test_an_unmeasured_fleet_is_not_treated_as_slow(seeded):
    """A fresh fleet has finished no jobs. Reading unknown as too slow would fail every
    speed-sensitive request on a healthy new install."""
    assert resolve_capability(_fleet(), seeded, capability=None, task_class="summarize",
                              min_ability=4, min_tps=999) == "8b-extract"


def test_explicit_addressing_is_checked_for_speed_too(seeded_with_nodes):
    """Naming a capability directly is a power-user shortcut, not a bypass — the same rule
    privacy and budget already follow there."""
    with pytest.raises(NoCapableArtifact, match="below the requested"):
        resolve_capability(_fleet(), seeded_with_nodes, capability="8b-extract",
                           task_class=None, min_ability=None, min_tps=20)


# --- requirements: what a model CAN DO, which no 1-10 score can express -----------------

def _requires(**kw):
    from clusterbuck.models import Requirements
    return Requirements(**kw)


@pytest.fixture()
def curated(seeded, tmp_path):
    """Ability seeds plus a catalog: the 3B has a huge window and no vision, the 32B a
    small window, the 70B neither curated."""
    seeded.upsert_catalog(artifact="llama3.2:3b", family=None, params_b=None, quant=None,
                          size_gb=2, min_ram_gb=8, source="ollama",
                          registry_ref="llama3.2:3b", expected_ability=4.0, added_at="t",
                          context_tokens=131072, supports_tools=True,
                          supports_json_schema=True, supports_vision=False)
    seeded.upsert_catalog(artifact="qwen2.5:32b", family=None, params_b=None, quant=None,
                          size_gb=20, min_ram_gb=48, source="ollama",
                          registry_ref="qwen2.5:32b", expected_ability=7.0, added_at="t",
                          context_tokens=32768, supports_tools=True,
                          supports_json_schema=True, supports_vision=False)
    return seeded


def test_a_context_window_too_small_excludes_the_artifact(curated):
    """A 32k model and a 128k one can both be 'a 6 at summarize'. Sending a 60k document
    to the first silently truncates it, and no ability score can see the difference."""
    got = resolve_capability(_fleet(), curated, capability=None, task_class="summarize",
                             min_ability=4, requires=_requires(context_tokens=60000))
    assert got == "8b-extract", "picked a tier whose model cannot hold the prompt"


def test_a_feature_nothing_declares_fails_explicitly(curated):
    """Vision is a yes/no fact, so 'nearly' is not an option. Serving it on a text-only
    model would fail at the model server, where it looks like a model bug."""
    with pytest.raises(NoCapableArtifact, match="requirements unmet"):
        resolve_capability(_fleet(), curated, capability=None, task_class="summarize",
                           min_ability=4, requires=_requires(vision=True))


def test_an_uncurated_artifact_is_excluded_and_named(curated):
    """The 70B has no catalog row. Excluding it is the conservative reading, and the
    message has to say WHICH artifact to curate or the operator cannot act on it."""
    with pytest.raises(NoCapableArtifact, match="llama3.1:70b does not declare"):
        resolve_capability(_fleet(), curated, capability=None, task_class="summarize",
                           min_ability=7, requires=_requires(tools=True))


def test_requirements_are_filtered_before_ability_is_compared(curated):
    """A capable-but-unsuitable artifact must not win on ability. Order matters: filter
    on what a model CAN do, then compare how WELL it does it."""
    got = resolve_capability(_fleet(), curated, capability=None, task_class="summarize",
                             min_ability=4, requires=_requires(context_tokens=100000))
    assert got == "8b-extract"   # the 32B scores higher but holds only 32k


def test_no_requirements_means_no_filtering(curated):
    assert resolve_capability(_fleet(), curated, capability=None, task_class="summarize",
                              min_ability=4, requires=None) == "8b-extract"


def test_explicit_addressing_is_checked_for_requirements_too(curated):
    with pytest.raises(NoCapableArtifact, match="cannot meet the job's requirements"):
        resolve_capability(_fleet(), curated, capability="32b-reason", task_class=None,
                           min_ability=None, requires=_requires(context_tokens=100000))


def test_a_provider_account_declares_its_own_features(seeded):
    """A cloud artifact has no host and never enters the catalog, so fleet.yaml is where
    its capabilities live. Without that it could never serve a job requiring one."""
    f = _fleet()
    f.capabilities["frontier"] = CapabilitySpec(
        model="anthropic/x", cloud=True, context_tokens=200000, supports_vision=True)
    seeded.set_ability(artifact="anthropic/x", task_class="summarize", score=7.0,
                       scale_version=SCALE_VERSION, updated_at="t")
    assert resolve_capability(f, seeded, capability=None, task_class="summarize",
                              min_ability=7, privacy="cloud_ok", urgency="necessary",
                              requires=_requires(vision=True)) == "frontier"
