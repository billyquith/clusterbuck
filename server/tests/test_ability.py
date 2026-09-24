"""Ability matrix + need-shaped routing (M5)."""

from __future__ import annotations

import json

import pytest
from clusterbuck.evaluation import SCALE_VERSION, seed_ability
from clusterbuck.fleet import CapabilitySpec, Fleet, NodeSpec
from clusterbuck.routing import (
    MissingCapability,
    NoCapableArtifact,
    resolve,
    resolve_capability,
)
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


# --- requirements: what a model CAN DO, which no 1-10 score can express -----------------


# --- the measurable ceiling: an impossible bar must say so ------------------------------

def test_a_bar_above_the_ceiling_says_so_rather_than_sending_you_model_shopping(seeded):
    """`min_ability` 8-10 can never be met — by anything, cloud included.

    `score_to_ability` clamps every write to TIER1_MAX_ABILITY because tier-1 is a
    compliance instrument and cannot certify the frontier band; the 8-10 range is
    reserved for the judged tiers, which are not built. The failure used to read "no
    artifact reaches ability 9", which sends the caller looking for a better model. No
    better model can exist yet, and cloud provider accounts are scored by the same
    clamped harness, so they cannot clear it either.
    """
    from clusterbuck.evaluation import TIER1_MAX_ABILITY

    with pytest.raises(NoCapableArtifact) as e:
        resolve_capability(_fleet(), seeded, capability=None,
                           task_class="summarize", min_ability=9)

    assert "ceiling" in str(e.value)
    assert f"{TIER1_MAX_ABILITY:g}" in str(e.value)
    assert "cloud included" in str(e.value), "the caller must not go shopping for cloud"


def test_the_ceiling_itself_is_reachable(seeded):
    """7 is attainable, which is what makes 8 the first impossible bar.

    Written as the boundary case on purpose: if the clamp ever moved, a test that only
    checked 9 would keep passing while the advertised range quietly changed meaning.
    """
    from clusterbuck.evaluation import TIER1_MAX_ABILITY

    cap = resolve_capability(_fleet(), seeded, capability=None,
                             task_class="summarize", min_ability=int(TIER1_MAX_ABILITY))
    assert cap, "the ceiling value must be servable, not just legal"


# --- ADR 37: what a model CAN DO is a filter, not a score ---------------------------
#
# The storage half of this shipped long ago — catalog columns, a dedicated migration
# (0008), fleet.yaml fields, a dashboard column — and `catalog.py` asserted the fields
# "ARE load-bearing at routing time". `routing.py` had never referenced one of them.


def _catalogued(store, artifact, **features):
    """Record what an artifact is declared able to do."""
    store.upsert_catalog(artifact=artifact, family="f", params_b=8.0, quant="Q4_K_M",
                         size_gb=5.0, min_ram_gb=12.0, source="ollama",
                         registry_ref=artifact, expected_ability=None,
                         added_at="t", **features)


def test_an_artifact_that_does_not_declare_a_required_feature_is_excluded(seeded):
    """The core of it. Both models clear the ability bar; only one declares tools, and
    the requirement is applied BEFORE ability is compared — otherwise the better
    summariser wins on score and the requirement is decided by an unrelated number."""
    _catalogued(seeded, "llama3.2:3b", supports_tools=True)
    _catalogued(seeded, "qwen2.5:32b", supports_tools=False)

    sel = resolve(_fleet(), seeded, capability=None, task_class="extract", min_ability=3,
                  requires={"tools": True})

    assert sel.artifact == "llama3.2:3b"


def test_undeclared_reads_as_no(seeded):
    """Deliberately the opposite of how speed is treated. An unmeasured SPEED costs at
    most a slower answer and self-corrects as measurements arrive. An undeclared FEATURE
    has no backstop: it fails at the model server, where it reads as a model bug rather
    than a routing one, or succeeds while quietly ignoring the request."""
    _catalogued(seeded, "llama3.2:3b")  # in the catalog, declares nothing

    with pytest.raises(MissingCapability) as e:
        resolve(_fleet(), seeded, capability=None, task_class="extract", min_ability=3,
                requires={"tools": True})
    assert "tools" in str(e.value)


def test_a_context_window_is_compared_not_just_checked_for_presence(seeded):
    """The case the ability matrix structurally cannot see: a 4k model and a 128k one can
    both honestly be "a 6 at summarize", and routing a 60k document to the first
    truncates it silently."""
    _catalogued(seeded, "llama3.2:3b", context_tokens=8192)
    _catalogued(seeded, "qwen2.5:32b", context_tokens=131072)

    sel = resolve(_fleet(), seeded, capability=None, task_class="summarize",
                  min_ability=3, requires={"context_tokens": 60000})
    assert sel.artifact == "qwen2.5:32b"

    with pytest.raises(MissingCapability):
        resolve(_fleet(), seeded, capability=None, task_class="summarize",
                min_ability=3, requires={"context_tokens": 200_000})


def test_the_refusal_names_the_artifact_and_the_missing_feature(seeded):
    """A missing DECLARATION and a missing ABILITY need completely different fixes: the
    first is usually one POST /catalog away because the model can in fact do the thing,
    the second needs a better model. Telling the operator "no artifact reaches ability 5"
    when the real problem is an unrecorded feature sends them hunting for the wrong
    thing."""
    _catalogued(seeded, "llama3.2:3b", context_tokens=8192)

    with pytest.raises(MissingCapability) as e:
        resolve(_fleet(), seeded, capability=None, task_class="extract", min_ability=3,
                requires={"context_tokens": 100_000})
    msg = str(e.value)
    assert "llama3.2:3b" in msg and "8192" in msg and "100000" in msg


def test_explicit_capability_addressing_is_filtered_too(seeded):
    """The advanced form is not a bypass — the same reason privacy and budget are checked
    there. A caller naming a tier is told it cannot do what they asked for, rather than
    finding out from a truncated answer."""
    _catalogued(seeded, "qwen2.5:32b", supports_vision=False)

    with pytest.raises(MissingCapability) as e:
        resolve(_fleet(), seeded, capability="32b-reason", task_class=None,
                min_ability=None, requires={"vision": True})
    assert "32b-reason" in str(e.value) and "vision" in str(e.value)


def test_a_job_requiring_nothing_is_untouched(seeded):
    """The overwhelmingly common case, and it must not acquire ceremony — nor start
    depending on the catalog being populated."""
    assert resolve(_fleet(), seeded, capability=None, task_class="extract",
                   min_ability=3).artifact
    assert resolve(_fleet(), seeded, capability=None, task_class="extract",
                   min_ability=3, requires={}).artifact


def test_false_is_not_a_requirement(seeded):
    """`vision: false` means "I don't need vision", not "exclude models that have it".
    Only truthy values are requirements — `Requires.asked_for` drops the rest."""
    from clusterbuck.models import Requires

    assert Requires(vision=False, tools=True).asked_for() == {"tools": True}
    assert Requires().asked_for() == {}

    _catalogued(seeded, "llama3.2:3b", supports_vision=True)
    assert resolve(_fleet(), seeded, capability=None, task_class="extract",
                   min_ability=3, requires={"vision": False}).artifact
