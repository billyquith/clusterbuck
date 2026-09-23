"""fleet.yaml registry loader (protocols.md §5)."""

from __future__ import annotations

from pathlib import Path

import pytest
from clusterbuck.fleet import (
    CapabilitySpec,
    Fleet,
    NodeSpec,
    load_fleet,
    resolve_api_key,
    unservable_capabilities,
)

SEED = Path(__file__).resolve().parents[1] / "fleet.yaml"


def test_seed_loads():
    fleet = load_fleet(SEED)
    assert "8b-extract" in fleet.capabilities
    cap = fleet.capabilities["8b-extract"]
    assert fleet.stream_for("8b-extract") == "q:8b-extract"
    assert cap.model_server.startswith("http")
    assert cap.model


def test_nodes_for_capability():
    fleet = load_fleet(SEED)
    ids = {n.id for n in fleet.nodes_for("70b-reason")}
    assert ids == {"node-b"}  # only node-b advertises 70b-reason


def test_rejects_unknown_field(tmp_path):
    bad = tmp_path / "fleet.yaml"
    bad.write_text("nodes: []\ncapabilities: {}\nbogus: 1\n")
    # The specific type, not a blind `Exception`: a bare raises() passes on ANY error,
    # including an unrelated bug in the loader, so it would keep reporting green after
    # the validation it exists to check had stopped working.
    with pytest.raises(ValueError, match="bogus"):
        load_fleet(bad)


def test_empty_fleet_is_valid():
    assert Fleet().capabilities == {}


# --- registered provider accounts (ADR 30) ---

def test_seed_fleet_declares_a_no_host_cloud_capability():
    fleet = load_fleet(SEED)
    cap = fleet.capabilities["claude-sonnet"]
    assert cap.cloud is True
    assert cap.model_server is None  # no host — the coordinator calls it, not a worker
    assert cap.api_key_env == "CBK_ANTHROPIC_API_KEY"
    assert cap.model == "anthropic/claude-3-5-sonnet-20241022"


def test_resolve_api_key_reads_the_named_env_var(monkeypatch):
    spec = CapabilitySpec(queue="q:x", model="anthropic/x", cloud=True,
                          api_key_env="CBK_TEST_PROVIDER_KEY")
    monkeypatch.delenv("CBK_TEST_PROVIDER_KEY", raising=False)
    assert resolve_api_key(spec) is None
    monkeypatch.setenv("CBK_TEST_PROVIDER_KEY", "sk-real")
    assert resolve_api_key(spec) == "sk-real"


def test_resolve_api_key_none_when_unconfigured():
    spec = CapabilitySpec(queue="q:x", model="m")
    assert resolve_api_key(spec) is None


def test_node_cannot_be_assigned_a_no_host_cloud_capability():
    """A registered provider account has no host node — only the coordinator's cloud
    executor can serve it (ADR 30). A node listing it would be a worker that can never
    actually drain that queue, so this fails fast at load rather than silently."""
    with pytest.raises(Exception, match="no worker can serve it"):
        Fleet(
            nodes=[NodeSpec(id="node-a", capabilities=["frontier"])],
            capabilities={"frontier": CapabilitySpec(queue="q:frontier",
                                                      model="anthropic/x", cloud=True)},
        )


def test_hosted_cloud_capability_can_still_be_assigned_to_a_node():
    """The OLDER `cloud: true` + `model_server` shape (a hosted OpenAI-compatible endpoint
    a real worker calls directly) is unaffected — only the no-host shape is rejected."""
    fleet = Fleet(
        nodes=[NodeSpec(id="node-a", capabilities=["hosted-cloud"])],
        capabilities={"hosted-cloud": CapabilitySpec(
            queue="q:hosted-cloud", model_server="https://gateway.example.invalid/v1",
            model="m", cloud=True)},
    )
    assert fleet.nodes_for("hosted-cloud") == fleet.nodes


def test_a_declared_queue_that_disagrees_with_the_contract_is_rejected():
    """Workers derive `q:<capability>` and never read this file, so a declared name that
    differs configures nothing — it just makes the file lie. Fail at load instead."""
    with pytest.raises(ValueError, match="workers derive it"):
        Fleet(capabilities={"8b-extract": CapabilitySpec(
            queue="q:something-else", model="m", model_server="http://x/v1")})


def test_a_declared_queue_that_matches_still_loads():
    """Older files that spell out the derived name keep working."""
    f = Fleet(capabilities={"8b-extract": CapabilitySpec(
        queue="q:8b-extract", model="m", model_server="http://x/v1")})
    assert f.stream_for("8b-extract") == "q:8b-extract"


# --- the registry says one thing, the node runs another (the silent under-serve) --------

def _two_tier_fleet() -> Fleet:
    return Fleet(
        nodes=[NodeSpec(id="n", capabilities=["small", "big"])],
        capabilities={
            "small": CapabilitySpec(model_server="http://h/v1", model="llama3.2:3b"),
            "big": CapabilitySpec(model_server="http://h/v1", model="qwen2.5:32b"),
        },
    )


def test_a_node_running_one_model_across_two_tiers_is_flagged():
    """The signature failure: enrolment succeeds, heartbeats are green, jobs are answered —
    by a model that never cleared the bar the job asked for."""
    warnings = unservable_capabilities(
        _two_tier_fleet(), ["small", "big"], installed=["llama3.2:3b"])
    assert len(warnings) == 1
    assert "'big'" in warnings[0] and "qwen2.5:32b" in warnings[0]


def test_a_node_with_both_models_is_clean():
    assert unservable_capabilities(
        _two_tier_fleet(), ["small", "big"],
        installed=["llama3.2:3b", "qwen2.5:32b"]) == []


def test_an_implicit_latest_tag_is_the_same_artifact():
    """Ollama reports an explicit tag; a registry commonly omits it. Flagging that as a
    mismatch would fire on the most ordinary configuration there is, and a check that
    cries wolf is one nobody reads."""
    fleet = Fleet(capabilities={
        "c": CapabilitySpec(model_server="http://h/v1", model="llama3.2")})
    assert unservable_capabilities(fleet, ["c"], installed=["llama3.2:latest"]) == []
    assert unservable_capabilities(fleet, ["c"], installed=["llama3.2"]) == []


def test_no_inventory_means_unknown_not_empty():
    """An empty `installed` is a model server that did not answer. Warning about every
    tier at that moment would bury the real mismatches in noise."""
    assert unservable_capabilities(_two_tier_fleet(), ["small", "big"], installed=[]) == []


def test_a_provider_account_is_not_the_nodes_job_to_serve():
    """A no-host cloud capability is called by the coordinator in-process, so a node never
    needs its model installed."""
    fleet = Fleet(capabilities={
        "frontier": CapabilitySpec(model="anthropic/x", cloud=True)})
    assert unservable_capabilities(fleet, ["frontier"], installed=["llama3.2:3b"]) == []


def test_an_unknown_tier_is_left_to_the_join_time_warning():
    assert unservable_capabilities(_two_tier_fleet(), ["nonexistent"],
                                   installed=["llama3.2:3b"]) == []


def test_no_fleet_registry_means_nothing_to_check():
    assert unservable_capabilities(None, ["small"], installed=["x"]) == []
