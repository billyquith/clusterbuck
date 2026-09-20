"""fleet.yaml registry loader (protocols.md §5)."""

from __future__ import annotations

from pathlib import Path

import pytest

from clusterbuck.fleet import CapabilitySpec, Fleet, NodeSpec, load_fleet, resolve_api_key

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
    with pytest.raises(Exception):
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
