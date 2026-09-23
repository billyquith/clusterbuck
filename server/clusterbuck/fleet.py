"""The static model/capability registry (protocols.md §5).

Loads `fleet.yaml` into typed models. It feeds the sync gateway (LiteLLM deployments,
built from `capabilities`) and is the seed for the future dynamic registry (M4). The
async worker is NOT configured from here in M1 — it stays env-configured; fleet.yaml
describes the fleet for the server's sync plane and coordinator.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, model_validator

from .queue import stream_key


class CapabilitySpec(BaseModel):
    model_config = {"extra": "forbid"}

    # The stream a worker actually reads is DERIVED from the capability name
    # (`q:<capability>`, protocols.md §5) — workers never see fleet.yaml, so they cannot be
    # told a different one. Declaring it here therefore configures nothing; it is accepted
    # only so existing files keep loading, and validated below so a value that disagrees
    # with the contract fails at load instead of silently publishing to a stream nothing
    # drains. Use `stream_for()` to get the real name.
    queue: str | None = None
    # OpenAI-compatible base URL the sync plane (LiteLLM) calls directly. None for a
    # registered provider account (ADR 30): those have no host — the coordinator calls the
    # provider natively via LiteLLM, keyed by `model`'s "<provider>/<model>" form.
    model_server: str | None = None
    model: str  # served model name at that endpoint, or a LiteLLM "<provider>/<model>" id
    # Cloud-equivalent price per 1k tokens. For a local capability this is the rate a local
    # run *avoids* paying (the avoided-cloud-spend headline); for a cloud one it is the
    # provider's actual rate (real budget spend, ADR 30). Split input/output because output
    # typically runs several times pricier.
    price_in_per_1k: float = 0.0
    price_out_per_1k: float = 0.0
    # True for a capability served off-LAN (a hosted OpenAI-compatible endpoint or a
    # registered provider account). `privacy: local_only` jobs never route here (ADR 14) —
    # without this flag the invariant was unenforceable, since model_server is just a URL
    # and could point anywhere.
    cloud: bool = False
    # Name of the environment variable holding this provider's API key (ADR 30) — never the
    # key itself, so fleet.yaml stays committable. The coordinator reads it; a worker never
    # sees it. Unset env at routing time excludes the artifact rather than failing startup.
    api_key_env: str | None = None
    # Capabilities of this tier's model (ADR 37), for a provider account only. A local
    # artifact's features live in the model catalog, which is the coordinator's record of
    # what it knows about artifacts it can install — but a provider account has no host and
    # never enters that catalog, so the operator who registered it declares them here. The
    # two sources cover disjoint sets of artifacts and never disagree.
    context_tokens: int | None = None
    supports_tools: bool | None = None
    supports_json_schema: bool | None = None
    supports_vision: bool | None = None


class NodeSpec(BaseModel):
    model_config = {"extra": "forbid"}

    id: str
    mac: str | None = None
    wake: Literal["opportunistic", "scheduled", "on-demand"] = "opportunistic"
    capabilities: list[str]


class Fleet(BaseModel):
    model_config = {"extra": "forbid"}

    nodes: list[NodeSpec] = []
    capabilities: dict[str, CapabilitySpec] = {}

    def nodes_for(self, capability: str) -> list[NodeSpec]:
        return [n for n in self.nodes if capability in n.capabilities]

    def price(self, capability: str) -> tuple[float, float]:
        """(input, output) cloud-equivalent price per 1k tokens; (0, 0) if unpriced."""
        spec = self.capabilities.get(capability)
        return (spec.price_in_per_1k, spec.price_out_per_1k) if spec else (0.0, 0.0)

    def stream_for(self, capability: str) -> str:
        """The base stream for a capability — the contract, not whatever was declared."""
        return stream_key(capability)

    @model_validator(mode="after")
    def _declared_queues_match_the_contract(self) -> Fleet:
        """A declared `queue:` that disagrees with `q:<capability>` is a silent trap.

        The server would keep publishing to the derived stream while the file claims
        otherwise — or, if it were ever honoured, publish where no worker reads, since a
        worker derives its streams from its own capability list and never loads this file.
        Neither failure surfaces anywhere, so reject the mismatch at load.
        """
        for name, spec in self.capabilities.items():
            expected = stream_key(name)
            if spec.queue is not None and spec.queue != expected:
                raise ValueError(
                    f"capability {name!r} declares queue {spec.queue!r}, but the stream is "
                    f"always {expected!r} (protocols.md §5) — workers derive it from the "
                    f"capability name and never read this file. Remove the line, or rename "
                    f"the capability."
                )
        return self

    @model_validator(mode="after")
    def _no_host_cloud_capabilities_have_no_node(self) -> Fleet:
        """A registered provider account (ADR 30) has no host node — the coordinator calls
        it in-process, never a worker. Assigning one to a node would enroll a worker that
        can never actually serve it, so fail fast at load rather than silently."""
        for node in self.nodes:
            for cap in node.capabilities:
                spec = self.capabilities.get(cap)
                if spec is not None and spec.cloud and spec.model_server is None:
                    raise ValueError(
                        f"node {node.id!r} lists {cap!r}, a no-host cloud capability "
                        f"(provider account) — no worker can serve it, only the coordinator"
                    )
        return self


def resolve_api_key(spec: CapabilitySpec) -> str | None:
    """The API key for a provider-account capability, read from its named env var.

    Never a value stored in fleet.yaml itself (ADR 30) — only the variable NAME is
    configured, so the file stays committable. Missing/unset ⇒ None; callers exclude the
    artifact from routing rather than treating this as fatal (a fleet with an unconfigured
    provider must still boot for its local-only capabilities).
    """
    return os.environ.get(spec.api_key_env) if spec.api_key_env else None


def load_fleet(path: str | Path) -> Fleet:
    data = yaml.safe_load(Path(path).read_text()) or {}
    return Fleet.model_validate(data)


def _artifact_aliases(name: str) -> set[str]:
    """The spellings that mean the same artifact to a model server.

    Ollama's `/v1/models` reports an explicit tag, so a registry that says `llama3.2` and a
    node that reports `llama3.2:latest` are the same thing. Treating them as different
    would make the coherence check below cry wolf on the most ordinary configuration there
    is, and a check that cries wolf gets ignored.
    """
    name = name.strip()
    base = name.split(":", 1)[0]
    return {name, base, f"{base}:latest"}


def unservable_capabilities(
    fleet: Fleet | None, advertised: list[str], installed: list[str],
) -> list[str]:
    """Capabilities this node serves whose model it does not actually have installed.

    This is the gap between what the registry SAYS a tier is and what the node can really
    run. `fleet.yaml`'s `model:` decides whether a job clears its `min_ability` bar; the
    worker answers with whatever `CBK_MODEL` names, for every tier it serves. Nothing
    reconciled the two, so a node advertising both an 8B and a 32B tier while running a 7B
    answered the 32B tier's jobs with that 7B — at whatever quality that implies, silently,
    and reported as having met the floor.

    Read-only and advisory. It names the mismatch so an operator can fix it (a model that
    fits the tier, or a narrower CBK_CAPABILITIES) before enforcement makes those jobs fail
    instead of quietly under-serving.

    A node that reports no inventory at all gets no warnings: an empty `installed` means
    the model server did not answer, not that it is empty.
    """
    if fleet is None or not installed:
        return []
    have: set[str] = set()
    for artifact in installed:
        have |= _artifact_aliases(artifact)
    out = []
    for cap in advertised:
        spec = fleet.capabilities.get(cap)
        # Unknown tier: a different failure, already reported at join. A no-host cloud
        # capability is served by the coordinator, never by this node's model server.
        if spec is None or (spec.cloud and spec.model_server is None):
            continue
        if not (_artifact_aliases(spec.model) & have):
            out.append(
                f"serves {cap!r}, whose registered model is {spec.model!r}, but that model "
                f"is not installed here — jobs routed to {cap!r} on the strength of "
                f"{spec.model!r}'s ability score will be answered by something else"
            )
    return out
