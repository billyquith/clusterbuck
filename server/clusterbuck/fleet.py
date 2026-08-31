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


class CapabilitySpec(BaseModel):
    model_config = {"extra": "forbid"}

    queue: str
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

    @model_validator(mode="after")
    def _no_host_cloud_capabilities_have_no_node(self) -> "Fleet":
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
