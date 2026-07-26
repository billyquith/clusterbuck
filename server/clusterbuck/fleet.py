"""The static model/capability registry (protocols.md §5).

Loads `fleet.yaml` into typed models. It feeds the sync gateway (LiteLLM deployments,
built from `capabilities`) and is the seed for the future dynamic registry (M4). The
async worker is NOT configured from here in M1 — it stays env-configured; fleet.yaml
describes the fleet for the server's sync plane and coordinator.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel


class CapabilitySpec(BaseModel):
    model_config = {"extra": "forbid"}

    queue: str
    model_server: str  # OpenAI-compatible base URL the sync plane (LiteLLM) calls directly
    model: str  # served model name at that endpoint
    # Cloud-equivalent price per 1k tokens: the rate a local run *avoids* paying. Split
    # input/output because output typically runs several times pricier — and this feeds
    # the headline avoided-cloud-spend number (fleet-management.md → Usage accounting).
    price_in_per_1k: float = 0.0
    price_out_per_1k: float = 0.0
    # True for a capability served off-LAN (a hosted OpenAI-compatible endpoint or provider).
    # `privacy: local_only` jobs never route here (ADR 14) — without this flag the invariant
    # was unenforceable, since model_server is just a URL and could point anywhere.
    cloud: bool = False


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


def load_fleet(path: str | Path) -> Fleet:
    data = yaml.safe_load(Path(path).read_text()) or {}
    return Fleet.model_validate(data)
