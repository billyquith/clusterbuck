"""Addressing resolution: turn a client's *need* into a supply-side capability.

M0 STUB. The real resolver (model-evaluation.md, ADR 15/16) filters the model catalog
by measured `ability(artifact, task_class) >= min_ability` and privacy, then prefers
local → cheapest → fastest. Until the catalog and ability matrix exist, this maps
`min_ability` onto the seed capability tiers by a crude threshold and ignores
`task_class`. It exists only so clients can already address by need in M0.
"""

from __future__ import annotations


def resolve_capability(
    *,
    capability: str | None,
    task_class: str | None,
    min_ability: int | None,
) -> str:
    if capability is not None:
        return capability
    # Placeholder ladder over the seed tiers. Replaced by the catalog-driven resolver.
    assert min_ability is not None  # guaranteed by JobSubmit validation
    if min_ability <= 4:
        return "8b-extract"
    if min_ability <= 7:
        return "32b-reason"
    return "70b-reason"
