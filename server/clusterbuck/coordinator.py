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


def propose_capabilities(ram_gb: float) -> tuple[list[str], dict[str, list[str]]]:
    """Map a probed RAM size to a proposed capability set + presence-mode ladder.

    M4 STUB: the real proposal weighs accelerator, measured throughput, and the model
    catalog (fleet-management.md). Until that exists, RAM thresholds stand in. The owner
    confirms or edits the proposal — it's a default, not a mandate.
    """
    if ram_gb >= 64:
        caps = ["8b-extract", "32b-reason", "70b-reason"]
    elif ram_gb >= 32:
        caps = ["8b-extract", "32b-reason"]
    else:
        caps = ["8b-extract"]
    # A shared machine runs a small model while active, the big ones when away.
    ladder = {"active": caps[:1], "away": caps}
    return caps, ladder
