"""Node capability proposal: turn probed hardware into a proposed tier set.

Addressing resolution used to live here too, as an M0 stub that mapped `min_ability` onto
the seed tiers by a crude threshold. The real resolver is `routing.resolve_capability`
(ADR 15/16) and has been since the catalog and ability matrix landed; the stub was
unreferenced, and two functions of the same name with different semantics is an invitation
to edit the wrong one.
"""

from __future__ import annotations


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
