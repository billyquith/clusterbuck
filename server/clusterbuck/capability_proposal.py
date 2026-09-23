"""Node capability proposal: turn probed hardware into a proposed tier set.

Addressing resolution used to live here too, as an M0 stub that mapped `min_ability` onto
the seed tiers by a crude threshold. The real resolver is `routing.resolve_capability`
(ADR 15/16) and has been since the catalog and ability matrix landed; the stub was
unreferenced, and two functions of the same name with different semantics is an invitation
to edit the wrong one.
"""

from __future__ import annotations

# A node with no accelerator runs a large model from system RAM at a fraction of the
# speed. It is not incapable — but proposing a tier it can only serve unusably slowly
# sets an expectation the node cannot keep, and an over-advertised tier is the failure
# this system works hardest to avoid. So the proposal stops one rung short.
CPU_MAX_CAPABILITY_TIERS = 2


def propose_capabilities(
    ram_gb: float, accelerator: str = "cpu", vram_gb: float | None = None,
) -> tuple[list[str], dict[str, list[str]]]:
    """Map probed hardware to a proposed capability set + presence-mode ladder.

    The budget is **VRAM where it is known**, not system RAM. A workstation with 64 GB of
    RAM and an 8 GB card can hold a 70B only by spilling it across the bus every token, at
    roughly an order of magnitude less throughput; proposing the 70B tier there advertises
    a capability it cannot honour. Where VRAM is unknown — a CPU node, or an accelerator we
    could not measure — system RAM is the honest fallback, because that genuinely is the
    memory the model will live in.

    Still a default, not a mandate: the owner confirms or edits it, and the real proposal
    would also weigh measured throughput and the model catalog (fleet-management.md).
    """
    budget = vram_gb if vram_gb else ram_gb
    if budget >= 64:
        caps = ["8b-extract", "32b-reason", "70b-reason"]
    elif budget >= 32:
        caps = ["8b-extract", "32b-reason"]
    else:
        caps = ["8b-extract"]
    if accelerator == "cpu":
        caps = caps[:CPU_MAX_CAPABILITY_TIERS]
    # A shared machine runs a small model while active, the big ones when away.
    ladder = {"active": caps[:1], "away": caps}
    return caps, ladder
