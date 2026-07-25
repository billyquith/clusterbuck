"""Need-shaped routing (ADR 16 / model-evaluation.md): turn {task_class, min_ability,
privacy} into a concrete capability using the measured ability matrix.

Selection: filter artifacts by `ability(artifact, task_class) ≥ min_ability` (and, when
cloud artifacts exist, by privacy) → prefer local → cheapest. Explicit `capability`
addressing still wins for power users. If the matrix has nothing that clears the bar (e.g.
before any eval has run), fall back to the coarse RAM-tier stub so the system still routes.
"""

from __future__ import annotations

from .coordinator import resolve_capability as _threshold_stub
from .evaluation import SCALE_VERSION
from .fleet import Fleet
from .store import Store


def resolve_capability(
    fleet: Fleet | None,
    store: Store,
    *,
    capability: str | None,
    task_class: str | None,
    min_ability: int | None,
    privacy: str = "local_only",
    scale_version: str = SCALE_VERSION,
) -> str:
    if capability is not None:
        return capability

    if fleet is not None and task_class is not None and min_ability is not None:
        # Candidates whose artifact clears the ability bar for this task class.
        candidates: list[tuple[float, str]] = []
        for cap, spec in fleet.capabilities.items():
            score = store.get_ability(spec.model, task_class, scale_version)
            if score is not None and score >= min_ability:
                # All fleet artifacts are local; cheapest wins (prefer-local is implicit).
                candidates.append((spec.price_in_per_1k + spec.price_out_per_1k, cap))
        if candidates:
            candidates.sort()  # cheapest first
            return candidates[0][1]

    # Nothing measured clears the bar yet → coarse fallback.
    return _threshold_stub(capability=None, task_class=task_class, min_ability=min_ability)
