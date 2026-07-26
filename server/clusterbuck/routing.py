"""Need-shaped routing (ADR 16 / model-evaluation.md): turn {task_class, min_ability,
privacy} into a concrete capability using the measured ability matrix.

Selection: filter artifacts by `ability(artifact, task_class) ≥ min_ability` and by the job's
privacy class → prefer local → cheapest. Explicit `capability` addressing still wins for power
users.

If nothing clears the bar the request **fails explicitly** (`NoCapableArtifact` → 422) rather
than quietly running on a weaker model. Silently under-serving defeats the whole point of
need-shaped addressing: the client stated a floor and would have no way to learn it was missed.
Anchored seed scores (`provenance='seed'`) keep this workable before anything is measured.
"""

from __future__ import annotations

from .evaluation import SCALE_VERSION
from .fleet import Fleet
from .store import Store


class NoCapableArtifact(Exception):
    """No artifact clears the requested ability bar within the job's privacy class.

    model-evaluation.md is explicit: a `min_ability` no local artifact can meet routes to
    cloud (if `cloud_ok`) or **fails explicitly** (if `local_only`). Quietly serving the job
    on a weaker model defeats need-shaped addressing — the client asked for a floor and would
    have no way to know it was not met.
    """


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

    if fleet is None or task_class is None or min_ability is None:
        raise NoCapableArtifact(
            "need-shaped routing requires a fleet registry plus task_class and min_ability"
        )

    # Candidates whose artifact clears the ability bar for this task class, within privacy.
    candidates: list[tuple[float, str]] = []
    excluded_by_privacy = 0
    best_available: float | None = None
    for cap, spec in fleet.capabilities.items():
        if privacy == "local_only" and spec.cloud:
            excluded_by_privacy += 1
            continue  # a local_only job never leaves the LAN, whatever its ability (ADR 14)
        score = store.get_ability(spec.model, task_class, scale_version)
        if score is None:
            continue
        best_available = score if best_available is None else max(best_available, score)
        if score >= min_ability:
            candidates.append((spec.price_in_per_1k + spec.price_out_per_1k, cap))

    if candidates:
        candidates.sort()  # prefer local (all local here) → cheapest
        return candidates[0][1]

    detail = (
        f"no artifact reaches ability {min_ability} for task_class {task_class!r}"
        f" (best available: {best_available if best_available is not None else 'none measured'})"
    )
    if excluded_by_privacy:
        detail += f"; {excluded_by_privacy} cloud artifact(s) excluded by privacy=local_only"
    raise NoCapableArtifact(detail)
