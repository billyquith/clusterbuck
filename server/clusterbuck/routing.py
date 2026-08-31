"""Need-shaped routing (ADR 16 / model-evaluation.md): turn {task_class, min_ability,
privacy} into a concrete capability using the measured ability matrix.

Selection: filter artifacts by `ability(artifact, task_class) ≥ min_ability`, by the job's
privacy class, and — for a cloud candidate — by urgency's wake rights and the cloud budget
(ADR 30) → prefer local → cheapest. Explicit `capability` addressing still wins for power
users, but is no longer a free pass: fleet-management.md requires the same privacy and
budget invariants there too ("submission validation rejects incoherent combos... fast, at
the API"), so a caller naming a cloud capability directly is checked exactly as a
need-shaped one would be.

If nothing clears the bar the request **fails explicitly** (`NoCapableArtifact` → 422) rather
than quietly running on a weaker model. Silently under-serving defeats the whole point of
need-shaped addressing: the client stated a floor and would have no way to learn it was missed.
Anchored seed scores (`provenance='seed'`) keep this workable before anything is measured.
"""

from __future__ import annotations

from .budget import BudgetDecision, check_cloud_budget
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


def _cloud_gate(
    spec_cloud: bool, *, privacy: str, urgency: str, budget: BudgetDecision,
) -> str | None:
    """None if a cloud candidate with this spec/urgency/budget may be used; else why not.

    Non-cloud specs always pass (returns None) — this only ever excludes a `cloud: true`
    capability, never a local one.
    """
    if not spec_cloud:
        return None
    if privacy == "local_only":
        return "privacy=local_only"  # ADR 14: never leaves the LAN, whatever its ability
    if urgency == "waitable":
        # ADR 18: waitable never creates capacity for itself — no wake, no cloud, no demand.
        # This is a wake-rights question, not a budget one, so it is checked first.
        return "urgency=waitable never uses cloud"
    if not budget.allowed:
        return budget.reason
    return None


def resolve_capability(
    fleet: Fleet | None,
    store: Store,
    *,
    capability: str | None,
    task_class: str | None,
    min_ability: int | None,
    privacy: str = "local_only",
    urgency: str = "waitable",
    cloud_budget_monthly: float | None = None,
    cloud_budget_reserve_fraction: float = 0.2,
    now: float | None = None,
    scale_version: str = SCALE_VERSION,
) -> str:
    budget = check_cloud_budget(
        store, monthly_cap=cloud_budget_monthly, urgency=urgency,
        reserve_fraction=cloud_budget_reserve_fraction, now=now,
    )

    if capability is not None:
        spec = fleet.capabilities.get(capability) if fleet else None
        if spec is not None:
            excluded = _cloud_gate(spec.cloud, privacy=privacy, urgency=urgency, budget=budget)
            if excluded is not None:
                raise NoCapableArtifact(
                    f"capability {capability!r} is cloud-backed and excluded: {excluded}"
                )
        return capability

    if fleet is None or task_class is None or min_ability is None:
        raise NoCapableArtifact(
            "need-shaped routing requires a fleet registry plus task_class and min_ability"
        )

    # Candidates whose artifact clears the ability bar for this task class, within privacy,
    # urgency's wake rights, and (for a cloud candidate) budget.
    candidates: list[tuple[bool, float, str]] = []  # (is_cloud, price, capability)
    excluded_by_privacy = 0
    excluded_by_budget = 0
    best_available: float | None = None
    for cap, spec in fleet.capabilities.items():
        excluded = _cloud_gate(spec.cloud, privacy=privacy, urgency=urgency, budget=budget)
        if excluded is not None:
            if excluded == "privacy=local_only":
                excluded_by_privacy += 1
            elif excluded != "urgency=waitable never uses cloud":
                excluded_by_budget += 1
            continue
        score = store.get_ability(spec.model, task_class, scale_version)
        if score is None:
            continue
        best_available = score if best_available is None else max(best_available, score)
        if score >= min_ability:
            candidates.append((spec.cloud, spec.price_in_per_1k + spec.price_out_per_1k, cap))

    if candidates:
        candidates.sort()  # prefer local (False < True) → cheapest
        return candidates[0][2]

    detail = (
        f"no artifact reaches ability {min_ability} for task_class {task_class!r}"
        f" (best available: {best_available if best_available is not None else 'none measured'})"
    )
    if excluded_by_privacy:
        detail += f"; {excluded_by_privacy} cloud artifact(s) excluded by privacy=local_only"
    if excluded_by_budget:
        detail += f"; {excluded_by_budget} cloud artifact(s) excluded by budget ({budget.reason})"
    raise NoCapableArtifact(detail)
