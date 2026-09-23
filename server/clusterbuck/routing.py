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

**The result names an artifact, not just a capability.** Selection is made on an artifact's
measured ability, but a capability is only a queue name: the worker that drains it answers
with whatever `CBK_MODEL` says, for every capability it serves. Returning the artifact lets
the submit path pin it on the job (`params.model`) exactly as the eval harness already does,
so the model that clears the bar is the model that runs. Without the pin, `min_ability` was
enforced against a name in `fleet.yaml` that nothing reconciled with reality.

The provenance travels with it for the same reason: a job served on a `seed` placeholder has
not been served on a measurement, and only the caller can decide whether that matters.
"""

from __future__ import annotations

from dataclasses import dataclass

from .budget import BudgetDecision, check_cloud_budget
from .evaluation import SCALE_VERSION, TIER1_MAX_ABILITY
from .fleet import Fleet
from .store import Store


@dataclass(frozen=True)
class Selection:
    """What routing chose, and on what evidence.

    `artifact` is the model that must actually run the job — pinned onto it downstream so
    the worker cannot substitute its own. `score`/`provenance` are None for explicit
    `capability` addressing, which names a supply-side tier rather than clearing a bar.
    """

    capability: str
    artifact: str
    cloud: bool
    score: float | None = None
    provenance: str | None = None

    @property
    def on_a_guess(self) -> bool:
        """True when the bar was cleared by a seeded placeholder, not a measurement."""
        return self.provenance == "seed"


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


# Requirement name → the attribute holding it, on a catalog row or a capability spec.
def resolve(
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
) -> Selection:
    """The capability to enqueue on AND the artifact that must run the job."""
    budget = check_cloud_budget(
        store, monthly_cap=cloud_budget_monthly, urgency=urgency,
        reserve_fraction=cloud_budget_reserve_fraction, now=now,
    )

    if capability is not None:
        if fleet is None:
            raise NoCapableArtifact(
                f"capability {capability!r} requested but no fleet registry is configured"
            )
        if capability not in fleet.capabilities:
            detail = f"unknown capability {capability!r}"
            available = sorted(fleet.capabilities)
            detail += (
                f" (available: {', '.join(available)})"
                if available
                else " (fleet registry has no capabilities registered)"
            )
            raise NoCapableArtifact(detail)
        spec = fleet.capabilities[capability]
        excluded = _cloud_gate(spec.cloud, privacy=privacy, urgency=urgency, budget=budget)
        if excluded is not None:
            raise NoCapableArtifact(
                f"capability {capability!r} is cloud-backed and excluded: {excluded}"
            )
        # Explicit addressing names a tier, and the tier's registered model is the artifact
        # it means. Pinning it here is what stops the tier's meaning being decided by
        # whatever CBK_MODEL happens to say on the node that claims the job.
        return Selection(capability=capability, artifact=spec.model, cloud=spec.cloud)

    if fleet is None or task_class is None or min_ability is None:
        raise NoCapableArtifact(
            "need-shaped routing requires a fleet registry plus task_class and min_ability"
        )

    # Candidates whose artifact clears the ability bar for this task class, within privacy,
    # urgency's wake rights, and (for a cloud candidate) budget.
    # (is_cloud, price, capability, artifact, score, provenance)
    candidates: list[tuple[bool, float, str, str, float, str | None, float | None]] = []
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
        if score < min_ability:
            continue
        candidates.append((
            spec.cloud, spec.price_in_per_1k + spec.price_out_per_1k, cap,
            spec.model, score,
            store.ability_provenance(spec.model, task_class, scale_version),
        ))

    if candidates:
        candidates.sort()  # prefer local (False < True) → cheapest
        cloud, _price, cap, artifact, score, provenance = candidates[0]
        return Selection(capability=cap, artifact=artifact, cloud=cloud,
                         score=score, provenance=provenance)

    detail = (
        f"no artifact reaches ability {min_ability} for task_class {task_class!r}"
        f" (best available: {best_available if best_available is not None else 'none measured'})"
    )
    if excluded_by_privacy:
        detail += f"; {excluded_by_privacy} cloud artifact(s) excluded by privacy=local_only"
    if excluded_by_budget:
        detail += f"; {excluded_by_budget} cloud artifact(s) excluded by budget ({budget.reason})"
    if min_ability > TIER1_MAX_ABILITY:
        # Without this the caller is told "no artifact reaches ability 9" and goes looking
        # for a better model — but no model, local or cloud, can hold a 9 today. The
        # tier-1 suite is a compliance instrument and is clamped to TIER1_MAX_ABILITY on
        # every write; the 8-10 band is reserved for the judged tiers, which are not
        # built. Naming the ceiling turns an impossible request into a legible one.
        detail += (
            f"; note {min_ability} is above the measurable ceiling of "
            f"{TIER1_MAX_ABILITY:g} — tier-1 scoring cannot certify the 8-10 band, so no "
            f"artifact can clear this bar, cloud included"
        )
    raise NoCapableArtifact(detail)


def resolve_capability(*args, **kwargs) -> str:
    """Just the capability. Callers that also need the artifact use `resolve` directly."""
    return resolve(*args, **kwargs).capability
