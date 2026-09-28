"""Need-shaped routing (ADR 16 / model-evaluation.md): turn {task_class, min_ability,
privacy} into a concrete capability using the measured ability matrix.

Selection: filter artifacts by `ability(artifact, task_class) ≥ min_ability`, by the job's
privacy class, and — for a cloud candidate — by urgency's wake rights and the cloud budget
(ADR 30) → prefer local → soonest answer → cheapest.

"Soonest answer" is `tier_eta`: the work already open on a tier, over the combined decode
speed of the machines serving it right now, plus a cold load if none has the model warm.
It used to be cheapest-first alone, which on a fleet whose prices are cloud-equivalent
rates means *smallest model first*: every need-shaped job went to the slowest tier that
cleared the bar, and a machine five times faster sat idle unless a client named its tier.
Price still breaks ties, and it still decides everything when no liveness is known — a
tier nobody is serving has no estimate, so with no heartbeats the old order is exactly
what comes back. Explicit `capability` addressing still wins for power
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

import math
from dataclasses import dataclass
from datetime import UTC, datetime

from .budget import BudgetDecision, check_cloud_budget
from .evaluation import SCALE_VERSION, TIER1_MAX_ABILITY
from .fleet import Fleet, resolve_api_key
from .store import Store
from .usage import list_price_per_1k

# Output length assumed for a job whose length is unknown, to turn tokens/sec into seconds.
# Only the RATIO between tiers matters for the ranking; this sets how a cold load weighs
# against decode time. Roughly the mean real completion on the fleet this was tuned on.
TYPICAL_OUTPUT_TOKENS = 700

# Decode speed assumed for a live node that has not reported one yet — a fresh node has
# served nothing. Deliberately modest, so an unmeasured machine never outranks a measured
# fast one on a guess, but a live one still beats a tier with no machine at all.
DEFAULT_TPS = 10.0

# Cold-load cost assumed when no live node has the model resident and none has reported
# how long its loads take.
DEFAULT_LOAD_S = 30.0


@dataclass(frozen=True)
class TierEta:
    seconds: float          # math.inf when no machine is serving the tier right now
    live_nodes: int
    open_jobs: int
    warm: bool


def tier_eta(fleet: Fleet, store: Store, *, now: datetime | None = None,
             silent_s: float | None = None) -> dict[str, TierEta]:
    """Seconds until a new job on each local tier would plausibly be answered.

    Liveness is `wake.nodes_serving`: a recent heartbeat AND the tier among the queues the
    node is reading now. The second half is what makes the presence ladder and `pause`
    count — a shared machine whose owner is at it stops reading its heavy tiers, and
    routing stops preferring them the same moment, rather than sending work to a queue
    only a sleeping ladder rung would drain.

    Deliberately crude: one queue, machines draining it in parallel at their reported
    decode speed. It only has to order tiers, not promise a time.
    """
    import json

    from .config import settings
    from .wake import nodes_serving

    now = now or datetime.now(UTC)
    silent_s = settings.node_silent_s if silent_s is None else silent_s
    nodes = {n.node_id: n for n in store.list_nodes()}
    open_jobs = store.open_jobs_by_capability()
    out: dict[str, TierEta] = {}
    for cap, spec in fleet.capabilities.items():
        if spec.cloud:
            continue
        live = [nodes[i] for i in nodes_serving(nodes.values(), cap, silent_s=silent_s,
                                                now=now)]
        ahead = open_jobs.get(cap, 0)
        if not live:
            out[cap] = TierEta(math.inf, 0, ahead, False)
            continue
        capacity = sum((n.tps or DEFAULT_TPS) for n in live)
        warm = any(spec.model in json.loads(n.loaded or "[]") for n in live)
        load = 0.0 if warm else min((n.load_s or DEFAULT_LOAD_S) for n in live)
        seconds = (ahead + 1) * TYPICAL_OUTPUT_TOKENS / capacity + load
        out[cap] = TierEta(seconds, len(live), ahead, warm)
    return out


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
    # Estimated seconds to an answer on the chosen tier, when one was known. Carried for
    # the log line: "why this tier" is otherwise invisible once speed decides it.
    eta_s: float | None = None
    # Where this job may go if the local tier chosen never serves it (rescue.py): a cloud
    # capability and the artifact on it that cleared the SAME bar, within the job's
    # privacy. Decided now, not at rescue time, so the rescue cannot land on something
    # the client's floor would have refused. Only privacy is applied to it here: urgency
    # and budget are re-checked when it is used, because both can change before then (an
    # escalated waitable gains wake rights; a budget refills on the 1st).
    cloud_alternate: str | None = None
    cloud_alternate_model: str | None = None
    # The local tier this job was meant for, when routing sent it to the cloud because no
    # machine could serve that tier (design.md §8, unavailability). None otherwise.
    fell_back_from: str | None = None

    @property
    def on_a_guess(self) -> bool:
        """True when the bar was cleared by a seeded placeholder, not a measurement."""
        return self.provenance == "seed"


class RoutingRefusal(Exception):
    """A submit routing cannot serve, carrying the stable code a client branches on.

    The message names the fix for a person; `code` (errors.CODES) is what a client acts
    on, and it has to be set here, at the raise site that knows WHICH refusal this is —
    by the time the HTTP edge sees the exception, only the text is left to tell them apart.
    """

    code = "ability_unsatisfied"

    def __init__(self, message: str, *, code: str | None = None,
                 reason: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code
        self.reason = reason


class NoCapableArtifact(RoutingRefusal):
    """No artifact clears the requested ability bar within the job's privacy class.

    model-evaluation.md is explicit: a `min_ability` no local artifact can meet routes to
    cloud (if `cloud_ok`) or **fails explicitly** (if `local_only`). Quietly serving the job
    on a weaker model defeats need-shaped addressing — the client asked for a floor and would
    have no way to know it was not met.
    """


def can_call(spec) -> bool:
    """False for a registered provider account whose key is not set.

    The sync plane already leaves such an account out of its router; the async side
    has to refuse it as a TARGET too. Otherwise an operator who copied a fleet file
    without the key has jobs routed, fallen back or rescued onto an account the executor
    can only fail — which turns "a machine may still wake" into a certain failure.
    """
    return not (spec.cloud and spec.model_server is None) or bool(resolve_api_key(spec))


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


# Requirement name → the attribute declaring it, on a catalog row or a capability spec.
# (This mapping is all the dangling comment here ever had behind it; ADR 37's filter was
# specified, its storage was migrated, and the gate itself was never written.)
_FEATURE_ATTR = {
    "context_tokens": "context_tokens",
    "tools": "supports_tools",
    "json_schema": "supports_json_schema",
    "vision": "supports_vision",
}


class MissingCapability(RoutingRefusal):
    """No artifact DECLARES a required capability (ADR 37).

    Separate from `NoCapableArtifact` because the two call for completely different
    fixes: this one usually means the catalog does not describe a model that can in fact
    do the thing, and the answer is one `POST /catalog` away — not a better model.
    """

    code = "requirements_unsatisfied"


def _declared(artifact: str, spec, catalog: dict) -> dict:
    """What an artifact is declared to be able to do.

    Two sources, because they hold two different populations (design.md §6's "three
    registries"): a LOCAL artifact's features live in the model catalog, while a provider
    account has no host node, never enters that catalog, and declares them on its
    `fleet.yaml` capability instead. Catalog first — it is the per-artifact record, and a
    capability is only a queue name.
    """
    row = catalog.get(artifact)
    out = {}
    for name, attr in _FEATURE_ATTR.items():
        value = getattr(row, attr, None) if row is not None else None
        if value is None:
            value = getattr(spec, attr, None)
        out[name] = value
    return out


# The public name for what `/fleet` shows: the same lookup the filter runs, so what a
# client reads and what routing enforces cannot disagree.
declared_features = _declared


def _unmet(asked: dict, declared: dict) -> str | None:
    """Which requirement this artifact fails, or None if it meets them all.

    **Undeclared reads as "no"**, and that is the opposite of how speed is treated — on
    purpose. An unmeasured *speed* costs at most a slower answer and self-corrects as the
    node reports what it measured; nothing refuses a job for it. An undeclared *feature* has
    no such backstop: serving a tool-calling job on a model nobody has checked either
    fails at the model server, where it reads as a model bug rather than a routing one,
    or succeeds while quietly ignoring the tools.
    """
    for name, wanted in asked.items():
        have = declared.get(name)
        if name == "context_tokens":
            if have is None:
                return f"context window undeclared (needs >= {wanted})"
            if have < wanted:
                return f"context window {have} < {wanted}"
        elif not have:
            return f"{name} not declared" if have is None else f"{name} not supported"
    return None


def resolve(
    fleet: Fleet | None,
    store: Store,
    *,
    capability: str | None,
    task_class: str | None,
    min_ability: int | None,
    requires: dict | None = None,
    privacy: str = "local_only",
    urgency: str = "waitable",
    cloud_budget_monthly: float | None = None,
    cloud_budget_reserve_fraction: float = 0.2,
    now: float | None = None,
    scale_version: str = SCALE_VERSION,
    etas: dict[str, TierEta] | None = None,
    unavailable: frozenset[str] | set[str] = frozenset(),
) -> Selection:
    """The capability to enqueue on AND the artifact that must run the job.

    `unavailable` is the local tiers no machine can serve right now: none is reading the
    queue and none can be woken for it. A `cloud_ok` job whose every bar-clearing local
    tier is in it goes to the cloud (design.md §8). That is an AVAILABILITY rule and
    deliberately not a speed one — a tier that is up but slow still wins over the cloud
    (§12), because that case is overflow and overflow is not built. The caller computes
    the set because liveness is read from Redis, which this function does not touch.
    """
    budget = check_cloud_budget(
        store, monthly_cap=cloud_budget_monthly, urgency=urgency,
        reserve_fraction=cloud_budget_reserve_fraction, now=now,
    )
    # ADR 37's filter applies to BOTH addressing forms. Explicit `capability` addressing
    # is the advanced form, not a bypass — the same reason privacy and budget are checked
    # there too: a caller who names a tier still gets told it cannot do what they asked
    # for, rather than discovering it in a truncated answer.
    asked = dict(requires or {})
    catalog = {c.artifact: c for c in store.list_catalog()} if asked else {}

    if capability is not None:
        if fleet is None:
            raise NoCapableArtifact(
                f"capability {capability!r} requested but no fleet registry is configured",
                code="fleet_not_configured",
            )
        if capability not in fleet.capabilities:
            detail = f"unknown capability {capability!r}"
            available = sorted(fleet.capabilities)
            detail += (
                f" (available: {', '.join(available)})"
                if available
                else " (fleet registry has no capabilities registered)"
            )
            raise NoCapableArtifact(detail, code="capability_not_found")
        spec = fleet.capabilities[capability]
        excluded = _cloud_gate(spec.cloud, privacy=privacy, urgency=urgency, budget=budget)
        if excluded is not None:
            # Budget apart from privacy and urgency: those are the job's own terms and
            # will not change on a retry; a budget is spent down and refilled.
            is_policy = excluded in ("privacy=local_only", "urgency=waitable never uses cloud")
            raise NoCapableArtifact(
                f"capability {capability!r} is cloud-backed and excluded: {excluded}",
                code="cloud_not_permitted" if is_policy else "cloud_budget_exhausted",
                reason=excluded.split("=")[0] if is_policy else "budget",
            )
        if asked:
            why = _unmet(asked, _declared(spec.model, spec, catalog))
            if why is not None:
                raise MissingCapability(
                    f"capability {capability!r} runs {spec.model!r}, which does not meet "
                    f"the job's `requires`: {why}"
                )
        # A tier named explicitly has no ability bar to find a cloud equivalent through,
        # so its cloud alternative is whatever the operator declared as its
        # `cloud_fallback` — held to the job's privacy and `requires` like any other.
        alt = None
        fb = spec.cloud_fallback if not spec.cloud else None
        if fb is not None and privacy != "local_only" and can_call(fleet.capabilities[fb]):
            fb_spec = fleet.capabilities[fb]
            if not (asked and _unmet(asked, _declared(fb_spec.model, fb_spec, catalog))):
                alt = fb
        if alt is not None and capability in unavailable and _cloud_gate(
                True, privacy=privacy, urgency=urgency, budget=budget) is None:
            return Selection(capability=alt, artifact=fleet.capabilities[alt].model,
                             cloud=True, fell_back_from=capability)
        # Explicit addressing names a tier, and the tier's registered model is the artifact
        # it means. Pinning it here is what stops the tier's meaning being decided by
        # whatever CBK_MODEL happens to say on the node that claims the job.
        return Selection(capability=capability, artifact=spec.model, cloud=spec.cloud,
                         cloud_alternate=alt,
                         cloud_alternate_model=fleet.capabilities[alt].model if alt else None)

    if fleet is None:
        raise NoCapableArtifact(
            "need-shaped routing requires a fleet registry, and none is configured",
            code="fleet_not_configured",
        )
    if task_class is None or min_ability is None:
        raise NoCapableArtifact(
            "need-shaped routing requires both task_class and min_ability",
            code="invalid_request",
        )

    # Candidates whose artifact clears the ability bar for this task class, within privacy,
    # urgency's wake rights, and (for a cloud candidate) budget.
    etas = tier_eta(fleet, store) if etas is None else etas
    # (is_cloud, is_unavailable, eta, price, capability, artifact, score, provenance)
    candidates: list[tuple[bool, bool, float, float, str, str, float, str | None]] = []
    excluded_by_privacy = 0
    excluded_by_budget = 0
    unmet: dict[str, str] = {}
    best_available: float | None = None
    # Cloud artifacts that clear the bar but are held back by urgency or budget. Not
    # candidates now, but still a rescue target: privacy is the job's own and permanent,
    # the other two are re-checked when a rescue is actually attempted.
    held_back: list[tuple[float, str, str, float]] = []
    for cap, spec in fleet.capabilities.items():
        if not can_call(spec):
            continue  # an account with no key serves nothing; warned at startup (sync.py)
        excluded = _cloud_gate(spec.cloud, privacy=privacy, urgency=urgency, budget=budget)
        if excluded == "privacy=local_only":
            excluded_by_privacy += 1
            continue
        if excluded is not None:
            if excluded != "urgency=waitable never uses cloud":
                excluded_by_budget += 1
            # Out of the running now, so it must not colour the refusal below (its score
            # is not "available", its features are not why nothing matched) — only
            # remembered as a rescue target if it would clear the bar.
            if not (asked and _unmet(asked, _declared(spec.model, spec, catalog))):
                score = store.get_ability(spec.model, task_class, scale_version)
                if score is not None and score >= min_ability:
                    held_back.append((sum(list_price_per_1k(spec)),
                                      cap, spec.model, score))
            continue
        # BEFORE ability is compared (ADR 37), not after. The other way round, a
        # capable-but-unsuitable artifact wins on score and the requirement is decided by
        # an unrelated number — the job would be routed to the best summariser in the
        # fleet regardless of whether it can hold the document.
        if asked:
            why = _unmet(asked, _declared(spec.model, spec, catalog))
            if why is not None:
                unmet[spec.model] = why
                continue
        score = store.get_ability(spec.model, task_class, scale_version)
        if score is None:
            continue
        best_available = score if best_available is None else max(best_available, score)
        if score < min_ability:
            continue
        eta = etas[cap].seconds if cap in etas else math.inf
        candidates.append((
            spec.cloud, cap in unavailable, eta, sum(list_price_per_1k(spec)), cap,
            spec.model, score,
            store.ability_provenance(spec.model, task_class, scale_version),
        ))

    if candidates:
        # Local before cloud (False < True) — cost and privacy posture, unchanged. Then a
        # tier that can be served (awake, or wakeable) before one that cannot: both have
        # no ETA, and only one of them has a way to be answered. Then the soonest answer.
        # Then cheapest, which is also the whole order when no tier has a live machine.
        candidates.sort()
        local = [c for c in candidates if not c[0]]
        cloud_ok_now = [c for c in candidates if c[0]]
        pick = candidates[0]
        fell_back_from = None
        if local and cloud_ok_now and all(c[1] for c in local):
            # Every local tier that could run this has nobody to run it and nobody to
            # wake. Waiting would be waiting for a machine to turn up of its own accord,
            # which a job with cloud permission and wake rights did not ask to do.
            pick, fell_back_from = cloud_ok_now[0], local[0][4]
        cloud, _unavailable, eta, _price, cap, artifact, score, provenance = pick
        alt = None
        if not cloud:
            # Cheapest cloud artifact that cleared the bar: a rescue is a stand-in for the
            # local tier, not an upgrade on it. Prices are equal-weighted in and out.
            options = sorted([(c[3], c[4], c[5]) for c in cloud_ok_now]
                             + [(h[0], h[1], h[2]) for h in held_back])
            alt = options[0] if options else None
        return Selection(capability=cap, artifact=artifact, cloud=cloud,
                         score=score, provenance=provenance,
                         eta_s=None if math.isinf(eta) else eta,
                         cloud_alternate=alt[1] if alt else None,
                         cloud_alternate_model=alt[2] if alt else None,
                         fell_back_from=fell_back_from)

    if asked and not candidates and unmet and best_available is None:
        # Everything that could have run this was filtered on a declared capability, so
        # the ability bar was never the problem and reporting it as one would send the
        # operator looking for a better model. Name the artifact and the missing feature:
        # the fix is usually one `POST /catalog` away, because the model can very often
        # do the thing and simply has not been recorded as able to.
        listed = "; ".join(f"{a}: {why}" for a, why in sorted(unmet.items()))
        raise MissingCapability(
            f"no artifact declares what this job requires ({_describe(asked)}) — {listed}"
        )

    detail = (
        f"no artifact reaches ability {min_ability} for task_class {task_class!r}"
        f" (best available: "
        f"{best_available if best_available is not None else 'none measured'})"
    )
    if unmet:
        detail += (
            f"; {len(unmet)} artifact(s) excluded by `requires` "
            f"({_describe(asked)})"
        )
    if excluded_by_privacy:
        detail += f"; {excluded_by_privacy} cloud artifact(s) excluded by privacy=local_only"
    if excluded_by_budget:
        detail += (f"; {excluded_by_budget} cloud artifact(s) excluded by budget "
                   f"({budget.reason})")
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


def _describe(asked: dict) -> str:
    """The requirement set, for an error a human has to act on."""
    return ", ".join(
        f"context_tokens>={v}" if k == "context_tokens" else k
        for k, v in sorted(asked.items())
    )


def resolve_capability(*args, **kwargs) -> str:
    """Just the capability. Callers that also need the artifact use `resolve` directly."""
    return resolve(*args, **kwargs).capability
