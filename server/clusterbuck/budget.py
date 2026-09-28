"""Cloud budget pacing + reserve enforcement (ADR 30; model-evaluation.md → Provider
accounts, budget, and the cost-quality loop; fleet-management.md → Cloud tier: budget).

Before this module the monthly cap was display-only (`usage.py` computed `remaining`/
`over` but nothing read them to gate a routing decision) — CLAUDE.md's hardening-pass note
calls exactly this out as the kind of gap that got fixed elsewhere and should not recur
here. This is the real gate: `routing.resolve_capability` calls it before offering a
cloud candidate.

Two properties, matching the urgency ladder (fleet-management.md → Urgency, escalation &
): `waitable` never reaches this function at all — cloud is a wake-rights
question for it (ADR 18: "no wake, no cloud, no demand"), decided by the caller before any
budget is considered. `necessary` may spend the **paced pool** — the monthly cap minus the
reserve, scaled by how much of the month has elapsed, so week one can't burn the month.
`urgent` may additionally draw the **reserve**, bounded only by the full monthly cap.
"""

from __future__ import annotations

import calendar
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from .store import Store

DEFAULT_RESERVE_FRACTION = 0.2

# Output length assumed for a cloud job that sets no `max_tokens`, for the committed-spend
# estimate. Deliberately on the high side of a typical completion: an estimate that errs
# low lets a burst overshoot the cap, one that errs high only refuses a job a little early
# and is corrected the moment the real figure lands.
EST_OUTPUT_TOKENS = 2000


@dataclass(frozen=True)
class BudgetDecision:
    allowed: bool
    reason: str | None = None


def check_cloud_budget(
    store: Store,
    *,
    monthly_cap: float | None,
    urgency: str,
    reserve_fraction: float = DEFAULT_RESERVE_FRACTION,
    now: float | None = None,
) -> BudgetDecision:
    """Whether a job of this urgency may still spend from the cloud budget right now.

    No cap configured ⇒ unlimited (the pre-existing default: an operator who never set
    CBK_CLOUD_BUDGET_MONTHLY gets today's unrestricted behaviour, not a surprise refusal).
    """
    if monthly_cap is None:
        return BudgetDecision(True)

    now = time.time() if now is None else now
    dt = datetime.fromtimestamp(now, tz=UTC)
    month = dt.strftime("%Y-%m")
    days_in_month = calendar.monthrange(dt.year, dt.month)[1]
    elapsed_days = dt.day  # 1..days_in_month — today already counts as elapsed

    # Committed as well as reported. Spend is only SEEN once a provider answers, so
    # without the in-flight estimate every cloud job admitted in the same minute was
    # checked against the same stale total and a burst could sail past the cap.
    month_spend = store.cloud_spend_in_month(month) + store.committed_cloud_estimate(month)
    paced_pool = monthly_cap * (1.0 - reserve_fraction)
    allowed_by_now = paced_pool * elapsed_days / days_in_month

    if urgency == "urgent":
        # The reserve exists for exactly this case: pacing may already be exhausted, but an
        # urgent job may still spend up to the FULL monthly cap.
        if month_spend >= monthly_cap:
            return BudgetDecision(
                False,
                f"monthly cloud budget (${monthly_cap:g}) exhausted, including the "
                f"urgent reserve",
            )
        return BudgetDecision(True)

    # necessary (the only other urgency that should ever reach this function).
    if month_spend >= allowed_by_now:
        return BudgetDecision(
            False,
            f"paced cloud budget exhausted: ${month_spend:.2f} spent of "
            f"${allowed_by_now:.2f} allowed by day {elapsed_days}/{days_in_month} "
            f"(${monthly_cap:g}/mo, {reserve_fraction:.0%} reserved for urgent jobs)",
        )
    return BudgetDecision(True)


def estimate_cost(fleet, capability: str, job) -> float:
    """What a job queued for a provider is expected to cost, before it has run.

    Input is ~4 characters a token over the prompt text, output is `max_tokens` or
    EST_OUTPUT_TOKENS. Priced by LiteLLM where it knows the model, else the fleet's rate.
    Conservative by construction and replaced by the billed figure once the usage row is
    written — it only has to stop a burst of admissions outrunning the cap.
    """
    spec = fleet.capabilities.get(capability) if fleet else None
    if spec is None:
        return 0.0
    if job.messages:
        chars = sum(len(m.content) for m in job.messages)
    else:
        chars = len(job.prompt or "")
    tin = chars // 4 + 1
    params = job.params or {}
    tout = int(params.get("max_tokens") or EST_OUTPUT_TOKENS)
    model = params.get("model") or spec.model
    try:
        import litellm

        pin, pout = litellm.cost_per_token(model=model, prompt_tokens=tin,
                                           completion_tokens=tout)
        return float(pin + pout)
    except Exception:  # unmapped: the fleet price, which may be 0 — warned at startup
        return tin / 1000 * spec.price_in_per_1k + tout / 1000 * spec.price_out_per_1k
