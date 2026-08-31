"""Usage metering (fleet-management.md → Usage accounting; ADR 11).

Metering, not billing: per-job METADATA (tokens, model, node, capability, outcome, cost)
— never prompt or completion text. The headline number is **avoided cloud spend**: local
tokens priced at the cloud rate they would otherwise have paid, minus actual cloud spend.

M3a is async-only: sync completions go straight through LiteLLM to the client (no job, no
result blob) and aren't captured here (ADR 30 keeps that asymmetry rather than gating an
unmetered path). Capture is gated on a usage row not yet existing — not on job status — so
a client's GET flipping status can't skip a job.

`venue` used to be hardcoded `"local"` for every row, which made `cloud_spend_in_month`
structurally always zero — the budget figure was display-only *because nothing could ever
populate it*, not just because no cloud path existed yet (ADR 30). It is now derived from
the capability's own `cloud` flag, the same flag routing already uses for privacy.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from .fleet import Fleet
from .queue import Queue
from .store import Store

_log = logging.getLogger("clusterbuck.usage")


def _venue(fleet: Fleet | None, capability: str | None) -> str:
    spec = fleet.capabilities.get(capability) if fleet and capability else None
    return "cloud" if spec is not None and spec.cloud else "local"


def _job_cost(fleet: Fleet | None, capability: str, tin: int, tout: int) -> float:
    """local: avoided cost at the cloud-equivalent rate; cloud: the actual provider cost —
    both are `price_*_per_1k` on the same CapabilitySpec, so the formula is identical."""
    price_in, price_out = fleet.price(capability) if fleet else (0.0, 0.0)
    return tin / 1000 * price_in + tout / 1000 * price_out


async def usage_scan(
    store: Store, queue: Queue, fleet: Fleet | None, *, now: float | None = None
) -> int:
    """Capture usage for jobs whose terminal result newly appeared. Returns count captured."""
    now = time.time() if now is None else now
    dt = datetime.fromtimestamp(now, tz=timezone.utc)
    ts = dt.isoformat().replace("+00:00", "Z")
    day = dt.strftime("%Y-%m-%d")

    captured = 0
    for row in store.jobs_awaiting_usage():
        result = await queue.read_result(row.result_key)
        if result is None:
            # Expire a past-deadline job so it doesn't linger uncaptured (result may have
            # TTL'd away before we saw it). Jobs without a deadline are left as-is.
            dl = row.deadline_epoch
            if dl is not None and now > dl:
                store.set_status(row.id, "expired")
                store.record_usage(
                    job_id=row.id, ts=ts, capability=row.capability, model=None,
                    node=None, venue=_venue(fleet, row.capability), tokens_in=0, tokens_out=0,
                    outcome="expired", cost=0.0, day=day,
                )
                captured += 1
            continue

        status = result.get("status", "done")
        usage = result.get("usage") or {}
        tin = int(usage.get("prompt_tokens") or 0)
        tout = int(usage.get("completion_tokens") or 0)
        completion = result.get("completion")
        model = completion.get("model") if isinstance(completion, dict) else None

        store.record_usage(
            job_id=row.id, ts=ts, capability=row.capability, model=model,
            node=result.get("worker"), venue=_venue(fleet, row.capability),
            tokens_in=tin, tokens_out=tout,
            outcome=status, cost=_job_cost(fleet, row.capability, tin, tout), day=day,
        )
        store.set_status(row.id, status)  # so an unpolled completed job shows terminal
        captured += 1

    return captured


def build_usage_summary(
    store: Store, budget_monthly: float | None, *, now: float | None = None
) -> dict:
    """Assemble the /usage response: headline, budget burn, totals, and rollups."""
    now = time.time() if now is None else now
    h = store.usage_headline()
    local_cost, cloud_cost = float(h["local_cost"]), float(h["cloud_cost"])
    month = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m")
    cloud_month = store.cloud_spend_in_month(month)

    return {
        "headline": {
            "avoided_cloud_spend": round(local_cost, 6),
            "cloud_spend": round(cloud_cost, 6),
            "net_avoided": round(local_cost - cloud_cost, 6),
            "currency": "USD",
        },
        "budget": {
            "monthly_cap": budget_monthly,
            "cloud_spent_this_month": round(cloud_month, 6),
            "remaining": (round(budget_monthly - cloud_month, 6)
                          if budget_monthly is not None else None),
            "over": budget_monthly is not None and cloud_month > budget_monthly,
            # Real gating lives in budget.check_cloud_budget, applied at routing time
            # (routing.py) — a configured cap is now actually enforced, not just shown.
            "enforced": budget_monthly is not None,
        },
        "totals": {
            "jobs": h["jobs"], "tokens_in": h["tokens_in"], "tokens_out": h["tokens_out"],
        },
        "by_model": [dict(r) for r in store.usage_rollup("model")],
        "by_node": [dict(r) for r in store.usage_rollup("node")],
        "by_day": [dict(r) for r in store.usage_rollup("day")],
    }
