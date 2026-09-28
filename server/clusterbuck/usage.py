"""Usage metering (fleet-management.md → Usage accounting; ADR 11).

Metering, not billing: per-job METADATA (tokens, model, node, capability, outcome, cost)
— never prompt or completion text. The headline number is **avoided cloud spend**: local
tokens priced at the cloud rate they would otherwise have paid, minus actual cloud spend.

This module captures the async plane. Sync completions have no job and no result blob, so
`sync.py` writes their rows itself, inline, with the same cost functions (`_job_cost`) and
an id of its own (`sync-…`). Capture here is gated on a usage row not yet existing — not on
job status — so a client's GET flipping status can't skip a job.

`venue` used to be hardcoded `"local"` for every row, which made `cloud_spend_in_month`
structurally always zero — the budget figure was display-only *because nothing could ever
populate it*, not just because no cloud path existed yet (ADR 30). It is now derived from
the capability's own `cloud` flag, the same flag routing already uses for privacy.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta

from .fleet import Fleet
from .queue import Queue
from .store import Store

_log = logging.getLogger("clusterbuck.usage")


def venue_of(fleet: Fleet | None, capability: str | None) -> str:
    """Where a capability's work runs — "local" or "cloud".

    Public because the cancel route records a usage row too, and a cancelled job must be
    attributed to the same venue the metering scan would have given it.
    """
    spec = fleet.capabilities.get(capability) if fleet and capability else None
    return "cloud" if spec is not None and spec.cloud else "local"


def _fleet_cost(fleet: Fleet | None, capability: str | None, tin: int, tout: int) -> float:
    price_in, price_out = fleet.price(capability) if fleet and capability else (0.0, 0.0)
    return tin / 1000 * price_in + tout / 1000 * price_out


# Models already warned about, so an unmapped one says so once per process rather than on
# every job it serves.
_unpriced_warned: set[str] = set()


def litellm_model_id(spec_model: str, completion_model: str | None) -> str:
    """The "<provider>/<model>" id LiteLLM prices by, for the model that actually answered.

    A provider echoes the bare id (`claude-sonnet-5`), not the routing form; the provider
    is taken from the capability's own `model`, which is what the call was made through.
    """
    if not completion_model:
        return spec_model
    if "/" in completion_model or "/" not in spec_model:
        return completion_model
    return f"{spec_model.split('/', 1)[0]}/{completion_model}"


def litellm_can_price(model: str) -> bool:
    """Whether LiteLLM's price map knows this "<provider>/<model>" id."""
    import litellm

    try:
        info = litellm.get_model_info(model)
    except Exception:
        return False
    return bool(info.get("input_cost_per_token") or info.get("output_cost_per_token"))


def list_price_per_1k(spec) -> tuple[float, float]:
    """(input, output) USD per 1k tokens for a tier — for ORDERING work, not billing it.

    The fleet's own price when one is set; otherwise, for a cloud tier, LiteLLM's list
    price for its model. Without the second half a fleet that leaves cloud prices to
    LiteLLM (as it may) would rank every provider account at $0, and routing's cheapest-
    first tie-break among them would quietly become alphabetical.
    """
    if spec.price_in_per_1k or spec.price_out_per_1k or not spec.cloud:
        return spec.price_in_per_1k, spec.price_out_per_1k
    if not litellm_can_price(spec.model):
        return 0.0, 0.0
    import litellm

    info = litellm.get_model_info(spec.model)
    return (float(info.get("input_cost_per_token") or 0) * 1000,
            float(info.get("output_cost_per_token") or 0) * 1000)


def cached_input_tokens(usage: dict) -> int:
    """Prompt tokens served from the provider's cache — OpenAI's `prompt_tokens_details`,
    or Anthropic's `cache_read_input_tokens` as LiteLLM surfaces it."""
    details = usage.get("prompt_tokens_details") or {}
    return int(details.get("cached_tokens") or usage.get("cache_read_input_tokens") or 0)


def cloud_cost(
    fleet: Fleet | None, capability: str | None, completion: dict | None,
    tin: int, tout: int,
) -> tuple[float, str]:
    """(cost, source) for work a provider actually billed.

    LiteLLM's price map first: it knows the model that answered rather than the one the
    capability names, and it prices cache reads and writes, which a flat per-1k rate
    cannot — on a long reused prompt that is most of the bill. The fleet's own
    `price_*_per_1k` is the fallback for a model the map does not know, and says so in
    `source`, because an estimate recorded as if it were a quote is how a budget drifts.
    Neither ⇒ 0 with source `none`, warned rather than silent.
    """
    spec = fleet.capabilities.get(capability) if fleet and capability else None
    model = litellm_model_id(spec.model if spec else "",
                             completion.get("model") if isinstance(completion, dict) else None)
    # Asked first rather than trusting `completion_cost` to raise for an unknown model:
    # building the sync plane's Router registers every deployment in LiteLLM's global
    # price map, and an unmapped one is registered at $0 — after which it is "priced",
    # silently, at nothing. A zero price is not a price.
    if isinstance(completion, dict) and completion.get("usage") and litellm_can_price(model):
        try:
            import litellm

            cost = litellm.completion_cost(
                completion_response=litellm.ModelResponse(**completion), model=model)
            return float(cost), "litellm"
        except Exception:  # unmapped model, or a response shape it cannot price
            pass
    fallback = _fleet_cost(fleet, capability, tin, tout)
    if model not in _unpriced_warned:
        _unpriced_warned.add(model)
        if fallback:
            _log.warning("LiteLLM cannot price %r — using the fleet's price_*_per_1k for "
                         "%s, which ignores cache pricing", model, capability)
        else:
            _log.warning("no price for %r (LiteLLM does not know it and %s sets no "
                         "price_*_per_1k) — its spend is recorded as $0 and does not "
                         "count against the cloud budget", model, capability)
    return fallback, "fleet" if fallback else "none"


def _job_cost(
    fleet: Fleet | None, capability: str | None, tin: int, tout: int, *,
    venue: str = "local", completion: dict | None = None,
) -> tuple[float, str]:
    """local: avoided cost at the fleet's cloud-equivalent rate — a counterfactual, which
    nothing but the operator's own price can give; cloud: what the provider billed
    (`cloud_cost`)."""
    if venue == "cloud":
        return cloud_cost(fleet, capability, completion, tin, tout)
    cost = _fleet_cost(fleet, capability, tin, tout)
    return cost, "fleet" if cost else "none"


async def usage_scan(
    store: Store, queue: Queue, fleet: Fleet | None, *, now: float | None = None,
    group: str | None = None,
) -> int:
    """Capture usage for jobs whose terminal result newly appeared. Returns count captured."""
    now = time.time() if now is None else now
    dt = datetime.fromtimestamp(now, tz=UTC)
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
                # A cancellation we could not prove lands here rather than as an
                # expiry: DELETE /jobs/{id} floors the deadline precisely so this sweep
                # picks it up (see store.request_cancel). Reporting it as `expired`
                # would attribute it to a deadline the client may never have set.
                outcome = "cancelled" if row.cancel_requested else "expired"
                # Take it off the stream first, so a job past its deadline is never
                # handed to a worker afterwards. This sweep used to set the status only,
                # leaving the entry queued: a worker claimed it later, ran it, and the
                # result it wrote flipped `expired` to `done`. Only an UNCLAIMED entry is
                # withdrawn — a worker already generating is left to finish, as the
                # backstop does (backstop._terminalise explains why a plain withdraw
                # would orphan its pending row). Its late answer is then ignored, because
                # GET /jobs treats the status recorded here as final.
                if row.entry_id and row.stream:
                    from .config import settings

                    await queue.withdraw_if_unclaimed(
                        row.stream, row.entry_id, group=group or settings.consumer_group)
                store.set_status(row.id, outcome)
                store.record_usage(
                    job_id=row.id, ts=ts, capability=row.capability, model=None,
                    node=None, venue=venue_of(fleet, row.capability), tokens_in=0, tokens_out=0,
                    outcome=outcome, cost=0.0, day=day,
                )
                captured += 1
            continue

        status = result.get("status", "done")
        usage = result.get("usage") or {}
        tin = int(usage.get("prompt_tokens") or 0)
        tout = int(usage.get("completion_tokens") or 0)
        completion = result.get("completion")
        model = completion.get("model") if isinstance(completion, dict) else None
        venue = venue_of(fleet, row.capability)
        cost, source = _job_cost(fleet, row.capability, tin, tout,
                                 venue=venue, completion=completion)

        store.record_usage(
            job_id=row.id, ts=ts, capability=row.capability, model=model,
            node=result.get("worker"), venue=venue,
            tokens_in=tin, tokens_out=tout, tokens_cached_in=cached_input_tokens(usage),
            outcome=status, cost=cost, cost_source=source, day=day,
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
    month = datetime.fromtimestamp(now, tz=UTC).strftime("%Y-%m")
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


def _trailing_days(days: int, now: float | None) -> list[str]:
    now = time.time() if now is None else now
    today = datetime.fromtimestamp(now, tz=UTC).date()
    return [(today - timedelta(days=days - 1 - i)).isoformat() for i in range(days)]


def build_activity_series(rows, *, days: int, now: float | None = None) -> dict:
    """Zero-filled daily series, local vs cloud, for the usage page's activity-over-time
    chart — `rows` is Store.usage_daily_by_venue's (day, venue, jobs, tokens_in, tokens_out,
    cost) output. Zero-filling (rather than only emitting days with rows) keeps the x-axis a
    continuous trailing window regardless of which days actually saw traffic."""
    day_list = _trailing_days(days, now)
    idx = {d: i for i, d in enumerate(day_list)}

    local_jobs = [0] * days
    cloud_jobs = [0] * days
    # local's cost is the avoided-cloud-spend rate (usage._job_cost's docstring); cloud's is
    # the real provider spend — the same pairing usage_headline already reports.
    avoided_spend = [0.0] * days
    cloud_spend = [0.0] * days

    for r in rows:
        i = idx.get(r["day"])
        if i is None:
            continue
        if r["venue"] == "cloud":
            cloud_jobs[i] += r["jobs"]
            cloud_spend[i] += r["cost"]
        else:
            local_jobs[i] += r["jobs"]
            avoided_spend[i] += r["cost"]

    return {
        "days": day_list,
        "local_jobs": local_jobs,
        "cloud_jobs": cloud_jobs,
        "avoided_spend": [round(x, 6) for x in avoided_spend],
        "cloud_spend": [round(x, 6) for x in cloud_spend],
    }


def build_activity_series_by_node(rows, *, days: int, names: dict[str, str],
                                  order: list[str] = (), now: float | None = None) -> dict:
    """The same trailing window as build_activity_series, one jobs series per node —
    `rows` is Store.usage_daily_by_node's (day, node, jobs). `names` maps node_id to the
    hostname people know the machine by; a `cloud:<provider>` key keeps its own series,
    since it is exactly the work the local fleet did not absorb.

    The chart colours a series by its position, so position must follow the entity, not
    its volume or its name. `order` (the enrolled nodes, oldest first) always gets a
    series, zero-filled if idle — otherwise a worker that went quiet would drop out and
    shift every colour after it. Keys with usage but no longer enrolled come next, and
    cloud last.
    """
    day_list = _trailing_days(days, now)
    idx = {d: i for i, d in enumerate(day_list)}
    per_node: dict[str, list[int]] = {k: [0] * days for k in order}
    for r in rows:
        i = idx.get(r["day"])
        if i is None:
            continue
        key = r["node"] or "unknown"
        per_node.setdefault(key, [0] * days)[i] += r["jobs"]

    # Local workers first, cloud after: the question the view answers is "who did the
    # work", and cloud is the fallback, not a peer.
    rank = {k: i for i, k in enumerate(order)}
    keys = sorted(per_node, key=lambda k: (k.startswith("cloud:"), rank.get(k, len(rank)), k))
    return {
        "days": day_list,
        "series": [{"key": k, "label": names.get(k, k), "cloud": k.startswith("cloud:"),
                    "jobs": per_node[k]} for k in keys],
    }
