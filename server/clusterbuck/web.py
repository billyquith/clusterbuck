"""The web dashboard (ADR 21): server-rendered, htmx over the existing data, vendored
assets (no CDN, LAN-only). Two pages — `/` (live usage: headline, queues, reservations)
and `/models` (model configuration: proposals, ability, fleet capabilities, enrolled
nodes) — each loads once and its panels self-refresh with `hx-get` on a short interval.
Panels read app.state directly (same data the JSON APIs serve) rather than self-calling
over HTTP.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .config import settings
from .usage import build_usage_summary
from .wake import heartbeat_age_s

WEB_DIR = Path(__file__).resolve().parent / "web"
templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))

web_routes = APIRouter()


# --- node liveness -------------------------------------------------------------------
#
# The registry knows when a node last spoke (`last_heartbeat`, written on every heartbeat)
# but nothing ever compared that stamp to the clock, so every surface that renders a node
# showed the `mode` it last declared — forever. A box powered off in July still drew an
# `active` pill in September. `heartbeat_age_s` is the missing comparison; the queues
# panel already does the equivalent for stream consumers via `live_worker_consumers`.
#
# It lives in `wake.py` now, not here: the same stamp decides whether to broadcast a
# magic packet at a machine, so it stopped being a presentation concern. Only the
# rendering half — turning an age into "3d" — is still this module's business.


def humanize_age(seconds: float) -> str:
    """Coarse, one-unit age: the reader wants "days, not seconds", not a precise interval."""
    for limit, unit, name in ((60, 1, "s"), (3600, 60, "m"), (86400, 3600, "h")):
        if seconds < limit:
            return f"{int(seconds // unit)}{name}"
    return f"{int(seconds // 86400)}d"


@web_routes.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "dashboard.html")


@web_routes.get("/models", response_class=HTMLResponse)
async def models_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "models.html")


@web_routes.get("/performance", response_class=HTMLResponse)
async def performance_page(request: Request) -> HTMLResponse:
    from .perf_runner import CATEGORIES

    return templates.TemplateResponse(
        request, "performance.html", {"categories": sorted(CATEGORIES)}
    )


@web_routes.get("/ui/headline", response_class=HTMLResponse)
async def ui_headline(request: Request) -> HTMLResponse:
    u = build_usage_summary(request.app.state.store, settings.cloud_budget_monthly)
    return templates.TemplateResponse(request, "partials/headline.html", {"u": u})


@web_routes.get("/ui/connections", response_class=HTMLResponse)
async def ui_connections(request: Request) -> HTMLResponse:
    """Coordinator <-> worker/cloud topology: every enrolled node and every registered
    cloud provider account, each with how many jobs it has actually served (usage_rollup's
    "node" column carries a worker's node_id, or "cloud:<provider>" — cloud_executor.py)."""
    from .cloud_executor import cloud_capabilities, provider_of

    store = request.app.state.store
    fleet = request.app.state.fleet
    jobs_by_key = {r["key"]: r["jobs"] for r in store.usage_rollup("node")}

    now = datetime.now(UTC)
    workers = []
    for n in store.list_nodes():
        # Same correction as the nodes table: a spoke drawn from `mode` alone claims a
        # connection to a machine that may have been off for months.
        age = heartbeat_age_s(n, now=now)
        workers.append({
            "name": n.hostname or n.node_id, "mode": n.mode or "unknown",
            "silent": age is None or age > settings.node_silent_s,
            "age": humanize_age(age) if age is not None else None,
            "jobs": jobs_by_key.get(n.node_id, 0),
        })
    providers = sorted({provider_of(fleet.capabilities[cap].model)
                        for cap in cloud_capabilities(fleet)}) if fleet else []
    cloud = [{"provider": p, "jobs": jobs_by_key.get(f"cloud:{p}", 0)} for p in providers]

    return templates.TemplateResponse(
        request, "partials/connections.html", {"workers": workers, "cloud": cloud}
    )


@web_routes.get("/ui/activity-series")
async def ui_activity_series(request: Request) -> dict:
    """JSON (not HTML — the chart fetches and redraws itself; see dashboard.js) feeding the
    usage page's full-width activity-over-time chart: trailing 30 days, local vs cloud."""
    from .usage import build_activity_series

    days = 30
    rows = request.app.state.store.usage_daily_by_venue(days=days)
    return build_activity_series(rows, days=days)


@web_routes.get("/ui/timeline", response_class=HTMLResponse)
async def ui_timeline(request: Request) -> HTMLResponse:
    """Most recent metered jobs, newest first (Store.recent_usage) — the raw events behind
    the by_day rollup, fine-grained enough to matter at fleet-sized job volumes."""
    rows = request.app.state.store.recent_usage(limit=30)
    return templates.TemplateResponse(request, "partials/timeline.html", {"rows": rows})


@web_routes.get("/ui/queues", response_class=HTMLResponse)
async def ui_queues(request: Request) -> HTMLResponse:
    fleet = request.app.state.fleet
    queue = request.app.state.queue
    rows = []
    for cap in (fleet.capabilities if fleet else {}):
        stats = await queue.depth(cap, settings.consumer_group)
        rows.append({"capability": cap, **stats})
    return templates.TemplateResponse(request, "partials/queues.html", {"rows": rows})


@web_routes.get("/ui/fleet", response_class=HTMLResponse)
async def ui_fleet(request: Request) -> HTMLResponse:
    fleet = request.app.state.fleet
    caps = (
        [{"name": n, "queue": fleet.stream_for(n), "model": s.model,
          "nodes": ", ".join(nd.id for nd in fleet.nodes_for(n)) or "—"}
         for n, s in fleet.capabilities.items()]
        if fleet else []
    )
    nodes = (
        [{"id": n.id, "wake": n.wake, "capabilities": ", ".join(n.capabilities)}
         for n in fleet.nodes]
        if fleet else []
    )
    return templates.TemplateResponse(
        request, "partials/fleet.html", {"caps": caps, "nodes": nodes}
    )


@web_routes.get("/ui/reservations", response_class=HTMLResponse)
async def ui_reservations(request: Request) -> HTMLResponse:
    rows = request.app.state.store.list_reservations(limit=20)
    return templates.TemplateResponse(request, "partials/reservations.html", {"rows": rows})


@web_routes.get("/ui/proposals", response_class=HTMLResponse)
async def ui_proposals(request: Request) -> HTMLResponse:
    store = request.app.state.store
    rows = store.list_proposals("pending")
    # Proposals are keyed by node_id (the stable identity proposals/actions reference),
    # but on the LAN people know machines by hostname — show that instead where known.
    hostnames = {n.node_id: n.hostname for n in store.list_nodes()}
    props = [{"id": r.id, "kind": r.kind, "artifact": r.artifact,
              "node": hostnames.get(r.node_id) or r.node_id, "rationale": r.rationale}
             for r in rows]
    return templates.TemplateResponse(request, "partials/proposals.html", {"props": props})


@web_routes.post("/ui/proposals/{proposal_id}/{decision}", response_class=HTMLResponse)
async def ui_decide(request: Request, proposal_id: str, decision: str) -> HTMLResponse:
    """Approve/deny from the dashboard, then re-render the panel (htmx swap)."""
    store = request.app.state.store
    if decision in {"approve", "deny"} and store.get_proposal(proposal_id) is not None:
        status = "approved" if decision == "approve" else "denied"
        store.decide_proposal(
            proposal_id, status,
            datetime.now(UTC).isoformat().replace("+00:00", "Z"))
    return await ui_proposals(request)


@web_routes.get("/ui/models", response_class=HTMLResponse)
async def ui_models(request: Request) -> HTMLResponse:
    """One row per (artifact, node): the join the models page never made.

    The page used to be four panels that shared no key. Ability listed artifacts with a
    single headline number; Fleet listed tiers and their configured model; Enrolled nodes
    listed hardware and installed models. The artifact appeared in three of them under
    three framings and nothing tied them together, so "is this model good, and fast, here,
    and what can it do" was a manual cross-reference across three tables.

    Three things the API already returned and no template rendered:

      * per-task-class scores. The headline was an equal-weight MEAN, so a model scoring
        9/9/3/3 and one scoring 6/6/6/6 both displayed 6.0 — the difference between a
        specialist and a uniformly mediocre model, erased.
      * provenance and evidence. A `seed` placeholder (a guess shipped in the code) and a
        measurement over forty items rendered identically.
      * what a model can DO. `context_tokens`, tools, JSON schema and vision are curated
        per artifact and were on no page at all.
    """
    import json as _json

    from .evaluation import SCALE_VERSION, TASK_CLASSES
    from .fleet import unservable_capabilities

    store = request.app.state.store
    fleet = request.app.state.fleet

    scores: dict[tuple[str, str], object] = {
        (a.artifact, a.task_class): a for a in store.ability_matrix(SCALE_VERSION)
    }
    catalog = {c.artifact: c for c in store.list_catalog()}
    # Which tiers name this artifact as the model they serve.
    tiers_for: dict[str, list[str]] = {}
    for name, spec in (fleet.capabilities.items() if fleet else {}.items()):
        tiers_for.setdefault(spec.model, []).append(name)

    rows = []
    for n in store.list_nodes():
        installed = _json.loads(n.installed or "[]")
        loaded = set(_json.loads(n.loaded or "[]"))
        caps = _json.loads(n.capabilities or "[]")
        warnings = unservable_capabilities(fleet, advertised=caps, installed=installed)
        for artifact in sorted(installed):
            cells = []
            for tc in TASK_CLASSES:
                a = scores.get((artifact, tc))
                cells.append({
                    "task_class": tc,
                    "score": a.score if a else None,
                    # 'seed' is a guess that shipped in the code; 'measured' came from the
                    # harness. Rendering them alike is how a placeholder passes for evidence.
                    "seed": bool(a and a.provenance == "seed"),
                    "n_items": a.n_items if a else None,
                    "n_passed": a.n_passed if a else None,
                })
            c = catalog.get(artifact)
            rows.append({
                "artifact": artifact,
                "node": n.hostname or n.node_id,
                "node_id": n.node_id,
                "tiers": ", ".join(sorted(tiers_for.get(artifact, []))) or "—",
                "cells": cells,
                "measured_cells": sum(1 for x in cells if x["score"] is not None
                                      and not x["seed"]),
                "tps": n.tps,
                "warm": artifact in loaded,
                "mode": n.mode,
                # Curated facts, not scores: no 1-10 number can answer "does it do vision".
                "size_gb": getattr(c, "size_gb", None),
                "params_b": getattr(c, "params_b", None),
                "quant": getattr(c, "quant", None),
                "context_tokens": getattr(c, "context_tokens", None),
                "tools": getattr(c, "supports_tools", None),
                "json_schema": getattr(c, "supports_json_schema", None),
                "vision": getattr(c, "supports_vision", None),
                "in_catalog": c is not None,
                "warnings": warnings,
            })

    rows.sort(key=lambda r: (r["artifact"], r["node"]))

    # Artifacts the fleet knows about but no node has installed — otherwise a tier whose
    # model is nowhere simply vanishes from a page about models.
    on_a_node = {r["artifact"] for r in rows}
    orphan_tiers = sorted(
        {m for m in tiers_for if m not in on_a_node}
    )

    measuring = sorted(
        {r["artifact"] for r in store.eval_runs_summary() if r["pending"]})
    from .eval_runner import artifacts_needing_eval
    unmeasured = [a for a, _caps in artifacts_needing_eval(store)]

    return templates.TemplateResponse(
        request, "partials/models.html",
        {"rows": rows, "task_classes": TASK_CLASSES, "scale": SCALE_VERSION,
         "measuring": measuring, "unmeasured": unmeasured,
         "orphan_tiers": orphan_tiers})


@web_routes.get("/ui/nodes", response_class=HTMLResponse)
async def ui_nodes(request: Request) -> HTMLResponse:
    import json as _json

    now = datetime.now(UTC)
    nodes = []
    for n in request.app.state.store.list_nodes():
        installed = _json.loads(n.installed or "[]")
        age = heartbeat_age_s(n, now=now)
        nodes.append({
            "id": n.node_id, "host": n.hostname, "mode": n.mode,
            "caps": ", ".join(_json.loads(n.capabilities or "[]")),
            "loaded": ", ".join(_json.loads(n.loaded or "[]")),
            "installed": ", ".join(installed),
            "n_installed": len(installed),
            # Liveness. `mode` is only what the node last CLAIMED; `stale` is the
            # coordinator's own judgement about whether that claim is still worth
            # anything. `seen` stays raw ISO so the template can hand it to the
            # data-utc/.ts localiser like every other timestamp on the dashboard.
            "seen": n.last_heartbeat,
            "never": n.last_heartbeat is None,
            # An unknown age counts as silent, deliberately: `enrolled_at` is written
            # ISO-Z and non-null, so an unreadable stamp means a corrupted or hand-edited
            # row — and failing OPEN there would render exactly those rows as a clean
            # `active` pill, which is the defect this closes.
            "silent": age is None or age > settings.node_silent_s,
            "age": humanize_age(age) if age is not None else None,
            # Version governance (ADR 27): drift and unfitness must be visible, not buried.
            "version": n.agent_version or "unknown",
            "fitness": n.fitness or "unknown",
            "fitness_reason": n.fitness_reason,
            # Hardware probe (ADR 10 self-enrollment): what the node reported about itself.
            "os_arch": f"{n.os}/{n.arch}" if n.os else "—",
            "ram_gb": n.ram_gb,
            "accelerator": n.accelerator,
            "vram_gb": n.vram_gb,
            "load_s": n.load_s,
            "disk_free_gb": n.disk_free_gb,
            "profile": n.profile,
            # Measured, unlike everything above it on this line, which the node asserted
            # about itself at enrollment.
            "tps": n.tps,
        })
    return templates.TemplateResponse(request, "partials/nodes.html", {"nodes": nodes})


@web_routes.get("/ui/perf/runs", response_class=HTMLResponse)
async def ui_perf_runs(request: Request) -> HTMLResponse:
    from .perf_runner import perf_run_list_view

    store = request.app.state.store
    runs = [perf_run_list_view(store, r) for r in store.list_perf_runs()]
    return templates.TemplateResponse(request, "partials/perf_runs.html", {"runs": runs})


@web_routes.get("/ui/perf/runs/{run_id}", response_class=HTMLResponse)
async def ui_perf_run_detail(request: Request, run_id: str) -> HTMLResponse:
    from .perf_runner import perf_run_view

    store = request.app.state.store
    row = store.get_perf_run(run_id)
    if row is None:
        return HTMLResponse("<p class=\"empty\">run not found</p>", status_code=404)
    view = perf_run_view(store, row)
    trouble = [
        {"category": s.category, "outcome": s.outcome, "detail": s.detail}
        for s in store.perf_run_samples(run_id)
        if s.phase == "measure" and s.outcome in ("unassigned", "failed", "timeout")
    ][:20]
    return templates.TemplateResponse(
        request, "partials/perf_run_detail.html", {"run": view, "trouble": trouble},
    )


@web_routes.post("/ui/perf/start", response_class=HTMLResponse)
async def ui_perf_start(request: Request) -> HTMLResponse:
    from .models import PerfRunSubmit
    from .perf_runner import UnknownCategory, start_run

    form = await request.form()
    categories = form.getlist("categories") or None
    kwargs: dict = {"label": form.get("label") or "load test"}
    if categories:
        kwargs["categories"] = categories
    for field, cast in (
        ("concurrency", int), ("duration_s", float), ("warmup_s", float),
        ("n_jobs", int), ("min_ability_override", int),
    ):
        raw = form.get(field)
        if raw:
            kwargs[field] = cast(raw)
    if form.get("pin_model"):
        kwargs["pin_model"] = form.get("pin_model")

    try:
        start_run(request.app, PerfRunSubmit(**kwargs))
    except (UnknownCategory, ValueError):
        pass  # swallow a bad form submission; the re-rendered list is unaffected
    return await ui_perf_runs(request)


@web_routes.post("/ui/perf/runs/{run_id}/cancel", response_class=HTMLResponse)
async def ui_perf_cancel(request: Request, run_id: str) -> HTMLResponse:
    task = request.app.state.perf_tasks.get(run_id)
    if task is not None and not task.done():
        task.cancel()
    return await ui_perf_runs(request)
