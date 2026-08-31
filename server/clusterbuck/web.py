"""The web dashboard (ADR 21): server-rendered, htmx over the existing data, vendored
assets (no CDN, LAN-only). Two pages — `/` (live usage: headline, queues, reservations)
and `/models` (model configuration: proposals, ability, fleet capabilities, enrolled
nodes) — each loads once and its panels self-refresh with `hx-get` on a short interval.
Panels read app.state directly (same data the JSON APIs serve) rather than self-calling
over HTTP.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .config import settings
from .usage import build_usage_summary

WEB_DIR = Path(__file__).resolve().parent / "web"
templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))

web_routes = APIRouter()


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
        [{"name": n, "queue": s.queue, "model": s.model,
          "nodes": ", ".join(nd.id for nd in fleet.nodes_for(n)) or "—"}
         for n, s in fleet.capabilities.items()]
        if fleet else []
    )
    nodes = (
        [{"id": n.id, "wake": n.wake, "capabilities": ", ".join(n.capabilities)} for n in fleet.nodes]
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
    rows = request.app.state.store.list_proposals("pending")
    props = [{"id": r.id, "kind": r.kind, "artifact": r.artifact,
              "node": r.node_id, "rationale": r.rationale} for r in rows]
    return templates.TemplateResponse(request, "partials/proposals.html", {"props": props})


@web_routes.post("/ui/proposals/{proposal_id}/{decision}", response_class=HTMLResponse)
async def ui_decide(request: Request, proposal_id: str, decision: str) -> HTMLResponse:
    """Approve/deny from the dashboard, then re-render the panel (htmx swap)."""
    store = request.app.state.store
    if decision in {"approve", "deny"} and store.get_proposal(proposal_id) is not None:
        status = "approved" if decision == "approve" else "denied"
        store.decide_proposal(
            proposal_id, status,
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    return await ui_proposals(request)


@web_routes.get("/ui/ability", response_class=HTMLResponse)
async def ui_ability(request: Request) -> HTMLResponse:
    from .evaluation import SCALE_VERSION

    rows = request.app.state.store.ability_matrix(SCALE_VERSION)
    by_artifact: dict[str, list[float]] = {}
    for r in rows:
        by_artifact.setdefault(r.artifact, []).append(r.score)
    arts = sorted(
        ({"artifact": a, "headline": round(sum(s) / len(s), 1)} for a, s in by_artifact.items()),
        key=lambda x: x["headline"], reverse=True,
    )
    # In-flight / outstanding measurement, so an unscored model is visibly *being* handled
    # rather than silently absent.
    from .eval_runner import artifacts_needing_eval

    measuring = sorted(
        {r["artifact"] for r in request.app.state.store.eval_runs_summary() if r["pending"]}
    )
    unmeasured = [a for a, _caps in artifacts_needing_eval(request.app.state.store)]
    return templates.TemplateResponse(
        request, "partials/ability.html",
        {"arts": arts, "scale": SCALE_VERSION,
         "measuring": measuring, "unmeasured": unmeasured})


@web_routes.get("/ui/nodes", response_class=HTMLResponse)
async def ui_nodes(request: Request) -> HTMLResponse:
    import json as _json

    nodes = []
    for n in request.app.state.store.list_nodes():
        installed = _json.loads(n.installed or "[]")
        nodes.append({
            "id": n.node_id, "host": n.hostname, "mode": n.mode,
            "caps": ", ".join(_json.loads(n.capabilities or "[]")),
            "loaded": ", ".join(_json.loads(n.loaded or "[]")),
            "installed": ", ".join(installed),
            "n_installed": len(installed),
            "seen": (n.last_heartbeat or "—"),
            # Version governance (ADR 27): drift and unfitness must be visible, not buried.
            "version": n.agent_version or "unknown",
            "fitness": n.fitness or "unknown",
            "fitness_reason": n.fitness_reason,
            # Hardware probe (ADR 10 self-enrollment): what the node reported about itself.
            "os_arch": f"{n.os}/{n.arch}" if n.os else "—",
            "ram_gb": n.ram_gb,
            "accelerator": n.accelerator,
            "vram_gb": n.vram_gb,
            "disk_free_gb": n.disk_free_gb,
            "profile": n.profile,
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
