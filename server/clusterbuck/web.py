"""The web dashboard (ADR 21): server-rendered, htmx over the existing data, vendored
assets (no CDN, LAN-only). The full page loads once; each panel refreshes itself with
`hx-get` on a short interval. Panels read app.state directly (same data the JSON APIs
serve) rather than self-calling over HTTP.
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
        [{"name": n, "queue": s.queue, "model": s.model} for n, s in fleet.capabilities.items()]
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
    props = [{"id": r["id"], "kind": r["kind"], "artifact": r["artifact"],
              "node": r["node_id"], "rationale": r["rationale"]} for r in rows]
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
        by_artifact.setdefault(r["artifact"], []).append(r["score"])
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
        installed = _json.loads(n["installed"] or "[]")
        nodes.append({
            "id": n["node_id"], "host": n["hostname"], "mode": n["mode"],
            "caps": ", ".join(_json.loads(n["capabilities"] or "[]")),
            "loaded": ", ".join(_json.loads(n["loaded"] or "[]")),
            "installed": ", ".join(installed),
            "n_installed": len(installed),
            "seen": (n["last_heartbeat"] or "—"),
            # Version governance (ADR 27): drift and unfitness must be visible, not buried.
            "version": n["agent_version"] or "unknown",
            "fitness": n["fitness"] or "unknown",
            "fitness_reason": n["fitness_reason"],
        })
    return templates.TemplateResponse(request, "partials/nodes.html", {"nodes": nodes})
