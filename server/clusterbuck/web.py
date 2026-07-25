"""The web dashboard (ADR 21): server-rendered, htmx over the existing data, vendored
assets (no CDN, LAN-only). The full page loads once; each panel refreshes itself with
`hx-get` on a short interval. Panels read app.state directly (same data the JSON APIs
serve) rather than self-calling over HTTP.
"""

from __future__ import annotations

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
