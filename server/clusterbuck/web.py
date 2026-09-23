"""The web dashboard (ADR 21): server-rendered, htmx over the existing data, vendored
assets (no CDN, LAN-only). Three pages, each loaded once with panels that self-refresh via
`hx-get`:

  * `/` — a glance at the fleet: a one-line status strip (spend, only the queues that need
    attention, reservations if any), activity over time by venue or by worker, the
    coordinator's connections with what each worker has warm, and recent jobs.
  * `/models` — two tabs. Models: the joined (artifact, node) table and a card per enrolled
    node. Proposals: the planner's pending decisions, each laid out as candidate against
    incumbent and requirement against hardware.
  * `/performance` — load-test runs.

Panels read app.state directly (same data the JSON APIs serve) rather than self-calling
over HTTP. The dashboard is for looking at; every figure it drops is still on the JSON API.
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


@web_routes.get("/ui/status", response_class=HTMLResponse)
async def ui_status(request: Request) -> HTMLResponse:
    """One line for the usage page: what used to be three panels (headline, queues,
    reservations) that were mostly zeros. A healthy queue is not news, so only the ones
    that need a look get a pill; everything else collapses to "queues clear".

    The stuck test is the one the full queues table used: work waiting with neither a live
    consumer nor a cloud executor to take it. `pending` is deliberately not the signal —
    it counts work a worker has ALREADY claimed, which is progress, not backlog.
    """
    store = request.app.state.store
    fleet = request.app.state.fleet
    queue = request.app.state.queue
    u = build_usage_summary(store, settings.cloud_budget_monthly)

    attention = []
    for cap in (fleet.capabilities if fleet else {}):
        stats = await queue.depth(cap, settings.consumer_group)
        backlog = stats.get("backlog")
        nobody_home = (stats.get("consumers") or 0) + (stats.get("executors") or 0) == 0
        if backlog is None:
            attention.append({"capability": cap, "state": "unknown", "backlog": None})
        elif backlog and nobody_home:
            attention.append({"capability": cap, "state": "stuck", "backlog": backlog})
        elif backlog:
            attention.append({"capability": cap, "state": "backlog", "backlog": backlog})

    # Live ones only: list_reservations is history, and a strip that counted it would never
    # go quiet once a single reservation had ever been made.
    live = store.active_reservations()
    return templates.TemplateResponse(
        request, "partials/status.html",
        {"u": u, "attention": attention, "has_queues": bool(fleet and fleet.capabilities),
         "reservations": live})


@web_routes.get("/ui/connections", response_class=HTMLResponse)
async def ui_connections(request: Request) -> HTMLResponse:
    """Coordinator <-> worker/cloud topology: every enrolled node and every registered
    cloud provider account, each with how many jobs it has actually served (usage_rollup's
    "node" column carries a worker's node_id, or "cloud:<provider>" — cloud_executor.py)."""
    import json as _json

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
        loaded = _json.loads(n.loaded or "[]")
        installed = _json.loads(n.installed or "[]")
        workers.append({
            "name": n.hostname or n.node_id, "mode": n.mode or "unknown",
            "silent": age is None or age > settings.node_silent_s,
            "age": humanize_age(age) if age is not None else None,
            "jobs": jobs_by_key.get(n.node_id, 0),
            # What this machine can answer with right now (warm) versus after a cold load.
            # "Which models are available" is the question this panel is looked at for.
            "warm": sorted(loaded),
            "cold": sorted(m for m in installed if m not in set(loaded)),
            "tiers": sorted(_json.loads(n.capabilities or "[]")),
        })
    cloud_caps = cloud_capabilities(fleet) if fleet else []
    tiers_by_provider: dict[str, list[str]] = {}
    for cap in cloud_caps:
        tiers_by_provider.setdefault(provider_of(fleet.capabilities[cap].model), []).append(cap)
    cloud = [{"provider": p, "jobs": jobs_by_key.get(f"cloud:{p}", 0),
              "tiers": sorted(tiers_by_provider[p])}
             for p in sorted(tiers_by_provider)]

    return templates.TemplateResponse(
        request, "partials/connections.html", {"workers": workers, "cloud": cloud}
    )


@web_routes.get("/ui/activity-series")
async def ui_activity_series(request: Request, by: str = "venue") -> dict:
    """JSON (not HTML — the chart fetches and redraws itself; see dashboard.js) feeding the
    usage page's full-width activity-over-time chart: trailing 30 days, either local vs
    cloud (`by=venue`, the default) or one jobs series per worker (`by=node`)."""
    from .usage import build_activity_series, build_activity_series_by_node

    days = 30
    store = request.app.state.store
    if by == "node":
        # Every enrolled node, in enrollment order, so a worker's colour slot is fixed for
        # as long as it is enrolled: a quiet month or a newcomer never repaints it.
        enrolled = sorted(store.list_nodes(), key=lambda n: (n.enrolled_at or "", n.node_id))
        return build_activity_series_by_node(
            store.usage_daily_by_node(days=days), days=days,
            names={n.node_id: n.hostname or n.node_id for n in enrolled},
            order=[n.node_id for n in enrolled])
    return build_activity_series(store.usage_daily_by_venue(days=days), days=days)


@web_routes.get("/ui/timeline", response_class=HTMLResponse)
async def ui_timeline(request: Request) -> HTMLResponse:
    """Most recent metered jobs, newest first (Store.recent_usage) — the raw events behind
    the by_day rollup, fine-grained enough to matter at fleet-sized job volumes."""
    store = request.app.state.store
    rows = store.recent_usage(limit=30)
    # Usage rows carry node_id; people know the machine by its hostname.
    names = {n.node_id: n.hostname for n in store.list_nodes() if n.hostname}
    return templates.TemplateResponse(
        request, "partials/timeline.html", {"rows": rows, "names": names})


# What approving or denying each kind of proposal actually does. It used to be a whole
# table column restating itself on every row; it is a tooltip on the buttons now, which is
# the moment someone wants to know.
_DECISION_HINTS = {
    "upgrade": ("install {artifact} on {node} (next time it's away)",
                "leave {node}'s models unchanged"),
    "reeval": ("run the eval harness against {artifact} to (re-)measure its ability",
               "leave {artifact}'s ability score as is"),
    "reclaim": ("remove {artifact} from {node}, freeing its disk",
                "keep {artifact} installed"),
}


def _proposal_view(store, row, node, catalog: dict, scale: str) -> dict:
    """A proposal laid out from the structured facts behind it, not parsed from its
    rationale string.

    The rationale is one sentence compressing four comparisons — candidate ability against
    the best installed, model size against disk quota, RAM needed against RAM present,
    model against accelerator memory — and reading it meant unpacking all four in your
    head. Each is recomputed here from the same sources `scan_node` used, so it can be
    shown as two columns side by side.

    The incumbent has to be recomputed: `scan_node` knows it but stores `incumbent=None`.
    """
    import json as _json

    from .catalog import fits, quota_for

    name = (node.hostname if node else None) or row.node_id
    accept, deny = _DECISION_HINTS.get(row.kind, ("apply this proposal", "leave it"))
    view = {
        "id": row.id, "kind": row.kind, "artifact": row.artifact, "node": name,
        "rationale": row.rationale,
        "accept_hint": accept.format(artifact=row.artifact, node=name),
        "deny_hint": deny.format(artifact=row.artifact, node=name),
        "cand": None, "hw": None, "verdict": None, "fit_reason": None,
        "task_class": row.task_class, "expected": None, "incumbent": None,
        "incumbent_score": None,
    }
    if node is not None:
        view["hw"] = {
            "ram_gb": node.ram_gb, "vram_gb": node.vram_gb,
            "accelerator": node.accelerator, "disk_free_gb": node.disk_free_gb,
            "quota_gb": quota_for(node.profile, node.disk_quota_gb),
            "profile": node.profile, "tps": node.tps,
        }
    cand = catalog.get(row.artifact)
    if cand is not None:
        view["cand"] = {
            "size_gb": cand.size_gb, "min_ram_gb": cand.min_ram_gb,
            "params_b": cand.params_b, "active_params_b": cand.active_params_b,
            "quant": cand.quant, "context_tokens": cand.context_tokens,
            "tools": cand.supports_tools, "json_schema": cand.supports_json_schema,
            "vision": cand.supports_vision, "family": cand.family,
        }
        view["expected"] = cand.expected_ability
        if node is not None:
            view["verdict"], view["fit_reason"] = fits(
                cand, node, view["hw"]["quota_gb"])

    if row.kind == "upgrade" and row.task_class and node is not None:
        best = None
        for have in _json.loads(node.installed or "[]"):
            score = store.get_ability(have, row.task_class, scale)
            if score is not None and (best is None or score > best[1]):
                best = (have, score)
        if best:
            view["incumbent"], view["incumbent_score"] = best
    return view


def _pending_proposals(request: Request) -> list[dict]:
    from .evaluation import SCALE_VERSION

    store = request.app.state.store
    # Proposals are keyed by node_id (the stable identity proposals/actions reference),
    # but on the LAN people know machines by hostname — the view shows that instead.
    nodes = {n.node_id: n for n in store.list_nodes()}
    catalog = {c.artifact: c for c in store.list_catalog()}
    return [_proposal_view(store, r, nodes.get(r.node_id), catalog, SCALE_VERSION)
            for r in store.list_proposals("pending")]


@web_routes.get("/ui/proposals", response_class=HTMLResponse)
async def ui_proposals(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request, "partials/proposals.html", {"props": _pending_proposals(request)})


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
    # A cloud tier's model lives at the provider, never on a node, so it is not an orphan.
    from .cloud_executor import cloud_capabilities

    cloud_models = ({fleet.capabilities[c].model for c in cloud_capabilities(fleet)}
                    if fleet else set())
    on_a_node = {r["artifact"] for r in rows}
    orphan_tiers = sorted(
        {m for m in tiers_for if m not in on_a_node and m not in cloud_models}
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

    fleet = request.app.state.fleet
    # fleet.yaml's per-node `wake` policy was the one thing the old Fleet panel showed that
    # no other panel did. A seeded node is named by its fleet id, which an enrolled node
    # matches by id or by hostname.
    wake = {nd.id: nd.wake for nd in fleet.nodes} if fleet else {}

    now = datetime.now(UTC)
    nodes = []
    for n in request.app.state.store.list_nodes():
        installed = _json.loads(n.installed or "[]")
        age = heartbeat_age_s(n, now=now)
        nodes.append({
            "wake": wake.get(n.node_id) or wake.get(n.hostname or ""),
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
