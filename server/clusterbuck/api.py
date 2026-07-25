"""FastAPI app: the async-plane submit/poll API (protocols.md §1b).

POST /jobs   → resolve addressing, persist to SQLite, XADD to q:<capability>, 202.
GET  /jobs/{id} → assemble lifecycle state from SQLite + the result blob in Redis.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .background import coordinator_loop
from .catalog import (
    apply_action_result,
    next_action,
    propose_reeval,
    quota_for,
    scan_all,
    seed_catalog,
)
from .config import settings
from .coordinator import propose_capabilities
from .evaluation import SCALE_VERSION, TASK_CLASSES, seed_ability
from .fleet import load_fleet
from .routing import resolve_capability
from .ids import new_ids, new_join_token, new_node_id, new_node_key, new_reservation_id
from .models import (
    AttentionRequest,
    EnrollRequest,
    HeartbeatRequest,
    JobRecord,
    JobSubmit,
    ReservationSubmit,
    Urgency,
)
from .queue import Queue
from .reservations import admit, iso
from .signing import build_manifest, load_private_pem
from .store import Store
from .sync import build_router, sync_routes
from .usage import build_usage_summary
from .wake import WakeCoordinator
from .web import WEB_DIR, web_routes

# Terminal statuses live in the result blob; anything else is queue state.
_TERMINAL = {"done", "failed", "expired"}

# Urgency classes that carry wake rights (ADR 18): directly submitted or escalated.
_WAKE_RIGHTS = {Urgency.urgent, Urgency.necessary}

_log = logging.getLogger("clusterbuck")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _now_iso_at(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def create_app(
    redis_url: str | None = None,
    db_path: str | None = None,
    fleet_path: str | None = None,
    start_scheduler: bool = True,
    update_signing_key: str | None = None,
    update_release: str | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.queue = Queue.from_url(redis_url)
        app.state.store = Store(db_path or settings.db_path)

        # Sync plane: load the fleet registry and build the LiteLLM router if present.
        # Absent fleet.yaml ⇒ sync endpoints return 503; the async plane still works.
        path = Path(fleet_path or settings.fleet_path)
        if path.exists():
            app.state.fleet = load_fleet(path)
            app.state.sync_router = build_router(
                app.state.fleet, settings.cloud_fallback_model
            )
            _log.info(
                "sync plane up: %d capabilities from %s",
                len(app.state.fleet.capabilities), path.resolve(),
            )
        else:
            app.state.fleet = None
            app.state.sync_router = None
            # Relative default resolves against CWD — a common silent-503 footgun.
            _log.warning(
                "sync plane disabled: no fleet file at %s (set CBK_FLEET_PATH); "
                "/v1/* will return 503, async plane unaffected", path.resolve(),
            )

        app.state.update_signing_key = update_signing_key or settings.update_signing_key
        app.state.update_release = update_release or settings.update_release

        # Seed the ability matrix with anchored defaults (overwritten by real eval runs)
        # and the model catalog with generic known-good artifacts.
        seed_ability(app.state.store, now=_now_iso())
        seed_catalog(app.state.store, now=_now_iso())

        # Wake coordinator (needs the fleet for MAC lookup + Redis for liveness).
        app.state.wake = WakeCoordinator(
            app.state.fleet,
            app.state.queue.client,
            group=settings.consumer_group,
            dead_ms=settings.worker_dead_ms,
            cooldown_s=settings.wake_cooldown_s,
            broadcast=settings.wol_broadcast,
            port=settings.wol_port,
        )

        # Escalation engine: background scan promoting due waitable jobs.
        stop = asyncio.Event()
        task: asyncio.Task | None = None
        if start_scheduler:
            task = asyncio.create_task(
                coordinator_loop(
                    app.state.store, app.state.queue, app.state.wake, app.state.fleet,
                    interval_s=settings.escalation_interval_s, stop=stop,
                )
            )

        try:
            yield
        finally:
            stop.set()
            if task is not None:
                await task
            await app.state.queue.aclose()

    app = FastAPI(title="clusterbuck server", version="0.0.1", lifespan=lifespan)
    app.include_router(sync_routes)
    app.include_router(web_routes)
    app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/fleet")
    async def get_fleet() -> dict:
        """The capability/node registry (MACs omitted — not needed for listing)."""
        fleet = app.state.fleet
        if fleet is None:
            return {"capabilities": {}, "nodes": []}
        return {
            "capabilities": {
                name: {"queue": c.queue, "model": c.model, "model_server": c.model_server}
                for name, c in fleet.capabilities.items()
            },
            "nodes": [
                {"id": n.id, "wake": n.wake, "capabilities": n.capabilities}
                for n in fleet.nodes
            ],
        }

    @app.get("/usage")
    async def get_usage() -> dict:
        """Metering rollups + the avoided-cloud-spend headline (fleet-management → Usage)."""
        return build_usage_summary(app.state.store, settings.cloud_budget_monthly)

    @app.get("/ability")
    async def get_ability() -> dict:
        """The ability matrix + a per-artifact headline scalar (ADR 15/16)."""
        rows = app.state.store.ability_matrix(SCALE_VERSION)
        matrix = [{"artifact": r["artifact"], "task_class": r["task_class"],
                   "score": r["score"]} for r in rows]
        # Headline scalar per artifact = mean over its measured task classes (equal-weight
        # for now; a workload-weighted headline is the documented refinement).
        by_artifact: dict[str, list[float]] = {}
        for r in rows:
            by_artifact.setdefault(r["artifact"], []).append(r["score"])
        headline = {a: round(sum(s) / len(s), 1) for a, s in by_artifact.items()}
        return {"scale_version": SCALE_VERSION, "task_classes": TASK_CLASSES,
                "matrix": matrix, "headline": headline}

    @app.get("/queues")
    async def get_queues() -> dict:
        """Per-capability queue depth / pending / live consumers."""
        fleet = app.state.fleet
        out = []
        for cap in (fleet.capabilities if fleet else {}):
            stats = await app.state.queue.depth(cap, settings.consumer_group)
            out.append({"capability": cap, "queue": f"q:{cap}", **stats})
        return {"queues": out}

    @app.post("/attention")
    async def attention(body: AttentionRequest) -> dict:
        """A client's user became active (or went idle) — heat up / cool down its backlog."""
        store = app.state.store
        if body.state == "idle":
            # End the lease early: demote this client's unstarted attention-promotions.
            for job in store.attention_promoted_jobs(body.client_key):
                if await app.state.queue.read_result(job["result_key"]) is None:
                    store.demote_job(job["id"])
            store.delete_attention_lease(body.client_key)
            return {"promoted": 0, "prewarm": [], "lease_expires": None}

        affected = store.attention_promote(body.client_key, body.scope)
        expires_at = datetime.now(timezone.utc).timestamp() + body.ttl_s
        store.upsert_attention_lease(
            body.client_key, json.dumps(body.scope) if body.scope else None, expires_at,
        )

        # Promotion grants wake rights; pre-warm the artifacts the backlog needs.
        fleet = app.state.fleet
        caps = {r["capability"] for r in affected}
        prewarm = []
        for cap in caps:
            await app.state.wake.maybe_wake(cap, reason=f"attention:{body.client_key}")
            if fleet and cap in fleet.capabilities:
                prewarm.append(fleet.capabilities[cap].model)

        return {
            "promoted": len(affected),
            "prewarm": sorted(set(prewarm)),
            "lease_expires": _now_iso_at(expires_at),
        }

    @app.post("/jobs", status_code=202)
    async def submit_job(body: JobSubmit) -> JSONResponse:
        # A reservation, if named, must exist and be confirmed. It does NOT override
        # addressing (the job addresses normally); it's recorded as the linkage (§8).
        if body.reservation is not None:
            rsv = app.state.store.get_reservation(body.reservation)
            if rsv is None or rsv["status"] != "confirmed":
                raise HTTPException(
                    status_code=400, detail="unknown or unconfirmed reservation"
                )

        capability = resolve_capability(
            app.state.fleet, app.state.store,
            capability=body.capability,
            task_class=body.task_class,
            min_ability=body.min_ability,
            privacy=body.privacy.value,
        )
        job_id, result_key = new_ids()
        now = datetime.now(timezone.utc)
        created_at = now.isoformat().replace("+00:00", "Z")

        record = JobRecord(
            id=job_id,
            created_at=created_at,
            capability=capability,
            messages=body.messages,
            prompt=body.prompt,
            params=body.params,
            urgency=body.urgency,
            escalate_after_min=body.escalate_after_min,
            privacy=body.privacy,
            deadline=body.deadline,
            result_key=result_key,
        )

        # A patience bound (waitable(N)) becomes an absolute escalation deadline.
        escalate_at = None
        if body.urgency is Urgency.waitable and body.escalate_after_min is not None:
            escalate_at = (now + timedelta(minutes=body.escalate_after_min)).timestamp()

        deadline_epoch = None
        if body.deadline:
            try:
                deadline_epoch = datetime.fromisoformat(
                    body.deadline.replace("Z", "+00:00")
                ).timestamp()
            except ValueError:
                deadline_epoch = None

        app.state.store.insert(
            id=job_id,
            result_key=result_key,
            capability=capability,
            created_at=created_at,
            urgency=body.urgency.value,
            escalate_at=escalate_at,
            reservation=body.reservation,
            deadline_epoch=deadline_epoch,
            client_key=body.client_key,
            task_class=body.task_class,
        )
        await app.state.queue.enqueue(record.to_wire())

        # Wake rights: urgent/necessary jobs may create capacity on submit.
        if body.urgency in _WAKE_RIGHTS:
            await app.state.wake.maybe_wake(capability, reason=f"submit:{body.urgency.value}")

        return JSONResponse(
            status_code=202,
            content={"id": job_id, "result_key": result_key, "status": "queued"},
        )

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str) -> dict:
        row = app.state.store.get(job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="unknown job id")

        result = await app.state.queue.read_result(row["result_key"])
        if result is None:
            # No terminal result yet — report the queue-state we last recorded.
            return {
                "id": job_id,
                "status": row["status"],
                "urgency": row["urgency"],  # reflects escalation (waitable → necessary)
                "result": None,
                "error": None,
                "attempts": 0,
                "worker": None,
            }

        status = result.get("status", "done")
        if status in _TERMINAL and row["status"] != status:
            app.state.store.set_status(job_id, status)

        return {
            "id": job_id,
            "status": status,
            "urgency": row["urgency"],
            "result": result.get("completion"),
            "error": result.get("error"),
            "attempts": result.get("attempts", 1),
            "worker": result.get("worker"),
        }

    def _reservation_view(row) -> dict:
        plan = None
        if row["status"] == "confirmed":
            plan = {
                "node": row["node"],
                "artifact": row["artifact"],
                "warm_by": iso(row["warm_by"]),
                "starts": iso(row["starts"]),
                "ends": iso(row["ends"]),
            }
        return {
            "id": row["id"],
            "status": row["status"],
            "state": row["state"],
            "plan": plan,
        }

    @app.post("/reservations", status_code=201)
    async def create_reservation(body: ReservationSubmit) -> JSONResponse:
        decision = admit(
            app.state.fleet, app.state.store,
            task_class=body.task_class,
            min_ability=body.min_ability,
            window_start=body.window.start,
            duration_min=body.duration_min,
            lead_s=settings.warm_lead_s,
        )
        rsv_id = new_reservation_id()
        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        # confirmed reservations start their lifecycle; declined ones have none.
        state = "scheduled" if decision.status == "confirmed" else None
        app.state.store.insert_reservation(
            id=rsv_id, status=decision.status, state=state,
            task_class=body.task_class, min_ability=body.min_ability,
            capability=decision.capability, node=decision.node, artifact=decision.artifact,
            priority=body.priority, privacy=body.privacy.value, load=body.load,
            duration_min=body.duration_min, est_jobs=body.est_jobs,
            warm_by=decision.warm_by, starts=decision.starts, ends=decision.ends,
            created_at=created_at,
        )
        content = _reservation_view(app.state.store.get_reservation(rsv_id))
        content["counter"] = None  # counter-offers deferred (M2b)
        if decision.status == "declined":
            content["reason"] = decision.reason
        return JSONResponse(status_code=201, content=content)

    @app.get("/reservations")
    async def list_reservations() -> dict:
        rows = app.state.store.list_reservations()
        return {"reservations": [_reservation_view(r) for r in rows]}

    @app.get("/reservations/{rsv_id}")
    async def get_reservation(rsv_id: str) -> dict:
        row = app.state.store.get_reservation(rsv_id)
        if row is None:
            raise HTTPException(status_code=404, detail="unknown reservation id")
        return _reservation_view(row)

    @app.delete("/reservations/{rsv_id}")
    async def cancel_reservation(rsv_id: str) -> dict:
        if app.state.store.get_reservation(rsv_id) is None:
            raise HTTPException(status_code=404, detail="unknown reservation id")
        app.state.store.cancel_reservation(rsv_id)
        return _reservation_view(app.state.store.get_reservation(rsv_id))

    # --- dynamic registry: enrollment + heartbeat (protocols.md §6) ---

    @app.post("/nodes/tokens", status_code=201)
    async def mint_join_token() -> dict:
        """Admin mints a one-time join token (LAN admin auth is a later concern)."""
        token = new_join_token()
        app.state.store.mint_token(token, _now_iso())
        return {"join_token": token}

    @app.post("/nodes/enroll", status_code=201)
    async def enroll_node(body: EnrollRequest) -> JSONResponse:
        node_id = new_node_id()
        if not app.state.store.claim_token(body.join_token, used_by=node_id):
            raise HTTPException(status_code=401, detail="invalid or already-used join token")
        node_key = new_node_key()
        caps, ladder = propose_capabilities(body.hw.ram_gb)
        app.state.store.enroll_node(
            node_id=node_id, node_key=node_key, req=body,
            capabilities=json.dumps(caps), enrolled_at=_now_iso(),
        )
        return JSONResponse(status_code=201, content={
            "node_id": node_id, "node_key": node_key,
            "proposed": {"capabilities": caps, "ladder": ladder},
        })

    @app.post("/nodes/{node_id}/heartbeat")
    async def heartbeat(
        node_id: str, body: HeartbeatRequest,
        x_cbk_node_key: str | None = Header(default=None),
    ) -> dict:
        node = app.state.store.get_node(node_id)
        if node is None:
            raise HTTPException(status_code=404, detail="unknown node")
        if x_cbk_node_key != node["node_key"]:
            raise HTTPException(status_code=401, detail="bad node key")
        now = _now_iso()
        store = app.state.store
        store.record_heartbeat(
            node_id=node_id, mode=body.mode,
            installed=json.dumps(body.installed), loaded=json.dumps(body.loaded),
            queues=json.dumps(body.queues),
            jobs_done=body.stats.get("jobs_done"), tps=body.stats.get("tps"),
            last_heartbeat=now,
        )

        # Record observed artifacts + digests; a changed digest is a NEW artifact whose
        # stored ability is stale, so it earns a re-eval proposal (ADR 15).
        observed = {a: (body.digests or {}).get(a) for a in body.installed}
        notes: list[str] = []
        for artifact, old, new in store.observe_node_models(node_id, observed, now):
            if propose_reeval(store, node_id, artifact, old, new, now=now):
                notes.append(f"{artifact} changed upstream — re-evaluation proposed")

        # Close the loop on any action the previous response issued.
        if body.action_result is not None:
            notes += apply_action_result(store, node_id, body.action_result, now=now)

        # Hand out the next approved action, if one is permitted in this presence mode.
        action = next_action(store, store.get_node(node_id), mode=body.mode)

        # Self-update manifest delivery (M4c apply path) is deferred; see ADR 13.
        return {"update": None, "action": action, "planner_notes": notes}

    @app.get("/updates/manifest")
    async def update_manifest(rid: str) -> dict:
        """Signed release manifest for a runtime id (protocols.md §7). 404 if no update
        channel is configured (no signing key / release). The worker verifies the
        signature against its pinned public key before applying anything (ADR 13)."""
        signing_key = app.state.update_signing_key
        release_path = app.state.update_release
        if not signing_key or not release_path:
            raise HTTPException(status_code=404, detail="update channel not configured")
        release = json.loads(Path(release_path).read_text())
        art = release.get("artifacts", {}).get(rid)
        if art is None:
            raise HTTPException(status_code=404, detail=f"no artifact for rid {rid}")
        key = load_private_pem(Path(signing_key).read_text())
        return build_manifest(
            key, version=release["version"], rid=rid, url=art["url"], sha256=art["sha256"],
            channel=release.get("channel", "stable"),
            protocol_version=release.get("protocol_version", 1),
        )

    # --- model catalog & planner proposals (M6b) ---

    @app.get("/catalog")
    async def get_catalog() -> dict:
        return {"artifacts": [
            {"artifact": r["artifact"], "family": r["family"], "params_b": r["params_b"],
             "quant": r["quant"], "size_gb": r["size_gb"], "min_ram_gb": r["min_ram_gb"],
             "source": r["source"], "registry_ref": r["registry_ref"],
             "expected_ability": r["expected_ability"]}
            for r in app.state.store.list_catalog()]}

    @app.get("/proposals")
    async def get_proposals(status: str | None = None) -> dict:
        return {"proposals": [
            {"id": r["id"], "kind": r["kind"], "node_id": r["node_id"],
             "artifact": r["artifact"], "task_class": r["task_class"],
             "rationale": r["rationale"], "status": r["status"],
             "created_at": r["created_at"], "decided_at": r["decided_at"]}
            for r in app.state.store.list_proposals(status)]}

    @app.post("/proposals/scan")
    async def scan_proposals() -> dict:
        """Force a planner pass (it also runs on the coordinator tick)."""
        ids = scan_all(app.state.store, now=_now_iso())
        return {"created": ids}

    @app.post("/proposals/{proposal_id}/approve")
    async def approve_proposal(proposal_id: str) -> dict:
        return _decide(proposal_id, "approved")

    @app.post("/proposals/{proposal_id}/deny")
    async def deny_proposal(proposal_id: str) -> dict:
        return _decide(proposal_id, "denied")

    def _decide(proposal_id: str, status: str) -> dict:
        store = app.state.store
        if store.get_proposal(proposal_id) is None:
            raise HTTPException(status_code=404, detail="unknown proposal id")
        if not store.decide_proposal(proposal_id, status, _now_iso()):
            raise HTTPException(status_code=409, detail="proposal already decided")
        row = store.get_proposal(proposal_id)
        return {"id": row["id"], "status": row["status"], "decided_at": row["decided_at"]}

    @app.post("/nodes/{node_id}/policy")
    async def set_node_policy(
        node_id: str, disk_quota_gb: float | None = None, auto_approve: bool | None = None,
    ) -> dict:
        """The owner's contract for this node: storage quota and whether installs may be
        applied without a human decision (opt-in, off by default)."""
        store = app.state.store
        node = store.get_node(node_id)
        if node is None:
            raise HTTPException(status_code=404, detail="unknown node")
        store.set_node_flags(node_id, disk_quota_gb=disk_quota_gb, auto_approve=auto_approve)
        node = store.get_node(node_id)
        return {"node_id": node_id,
                "disk_quota_gb": quota_for(node["profile"], node["disk_quota_gb"]),
                "auto_approve": bool(node["auto_approve"])}

    @app.get("/nodes")
    async def list_nodes() -> dict:
        def view(n) -> dict:  # never expose node_key
            return {
                "node_id": n["node_id"], "hostname": n["hostname"],
                "os": n["os"], "arch": n["arch"], "profile": n["profile"],
                "ram_gb": n["ram_gb"], "accelerator": n["accelerator"],
                "capabilities": json.loads(n["capabilities"] or "[]"),
                "mode": n["mode"],
                # Observed from the node's model server, not configured (M6a).
                "installed": json.loads(n["installed"] or "[]"),
                "loaded": json.loads(n["loaded"] or "[]"),
                "last_heartbeat": n["last_heartbeat"],
            }
        return {"nodes": [view(n) for n in app.state.store.list_nodes()]}

    return app


app = create_app()
