"""FastAPI app: the async-plane submit/poll API (protocols.md §1b).

POST /jobs   → resolve addressing, persist to SQLite, XADD to q:<capability>, 202.
GET  /jobs/{id} → assemble lifecycle state from SQLite + the result blob in Redis.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .background import coordinator_loop
from .config import settings
from .coordinator import resolve_capability
from .fleet import load_fleet
from .ids import new_ids, new_reservation_id
from .models import JobRecord, JobSubmit, ReservationSubmit, Urgency
from .queue import Queue
from .reservations import admit, iso
from .store import Store
from .sync import build_router, sync_routes
from .usage import build_usage_summary
from .wake import WakeCoordinator

# Terminal statuses live in the result blob; anything else is queue state.
_TERMINAL = {"done", "failed", "expired"}

# Urgency classes that carry wake rights (ADR 18): directly submitted or escalated.
_WAKE_RIGHTS = {Urgency.urgent, Urgency.necessary}

_log = logging.getLogger("clusterbuck")


def create_app(
    redis_url: str | None = None,
    db_path: str | None = None,
    fleet_path: str | None = None,
    start_scheduler: bool = True,
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
            capability=body.capability,
            task_class=body.task_class,
            min_ability=body.min_ability,
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
            app.state.fleet,
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

    return app


app = create_app()
