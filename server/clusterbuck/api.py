"""FastAPI app: the async-plane submit/poll API (protocols.md §1b).

POST /jobs   → resolve addressing, persist to SQLite, XADD to q:<capability>, 202.
GET  /jobs/{id} → assemble lifecycle state from SQLite + the result blob in Redis.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .config import settings
from .coordinator import resolve_capability
from .ids import new_ids
from .models import JobRecord, JobSubmit
from .queue import Queue
from .store import Store

# Terminal statuses live in the result blob; anything else is queue state.
_TERMINAL = {"done", "failed", "expired"}


def create_app(redis_url: str | None = None, db_path: str | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.queue = Queue.from_url(redis_url)
        app.state.store = Store(db_path or settings.db_path)
        try:
            yield
        finally:
            await app.state.queue.aclose()

    app = FastAPI(title="clusterbuck server", version="0.0.1", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/jobs", status_code=202)
    async def submit_job(body: JobSubmit) -> JSONResponse:
        capability = resolve_capability(
            capability=body.capability,
            task_class=body.task_class,
            min_ability=body.min_ability,
        )
        job_id, result_key = new_ids()
        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

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

        app.state.store.insert(
            id=job_id,
            result_key=result_key,
            capability=capability,
            created_at=created_at,
        )
        await app.state.queue.enqueue(record.to_wire())

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
            "result": result.get("completion"),
            "error": result.get("error"),
            "attempts": result.get("attempts", 1),
            "worker": result.get("worker"),
        }

    return app


app = create_app()
