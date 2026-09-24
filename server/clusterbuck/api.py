"""FastAPI app: the async-plane submit/poll API (protocols.md §1b).

POST /jobs   → resolve addressing, persist to SQLite, XADD to q:<capability>, 202.
GET  /jobs/{id} → assemble lifecycle state from SQLite + the result blob in Redis.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from .auth import install_auth, key_matches
from .background import coordinator_loop
from .capability_proposal import propose_capabilities
from .catalog import (
    apply_action_result,
    next_action,
    propose_reeval,
    quota_for,
    scan_all,
    seed_catalog,
)
from .cloud_executor import CloudExecutor, cloud_capabilities
from .config import JOIN_PASSWORD_MIN_LEN, settings
from .errors import CbkError, install_error_handlers
from .eval_runner import artifacts_needing_eval, eval_tick
from .evaluation import (
    SCALE_VERSION,
    TASK_CLASSES,
    TIER1_MAX_ABILITY,
    seed_ability,
)
from .fleet import load_fleet, unservable_capabilities
from .ids import new_ids, new_join_token, new_node_id, new_node_key, new_reservation_id
from .models import (
    RESULT_STATUSES,
    TERMINAL_STATUSES,
    CatalogEntrySubmit,
    EnrollRequest,
    HeartbeatRequest,
    JobRecord,
    JobSubmit,
    NodePolicy,
    PerfRunSubmit,
    ReservationSubmit,
    Urgency,
)
from .perf_runner import UnknownCategory, perf_run_list_view, perf_run_view, start_run
from .queue import Queue, stream_key, tier_for, tier_of
from .reservations import admit, iso
from .routing import RoutingRefusal, resolve
from .signing import build_manifest, load_private_pem, public_pem, sign_bootstrap
from .store import Store
from .sync import build_router, sync_routes
from .tiering import tiering_ready
from .usage import build_usage_summary, venue_of
from .versions import assess, parse_version, policy_from_settings, version_gt
from .wake import WakeCoordinator
from .web import WEB_DIR, web_routes

# Terminal statuses live in the result blob; anything else is queue state.

# Urgency classes that carry wake rights (ADR 18): directly submitted or escalated.
_WAKE_RIGHTS = {Urgency.urgent, Urgency.necessary}

_log = logging.getLogger("clusterbuck")


_IDEMPOTENCY_KEY_MAX = 255

# Addresses a remote worker can never reach.
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def _validated_idempotency_key(raw: str | None) -> str | None:
    """Normalise and check an `Idempotency-Key` header, or None if absent.

    Kept strict and small: the key is only ever compared for equality, so anything
    unprintable or unbounded is a client bug worth reporting rather than storing. An
    absent header leaves every pre-existing submit behaviour bit-for-bit unchanged.
    """
    if raw is None:
        return None
    key = raw.strip()
    if not key:
        raise CbkError("invalid_request", "Idempotency-Key must not be empty", 400,
                       param="Idempotency-Key")
    if len(key) > _IDEMPOTENCY_KEY_MAX:
        raise CbkError(
            "invalid_request",
            f"Idempotency-Key must be at most {_IDEMPOTENCY_KEY_MAX} characters", 400,
            param="Idempotency-Key",
        )
    if not key.isascii() or not key.isprintable():
        raise CbkError("invalid_request", "Idempotency-Key must be printable ASCII", 400,
                       param="Idempotency-Key")
    return key


# Statuses the COORDINATOR decides, from its own clock or a client's DELETE, rather than
# from an executor's result. Once recorded they are final and no result blob overrides
# them. (`done` and `failed` need no such rule: they come from the first result written,
# and result writes are first-writer-wins.)
_SEALED_STATUSES = frozenset({"expired", "cancelled"})


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


# The Python worker is one artifact for every platform — no per-OS build, no matrix.
PY_ARTIFACT = "py3-none-any"

# agent_flavour → how to name the artifact this node can actually EXECUTE. Keyed by flavour
# because os+arch alone is not enough to name an artifact unambiguously; selection FAILS
# CLOSED — an unrecognised flavour gets no update, not a guess.
_ARTIFACT_KEY = {
    "python": lambda _os, _arch: PY_ARTIFACT,
}


def artifact_key_for(flavour: str | None, os_name: str | None,
                     arch: str | None) -> str | None:
    """Release-artifact key for a node, or None if we cannot name one safely.

    FAILS CLOSED. An unrecognised flavour returns None and the node is offered no update at
    all, because a worker left on an old version is a much smaller problem than a worker
    that replaced itself with an executable for a different runtime.

    An absent flavour reads as `python`, the only implementation there is — so a worker
    old enough to omit the field still gets the artifact it can actually run.
    """
    resolve = _ARTIFACT_KEY.get((flavour or "python").lower())
    if resolve is None:
        return None
    return resolve(os_name, arch)


def _build_update_for(app, node, flavour: str | None = None) -> dict | None:
    """A signed manifest this node can actually run, or None if no channel/artifact applies.

    Signed per-request rather than served from a static file so the payload always matches
    what this coordinator considers current; the worker verifies before touching a byte.

    `flavour` comes from the live heartbeat rather than the stored row so a worker that was
    just re-installed in a different language is judged on what it is now, not what it was.
    """
    key_path, release_path = app.state.update_signing_key, app.state.update_release
    if not key_path or not release_path:
        return None
    rid = artifact_key_for(flavour, node.os, node.arch)
    if rid is None:
        _log.warning("no artifact mapping for node %s (flavour=%s %s/%s) — cannot offer "
                     "an update", node.node_id, flavour or "python",
                     node.os, node.arch)
        return None
    try:
        release = json.loads(Path(release_path).read_text())
        art = release.get("artifacts", {}).get(rid)
        if art is None:
            return None
        # Only ever offer a node something NEWER than it runs.
        #
        # This was an equality check, which meant a channel naming an older build offered
        # every node a downgrade — and the worker, checking equality too, installed it.
        # That is the accidental half of the hazard: a release bumps five settings and
        # nothing reconciles them (see the release note in design.md), so a channel left
        # behind is an ordinary mistake rather than an attack. This fleet's did exactly
        # that, offering 0.10.0 to a node running 0.18.0 with auto-update on.
        #
        # The worker refuses a non-newer manifest as well, and that refusal is the real
        # control, since it also covers a REPLAYED manifest an attacker serves without
        # going through this function. This half stops the coordinator being the thing
        # that gets it wrong, and keeps the mistake out of a signed payload entirely.
        offered, running = parse_version(release["version"]), parse_version(
            node.agent_version)
        if running is not None and offered is not None and not version_gt(
            offered, running
        ):
            return None
        key = load_private_pem(Path(key_path).read_text())
        return build_manifest(
            key, version=release["version"], rid=rid, url=art["url"],
            sha256=art["sha256"], channel=release.get("channel", "stable"),
            protocol_version=release.get("protocol_version", 1),
        )
    except Exception:
        _log.exception("could not build an update manifest for %s", node.node_id)
        return None


def _now_iso_at(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=UTC).isoformat().replace("+00:00", "Z")


def create_app(
    redis_url: str | None = None,
    db_path: str | None = None,
    fleet_path: str | None = None,
    start_scheduler: bool = True,
    update_signing_key: str | None = None,
    update_release: str | None = None,
    api_key: str | None = None,
    join_password: str | None = None,
    worker_artifact: str | None = None,
    broker_advertise_url: str | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.queue = Queue.from_url(redis_url)
        resolved_db = db_path or settings.db_path
        app.state.store = Store(resolved_db)
        # ABSOLUTE, always. The default is relative and resolves against the working
        # directory, so a unit file with a different WorkingDirectory silently opens a
        # different database — the coordinator then starts healthy with an empty schema,
        # which looks like the fleet forgot everything rather than like the wrong path.
        # One line at startup is what turns that into a two-second diagnosis.
        _log.info("database: %s", Path(resolved_db).resolve())
        app.state.perf_tasks = {}  # run_id -> asyncio.Task, for cancellation

        # A row left 'running' belongs to a process that's gone (crash/SIGKILL — graceful
        # shutdown below always resolves its own runs) — reconcile so its cancel button
        # isn't a no-op forever.
        n_stale = app.state.store.cancel_stale_perf_runs(_now_iso())
        if n_stale:
            _log.warning(
                "reconciled %d perf run(s) stuck 'running' from a prior process", n_stale)

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
            # An unpriced capability contributes $0.00 to the avoided-cloud-spend
            # headline, silently. That headline is the number this project exists to
            # produce, so a tier added without prices does not merely go unmeasured —
            # it drags the total down and looks like the fleet earned less. Cheap to
            # forget (prices are the one field with a harmless-looking default) and
            # invisible afterwards, so say it once at startup.
            unpriced = sorted(
                name for name, c in app.state.fleet.capabilities.items()
                if not (c.price_in_per_1k or c.price_out_per_1k)
            )
            if unpriced:
                _log.warning(
                    "no price set for %s — these contribute $0 to avoided cloud spend "
                    "(set price_in_per_1k / price_out_per_1k in %s)",
                    ", ".join(unpriced), path.name,
                )
        else:
            app.state.fleet = None
            app.state.sync_router = None
            # Relative default resolves against CWD — a common silent-503 footgun.
            _log.warning(
                "sync plane disabled: no fleet file at %s (set CBK_FLEET_PATH); "
                "/v1/* will return 503, async plane unaffected", path.resolve(),
            )

        # Last-reported capability/model mismatch per node, so the warning is logged when
        # it changes rather than on every heartbeat. In-memory by design: it is a cache for
        # log deduplication, and losing it on restart just means one more warning.
        app.state.capability_warnings = {}

        app.state.update_signing_key = update_signing_key or settings.update_signing_key
        app.state.update_release = update_release or settings.update_release

        # Seed the ability matrix with anchored defaults (overwritten by real eval runs)
        # and the model catalog with generic known-good artifacts.
        seed_ability(app.state.store, now=_now_iso())
        seed_catalog(app.state.store, now=_now_iso())

        # Wake coordinator: the fleet for MAC lookup, and BOTH liveness signals — Redis
        # for stream-consumer idle time, the store for heartbeats. The heartbeat is the
        # one that keeps a node serving a long inference from reading as dead, since that
        # task is never blocked by the model call (see wake.py).
        app.state.wake = WakeCoordinator(
            app.state.fleet,
            app.state.queue.client,
            group=settings.consumer_group,
            dead_ms=settings.worker_dead_ms,
            cooldown_s=settings.wake_cooldown_s,
            broadcast=settings.wol_broadcast,
            port=settings.wol_port,
            store=app.state.store,
            silent_s=settings.node_silent_s,
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

        # Cloud executor (ADR 30): drains registered provider accounts' streams in-process,
        # so their API keys never leave the coordinator. Only started when the fleet
        # actually declares one, so a plain local fleet.yaml (and start_scheduler=False
        # tests) are unaffected.
        cloud_stop = asyncio.Event()
        cloud_task: asyncio.Task | None = None
        if start_scheduler and cloud_capabilities(app.state.fleet):
            app.state.cloud_executor = CloudExecutor(
                app.state.queue, app.state.fleet, consumer_group=settings.consumer_group,
            )
            cloud_task = asyncio.create_task(app.state.cloud_executor.run(cloud_stop))

        try:
            yield
        finally:
            stop.set()
            if task is not None:
                await task
            cloud_stop.set()
            if cloud_task is not None:
                await cloud_task
            # Any load-test runs still going must not outlive the app (they hold the
            # queue/store the shutdown below is about to close).
            for t in list(app.state.perf_tasks.values()):
                t.cancel()
            for t in list(app.state.perf_tasks.values()):
                try:
                    await asyncio.wait_for(t, timeout=5)
                except asyncio.CancelledError:
                    pass  # expected — we just cancelled it above
                except Exception:
                    _log.exception("perf run task raised during shutdown")
            await app.state.queue.aclose()

    app = FastAPI(title="clusterbuck server", version="0.0.1", lifespan=lifespan)
    # Attach auth before the routes so it gates every one of them (DESIGN.md → Security).
    install_auth(app, api_key if api_key is not None else settings.api_key)
    install_error_handlers(app)
    app.include_router(sync_routes)
    app.include_router(web_routes)
    app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon():
        return RedirectResponse("/static/favicon.ico", status_code=301)

    @app.get("/healthz")
    async def healthz(response: Response) -> dict:
        """Liveness AND readiness: can this coordinator actually do its job right now?

        It used to return a hardcoded `{"status": "ok"}`, checking nothing. That is the
        one answer this endpoint must never give wrongly: it is unauthenticated (so it
        is what a probe or a script can reach), the README tells an operator to check it
        when diagnosing the two reachability rules, and the systemd unit uses it. A
        coordinator whose Redis is unreachable cannot enqueue a job, cannot return a
        result and cannot run the reaper — and it answered `ok`.

        Redis is checked with a PING and SQLite with a trivial query, because those are
        the two things whose absence stops everything. Degraded ⇒ 503, so a caller that
        reads only the status code still learns the truth.

        Deliberately says WHICH dependency is down, and deliberately says no more than
        that: no URLs, no credentials, no version. The name of a failing dependency is
        what makes the check actionable; anything else would be handing detail to an
        unauthenticated caller for no operational gain.
        """
        checks: dict[str, str] = {}
        try:
            await app.state.queue.client.ping()
            checks["redis"] = "ok"
        except Exception as e:
            checks["redis"] = "unavailable"
            _log.warning("healthz: redis unreachable: %s", e)
        try:
            app.state.store.ping()
            checks["db"] = "ok"
        except Exception as e:
            checks["db"] = "unavailable"
            _log.warning("healthz: database unreachable: %s", e)

        healthy = all(v == "ok" for v in checks.values())
        if not healthy:
            response.status_code = 503
        return {"status": "ok" if healthy else "degraded", "checks": checks}

    @app.get("/fleet")
    async def get_fleet() -> dict:
        """The capability/node registry (MACs omitted — not needed for listing)."""
        fleet = app.state.fleet
        if fleet is None:
            return {"capabilities": {}, "nodes": []}
        return {
            "capabilities": {
                name: {"queue": stream_key(name), "model": c.model,
                       "model_server": c.model_server,
                       "cloud": c.cloud, "description": c.description}
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
        matrix = [{"artifact": r.artifact, "task_class": r.task_class,
                   "score": r.score,
                   # How much evidence is behind the score. model-evaluation.md asks for
                   # scores to be read "with an uncertainty note"; a bare number made a 7
                   # from forty items look identical to one from a lucky handful.
                   # `provenance: seed` with null counts is a placeholder, not a
                   # measurement — the harness will replace it.
                   "provenance": r.provenance,
                   "n_items": r.n_items, "n_passed": r.n_passed} for r in rows]
        # Headline scalar per artifact = mean over its measured task classes (equal-weight
        # for now; a workload-weighted headline is the documented refinement).
        # The dashboard no longer shows this mean at all — /ui/models renders the
        # per-task-class scores, because averaging a 9/9/3/3 specialist and a flat 6
        # to the same 6.0 erased the distinction the page existed to show.
        by_artifact: dict[str, list[float]] = {}
        for r in rows:
            by_artifact.setdefault(r.artifact, []).append(r.score)
        headline = {a: round(sum(s) / len(s), 1) for a, s in by_artifact.items()}
        return {"scale_version": SCALE_VERSION, "task_classes": TASK_CLASSES,
                # The highest a programmatic run may claim. Published so a caller can tell
                # "nothing here is that good" from "nothing here has been measured that
                # precisely" when a min_ability above it fails.
                "tier1_max_ability": TIER1_MAX_ABILITY,
                "matrix": matrix, "headline": headline}

    @app.post("/ability/clear")
    async def clear_ability(artifact: str) -> dict:
        """Drop an artifact's scores so the eval harness re-measures it (ADR 15).

        Normally only the heartbeat handler calls this, on a *digest* change. An operator
        needs the same reset when an artifact's behaviour changed without its digest
        moving — e.g. a model-server config/template edit — so expose it directly.

        Dropping the score is only half of it: ability is recomputed from the artifact's
        eval_runs, so a new measurement generation is opened too. Otherwise "force a
        re-measurement" produced a score averaged with the measurements the operator was
        trying to discard, which is the opposite of what they asked for.
        """
        n, generation = app.state.store.supersede_artifact(
            artifact, SCALE_VERSION, now=_now_iso())
        return {"artifact": artifact, "cleared": n, "generation": generation}

    @app.get("/eval")
    async def get_eval() -> dict:
        """Eval-harness state: what's measured, what's in flight, what still needs a score."""
        store = app.state.store
        pending = [
            {"artifact": a, "capabilities": caps}
            for a, caps in artifacts_needing_eval(store, fleet=app.state.fleet)
        ]
        return {
            "scale_version": SCALE_VERSION,
            "needs_eval": pending,
            "batches": [
                {"artifact": r["artifact"], "task_class": r["task_class"],
                 "generation": r["generation"], "pending": r["pending"],
                 "scored": r["scored"], "failed": r["failed"], "passed": r["passed"]}
                for r in store.eval_runs_summary()
            ],
        }

    @app.post("/eval/run")
    async def run_eval() -> dict:
        """Trigger a harness pass now instead of waiting for the coordinator's cadence."""
        collected, enqueued = await eval_tick(
            app.state.store, app.state.queue, now=_now_iso(), fleet=app.state.fleet
        )
        return {"scored": collected, "dispatched": enqueued}

    # --- performance load tests (Performance page) ---

    @app.post("/perf/runs", status_code=201)
    async def start_perf_run(body: PerfRunSubmit) -> JSONResponse:
        try:
            run_id = start_run(app, body)
        except UnknownCategory as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        return JSONResponse(status_code=201, content={"id": run_id, "status": "running"})

    @app.get("/perf/runs")
    async def list_perf_runs() -> dict:
        return {"runs": [perf_run_list_view(app.state.store, r)
                          for r in app.state.store.list_perf_runs()]}

    @app.get("/perf/runs/{run_id}")
    async def get_perf_run(run_id: str) -> dict:
        row = app.state.store.get_perf_run(run_id)
        if row is None:
            raise HTTPException(status_code=404, detail="unknown perf run id")
        return perf_run_view(app.state.store, row)

    @app.post("/perf/runs/{run_id}/cancel")
    async def cancel_perf_run(run_id: str) -> dict:
        row = app.state.store.get_perf_run(run_id)
        if row is None:
            raise HTTPException(status_code=404, detail="unknown perf run id")
        task = app.state.perf_tasks.get(run_id)
        if task is not None and not task.done():
            task.cancel()
            return {"id": run_id, "status": "cancelling"}
        return {"id": run_id, "status": row.status}

    @app.get("/queues")
    async def get_queues() -> dict:
        """Per-capability queue depth / pending / live consumers."""
        fleet = app.state.fleet
        out = []
        for cap in (fleet.capabilities if fleet else {}):
            stats = await app.state.queue.depth(cap, settings.consumer_group)
            out.append({"capability": cap, "queue": f"q:{cap}", **stats})
        return {"queues": out}

    @app.post("/jobs", status_code=202)
    async def submit_job(
        body: JobSubmit,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        key = _validated_idempotency_key(idempotency_key)

        # A reservation, if named, must exist and be confirmed. It does NOT override
        # addressing (the job addresses normally); it's recorded as the linkage (§8).
        if body.reservation is not None:
            rsv = app.state.store.get_reservation(body.reservation)
            if rsv is None or rsv.status != "confirmed":
                raise CbkError("reservation_invalid", "unknown or unconfirmed reservation",
                               400, param="reservation")

        try:
            selection = resolve(
                app.state.fleet, app.state.store,
                capability=body.capability,
                task_class=body.task_class,
                min_ability=body.min_ability,
                requires=body.requires.asked_for() if body.requires else None,
                privacy=body.privacy.value,
                urgency=body.urgency.value,
                cloud_budget_monthly=settings.cloud_budget_monthly,
                cloud_budget_reserve_fraction=settings.cloud_budget_reserve_fraction,
            )
        except RoutingRefusal as e:
            # Explicit failure beats silently serving below the requested ability floor,
            # or on a model that cannot do the thing at all. Both are 422 — the request
            # is well-formed and unservable — but they are raised separately so the
            # message names the right fix: a missing DECLARATION is usually one
            # `POST /catalog` away, whereas a missing ability needs a better model.
            #
            # The status stays 422 for every refusal here, as documented; the code is
            # what tells them apart.
            raise CbkError(e.code, str(e), 422, reason=e.reason,
                           capability=body.capability) from e
        capability = selection.capability
        if selection.eta_s is not None:
            # Speed and idleness now decide between tiers that all clear the bar; without
            # this line the choice is invisible after the fact.
            _log.info("job routed to %s (est. %.0fs to answer) for %s/%s",
                      capability, selection.eta_s, body.task_class, body.min_ability)

        # PIN THE ARTIFACT ROUTING CHOSE. A capability is only a queue name; the worker
        # that drains it answers with its own CBK_MODEL, for every capability it serves.
        # So the model whose ability cleared the bar and the model that ran the job were
        # unrelated, and a node serving two tiers with one model answered both at whatever
        # quality that implies, reported as having met the floor. The executors (worker and
        # cloud alike) already honour this pin — the eval harness has relied on it from the
        # start, precisely so a measurement is attributed to the right artifact.
        #
        # It overwrites a client-supplied `params.model` deliberately: otherwise any caller
        # could name a stronger model on a cheaper tier and `min_ability` would enforce
        # nothing at all.
        params = dict(body.params or {})
        params["model"] = selection.artifact

        if selection.on_a_guess:
            # Served on an anchored placeholder, not a measurement. The job still runs —
            # seeds exist so a fresh fleet routes at all — but this is the one place that
            # knows, and saying nothing is how a guess comes to look like evidence.
            _log.info(
                "job routed on a SEEDED ability for %s/%s (score %s) — not yet measured",
                selection.artifact, body.task_class, selection.score)
        job_id, result_key = new_ids()
        now = datetime.now(UTC)
        created_at = now.isoformat().replace("+00:00", "Z")

        record = JobRecord(
            id=job_id,
            created_at=created_at,
            capability=capability,
            messages=body.messages,
            prompt=body.prompt,
            params=params,
            urgency=body.urgency,
            escalate_after_min=body.escalate_after_min,
            privacy=body.privacy,
            deadline=body.deadline,
            # Rides to the worker because `json_schema` is what licenses forwarding
            # `params.response_format`: routing has now checked the pinned artifact can
            # honour it, which is the check that was missing when the worker dropped the
            # field unconditionally.
            requires=body.requires,
            # Carried to the executor so it can refuse rather than under-serve: the
            # coordinator knows which nodes serve a capability, but only the node knows how
            # fast it is right now, with this model loaded and the owner's machine as busy
            # as it happens to be.
            result_key=result_key,
            submitter=body.submitter,
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
            except ValueError as e:
                # Fail loudly. Silently dropping an unparseable deadline to "no expiry"
                # gave a client the opposite of what it asked for — an unbounded job — and
                # told it nothing, so the mistake was undiscoverable from the outside.
                # Consistent with how both addressing forms already 422 at submit rather
                # than queueing work nothing will honour.
                raise CbkError(
                    "invalid_request",
                    f"deadline is not an RFC 3339 timestamp: {body.deadline!r}", 422,
                    param="deadline",
                ) from e

        sub = body.submitter
        # Deliberately after the 400/422 checks above: a rejected request must not burn
        # the key. A client whose submit failed because no artifact cleared the ability
        # bar has to be able to retry the same key once the fleet gains one.
        existing = app.state.store.insert(
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
            submitter_app=sub.app if sub else None,
            submitter_instance=sub.instance if sub else None,
            submitter_request_id=sub.request_id if sub else None,
            submitted_at=sub.submitted_at if sub else None,
            # Stamped here, never taken from the body: a client can misreport its own
            # name but not the address it dialled from. This is what attributes a flood
            # from a client that sends no provenance at all.
            observed_ip=request.client.host if request.client else None,
            idempotency_key=key,
        )
        if existing is not None:
            # Somebody already submitted this logical call. Nothing was written, and
            # nothing is enqueued: hand back the job that did win.
            #
            # 200 rather than 202 (nothing was accepted for processing this time) and
            # rather than 409 (the client asked for at-most-once and got exactly that —
            # this is success). Same keys as the 202, so a client that checks neither the
            # status code nor the header still parses the reply and polls the right job.
            return JSONResponse(
                status_code=200,
                content={
                    "id": existing.id,
                    "result_key": existing.result_key,
                    "status": existing.status,
                },
                headers={"Idempotency-Replayed": "true"},
            )
        # Which stream this job goes on is decided by urgency (ADR 34), but only once
        # every node serving the capability demonstrably reads the urgent tier — an older
        # worker reads the base stream only, and an urgent-tier write would strand the
        # job. The gate is a scan of a table with a handful of rows; if it ever shows up
        # in a profile, recompute it on heartbeat rather than caching it here.
        tier = None
        if tiering_ready(app.state.store, app.state.fleet, capability,
                         mode=settings.urgent_streams):
            tier = tier_for(body.urgency.value)
        entry_id = await app.state.queue.enqueue(record.to_wire(), tier=tier)
        # The row was inserted before the XADD, so the delivery is recorded here. This is
        # what makes queue position exact rather than a stream scan.
        app.state.store.record_delivery(
            job_id, stream=stream_key(capability, tier), entry_id=entry_id
        )

        # Wake rights: urgent/necessary jobs may create capacity on submit.
        if body.urgency in _WAKE_RIGHTS:
            await app.state.wake.maybe_wake(capability, reason=f"submit:{body.urgency.value}")

        return JSONResponse(
            status_code=202,
            content={"id": job_id, "result_key": result_key, "status": "queued"},
        )

    def _submitter_view(row) -> dict | None:
        """Caller provenance for a job. `observed_ip` is server-stamped and the rest
        client-supplied — the split is what matters when attributing a burst of
        identical jobs, since only the client-supplied half can be misreported.

        None only for jobs recorded before provenance existed (pre-0004 rows): an
        address is stamped on every HTTP submit, so a live job always has a view."""
        view = {
            "app": row.submitter_app,
            "instance": row.submitter_instance,
            "request_id": row.submitter_request_id,
            "submitted_at": row.submitted_at,
            "observed_ip": row.observed_ip,
        }
        return view if any(v is not None for v in view.values()) else None

    def _timing_view(row, result: dict | None = None) -> dict:
        """When this job was received, started, and finished.

        `created_at` is server-stamped at receipt and authoritative.

        For `started_at`/`finished_at` the executor's own measurements win when present:
        it brackets the model call itself, so its numbers are exact, whereas the
        coordinator's are observations made on a tick and can miss a job entirely (one
        claimed and acked between two ticks never appears in the pending list). Falling
        back to the observed values is what keeps a *running* job's start visible, since
        no result exists yet.

        A null `started_at` therefore means "not measured and no claim observed" — never
        "not started". A client's wait state keys on `status` and `queue_position`.
        """
        result = result or {}
        return {
            "created_at": row.created_at,
            "started_at": result.get("started_at") or row.started_at,
            "finished_at": result.get("finished_at") or row.finished_at,
        }

    def _limits_view(row) -> dict:
        """When, if ever, clusterbuck stops trying.

        A client cannot otherwise tell "slow but healthy" from "will never be served",
        and had no principled point at which to stop waiting. `expires_at` is the
        effective give-up time; **null means nothing will ever give up on this job**,
        which is the honest answer for a `waitable` job submitted with neither a
        `deadline` nor an `escalate_after_min` (see protocols.md §1b).
        """
        deadline = _now_iso_at(row.deadline_epoch) if row.deadline_epoch else None

        # The max-queue-age backstop is the OTHER thing that gives up on a job, and it
        # was missing from this view: with `CBK_MAX_QUEUE_AGE_S` set, a client polling a
        # job the coordinator was about to expire was told `expires_at: null`, i.e. that
        # nothing would ever give up on it. Exactly backwards, and the one case where
        # this field lied rather than merely being unhelpful.
        #
        # Bounded to `queued` because that is what the sweep selects
        # (`Store.stale_queued_jobs`): a claimed job is not aged out, so once it is in
        # flight the deadline is again the only bound.
        backstop = None
        if settings.max_queue_age_s is not None and row.status == "queued":
            started = datetime.fromisoformat(row.created_at.replace("Z", "+00:00"))
            backstop = _now_iso_at(started.timestamp() + settings.max_queue_age_s)

        # Whichever gives up first. ISO-8601 UTC sorts lexicographically, which is why
        # both are already normalised to a trailing Z.
        bounds = [b for b in (deadline, backstop) if b is not None]
        return {
            "deadline": deadline,
            "escalates_at": _now_iso_at(row.escalate_at) if row.escalate_at else None,
            "expires_at": min(bounds) if bounds else None,
        }

    async def _queue_position(row) -> int | None:
        """How many never-delivered jobs sit ahead of this one, or None.

        Counts *unclaimed* work only: anything already claimed is in flight, not ahead in
        the queue, so a position of 0 is compatible with one job still generating. None
        once the job is no longer queued. Capped, so a deep queue does not make every poll
        read thousands of prompts — past the cap the number means "at least".
        """
        if row.status != "queued" or row.entry_id is None:
            return None
        # On the tier the entry is ACTUALLY on. Omitting this counted an urgent job's
        # position against the base stream: `undelivered` defaults to `tier=None`, so both
        # its XINFO GROUPS and its XRANGE read `q:<cap>` while `entry_id` had been minted
        # on `q:<cap>:urgent`. Stream ids are millisecond timestamps, so the comparison
        # never raised — it silently reported unrelated base-tier entries as being ahead
        # of the job that had just jumped that queue.
        ahead, _capped = await app.state.queue.undelivered(
            row.capability, settings.consumer_group, before_entry_id=row.entry_id,
            tier=tier_of(row.stream),
        )
        return ahead

    def _error_code(status: str, result: dict | None) -> str | None:
        """Why a job did not complete, as a code a client can branch on (protocols §1c).

        `error` stays the free-text string it always was, so no poller breaks; this sits
        beside it. The code comes from the result when its writer set one. Otherwise it is
        derived: the coordinator's own terminal states (a deadline expiry, a cancellation)
        write no result at all, and a worker too old to classify its failure still failed.

        A `failed` job whose result has passed CBK_RESULT_TTL_S answers null, like its
        `result`: the reason lived in the blob. Keeping it in SQLite would need a column
        for something a client must persist on first sight anyway.
        """
        if result is not None and result.get("error_code"):
            return result["error_code"]
        if status == "expired":
            return "job_expired"
        if status == "cancelled":
            return "job_cancelled"
        if status == "failed" and result is not None:
            return "worker_failed"
        return None

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str) -> dict:
        row = app.state.store.get(job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="unknown job id")

        result = await app.state.queue.read_result(row.result_key)
        if row.status in _SEALED_STATUSES and (result or {}).get("status") != row.status:
            # The coordinator gave up on this job. A result that lands afterwards comes
            # from a worker that had already claimed it and ran on regardless; it must not
            # turn `expired` into `done`, or `cancelled` into anything, after the client
            # was told to stop waiting. Terminal has to mean terminal. (A blob that AGREES
            # — the backstop's own `expired`, carrying its reason — is kept.)
            result = None
        if result is None:
            # No terminal result yet — report the queue-state we last recorded.
            return {
                "id": job_id,
                "status": row.status,
                "urgency": row.urgency,  # reflects escalation (waitable → necessary)
                "capability": row.capability,
                **_timing_view(row),
                **_limits_view(row),
                "queue_position": await _queue_position(row),
                "result": None,
                "usage": None,
                "error": None,
                "error_code": _error_code(row.status, None),
                "attempts": row.attempts,
                # From the pending list, so a running job finally has an answer to "has
                # anything picked this up" — the result blob cannot say until it is over.
                "worker": row.claimed_by,
                "submitter": _submitter_view(row),
            }

        status = result.get("status", "done")
        if status in RESULT_STATUSES and row.status != status:
            app.state.store.set_status(job_id, status)
            row = app.state.store.get(job_id) or row  # pick up finished_at

        return {
            "id": job_id,
            "status": status,
            "urgency": row.urgency,
            "capability": row.capability,
            **_timing_view(row, result),
            **_limits_view(row),
            "queue_position": None,  # terminal: nothing is ahead of it any more
            "result": result.get("completion"),
            "usage": result.get("usage"),
            "error": result.get("error"),
            "error_code": _error_code(status, result),
            "attempts": row.attempts,
            "worker": result.get("worker") or row.claimed_by,
            "submitter": _submitter_view(row),
        }

    @app.delete("/jobs/{job_id}")
    async def cancel_job(job_id: str) -> dict:
        """Withdraw a job. Best-effort by construction, and honest about which.

        `cancelled` is reported ONLY when the work provably never ran and never will.
        Otherwise the verdict is `cancelling`: a worker already holds the entry and no
        amount of coordinator-side bookkeeping can interrupt a model call, so claiming
        otherwise would be a lie. (`/perf/runs/{id}/cancel` already sets that precedent.)
        Either way the client stops waiting, which is the half we can always deliver.

        Any holder of the operator secret can cancel any job: `client_key` is a client
        scoping, not an authenticated identity, and ADR 26 deliberately declined
        per-client keys for LAN-only single-operator infrastructure. Accepted limitation,
        not an oversight.
        """
        row = app.state.store.get(job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="unknown job id")

        # The result blob is the first gate, not the stream. XACK leaves an entry in
        # place, so a job that already ran is indistinguishable from one never delivered
        # by looking at the stream alone.
        result = await app.state.queue.read_result(row.result_key)
        if result is not None or row.status in TERMINAL_STATUSES:
            return await get_job(job_id)  # already over; nothing to withdraw

        verdict = "cancelling"
        if row.entry_id and row.stream:
            outcome = await app.state.queue.withdraw(
                row.stream, row.entry_id, group=settings.consumer_group
            )
            if outcome == "deleted":
                verdict = "cancelled"
        # else: a row from before deliveries were recorded, or the window between the
        # insert and the XADD — nothing provable either way.

        if verdict == "cancelled":
            # Both halves, mirroring the expiry sweep: without the usage row,
            # `jobs_awaiting_usage` re-selects this job on every coordinator tick forever.
            app.state.store.set_status(job_id, "cancelled")
            app.state.store.record_usage(
                job_id=job_id, ts=_now_iso(), capability=row.capability, model=None,
                node=None, venue=venue_of(app.state.fleet, row.capability),
                tokens_in=0, tokens_out=0, outcome="cancelled", cost=0.0,
                day=datetime.now(UTC).strftime("%Y-%m-%d"),
            )
        else:
            # Flag it and floor the deadline so the expiry sweep terminalises it even if
            # the worker holding it never comes back (Redis drops the pending entry for a
            # deleted stream entry, so the reaper cannot).
            app.state.store.request_cancel(job_id)

        view = await get_job(job_id)
        view["status"] = verdict if verdict == "cancelling" else view["status"]
        return view

    def _reservation_view(row) -> dict:
        plan = None
        if row.status == "confirmed":
            plan = {
                "node": row.node,
                "artifact": row.artifact,
                "warm_by": iso(row.warm_by),
                "starts": iso(row.starts),
                "ends": iso(row.ends),
            }
        return {
            "id": row.id,
            "status": row.status,
            "state": row.state,
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
            privacy=body.privacy.value,
            # None ⇒ reservations.admit derives it from measured cold-load time,
            # falling back to DEFAULT_WARM_LEAD_S when the fleet has measured nothing.
            lead_s=settings.warm_lead_s,
        )
        rsv_id = new_reservation_id()
        created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
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

    def _bootstrap_password() -> str | None:
        """The configured join password, or None if bootstrap must stay disabled.

        Fails closed on the FEATURE, not on the service: a too-short password leaves the
        routes 404 and logs why, rather than refusing to start the coordinator (which
        would turn a weak env var into an outage on the next restart).
        """
        pw = join_password if join_password is not None else settings.join_password
        if not pw:
            return None
        if len(pw) < JOIN_PASSWORD_MIN_LEN:
            _log.warning(
                "CBK_JOIN_PASSWORD is shorter than %d characters — worker bootstrap is "
                "DISABLED. It guards the broker credential, so a guessable value would be "
                "equivalent to publishing that credential on the LAN.",
                JOIN_PASSWORD_MIN_LEN,
            )
            return None
        return pw

    def _artifact_path() -> Path | None:
        """The blessed `cbk.pyz`, or None if none is configured or it is missing.

        Shared by `/worker/artifact`, which serves it, and `/nodes/bootstrap`, which
        publishes its digest — so the digest a joiner checks always describes the bytes
        that same coordinator will hand it.
        """
        path = (worker_artifact if worker_artifact is not None
                else settings.worker_artifact)
        return Path(path) if path and Path(path).is_file() else None

    def _require_join_password(presented: str | None) -> None:
        """Gate for the two bootstrap routes. 404 when unconfigured, 401 when wrong.

        404 rather than 401 for the unconfigured case so a coordinator that has not opted
        in does not advertise that the feature exists.
        """
        configured = _bootstrap_password()
        if configured is None:
            raise HTTPException(status_code=404, detail="worker bootstrap is not configured")
        if not key_matches(presented, configured):
            raise HTTPException(status_code=401, detail="missing or invalid join password")

    @app.post("/nodes/bootstrap", status_code=201)
    async def bootstrap_worker(
        request: Request,
        x_cbk_join_password: str | None = Header(default=None),
    ) -> dict:
        """Everything a joining machine needs, in exchange for the join password.

        The point of this route is that the OPERATOR KEY NEVER LEAVES THE COORDINATOR. A
        worker has no business holding it — it mints tokens, approves model installs and
        deletes models — so a joining machine presents a join password once and gets back
        a single-use join token plus the broker URL. It then enrolls normally
        (`POST /nodes/enroll`) and thereafter authenticates with its own per-node key.

        `capabilities` is what the coordinator's registry currently knows, so the joining
        script can warn when a node is about to serve a tier that is not in it — the
        failure where enrolment succeeds, heartbeats look healthy, and no job ever routes.
        """
        _require_join_password(x_cbk_join_password)

        # A joining worker is remote by definition, so a loopback broker address points it
        # at its OWN localhost. That failure is invisible at join time — install, enrolment
        # and service start all succeed, and only then does the worker find no broker — so
        # refuse to hand one out rather than serving something that cannot work.
        advertised = (broker_advertise_url or settings.broker_advertise_url
                      or redis_url or settings.redis_url)
        if urlsplit(advertised).hostname in _LOOPBACK_HOSTS:
            raise HTTPException(
                status_code=503,
                detail=(
                    "refusing to advertise a loopback broker address to a remote worker. "
                    "Redis runs on the coordinator, so its own CBK_REDIS_URL is loopback; "
                    "set CBK_BROKER_ADVERTISE_URL to the address workers should dial "
                    "(e.g. redis://:<password>@<coordinator-lan-ip>:6379/0)."
                ),
            )

        # Validate BEFORE minting: otherwise every failed attempt burns a token row and an
        # unauthenticated caller can grow the table at will.
        token = new_join_token()
        app.state.store.mint_token(token, _now_iso())
        _log.info("bootstrap: issued a join token to %s",
                  request.client.host if request.client else "?")

        fleet = app.state.fleet
        body = {
            "join_token": token,
            "redis_url": advertised,
            "consumer_group": settings.consumer_group,
            "capabilities": sorted(fleet.capabilities) if fleet else [],
        }
        # What the joiner needs to check the artifact it is about to install as a service.
        #
        # `join.py` used to accept whatever `/worker/artifact` returned, verifying only
        # that it was non-empty — while the UPDATE channel that patches the same binary
        # afterwards verifies an ECDSA signature and a digest before writing a byte. The
        # bootstrap is the step that installs the first copy, so it was the weaker half of
        # a chain whose strong half nobody could reach without passing through it.
        #
        # Two levels, because they defend different things. The digest is always present
        # and catches a corrupted download or a swapped file on disk. The signature is
        # present only when an update signing key is configured, and is the real defence:
        # verified against a public key the operator moved out of band, it survives an
        # attacker who controls the whole channel, which a digest served over that same
        # channel cannot.
        artifact = _artifact_path()
        if artifact is not None:
            digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
            version = settings.worker_current_version or ""
            body["artifact_sha256"] = digest
            key_path = app.state.update_signing_key
            if key_path and Path(key_path).is_file():
                body["artifact_signature"] = sign_bootstrap(
                    load_private_pem(Path(key_path).read_text()),
                    sha256=digest, version=version)
                body["artifact_version"] = version
                # So the node can verify its own updates from here on. Without it
                # `update.py` refuses every manifest — correct, but it left the patch
                # channel permanently inert on every node built the documented way.
                body["update_public_key"] = public_pem(
                    load_private_pem(Path(key_path).read_text()))
        return body

    @app.get("/releases/{filename}")
    async def serve_release_artifact(filename: str) -> FileResponse:
        """A signed release artifact, for a worker updating itself (ADR 13/38).

        UNAUTHENTICATED, deliberately, and this is not the same judgement as
        `/worker/artifact` below. That one is the bootstrap path for a machine with no
        identity yet, gated by the join password its installer already holds — gating costs
        nothing there. This one is fetched by a worker already in the field, which holds a
        node key and must never be given the operator key. Requiring a credential would
        break every worker already deployed, which is precisely the fleet auto-update
        exists to serve.

        It buys nothing, either: the worker verifies an ECDSA signature over
        (version, rid, sha256, channel, url, protocol_version) and then the digest of what
        it downloaded, both before writing a byte. The channel is untrusted by design — an
        attacker who can serve this file still cannot make a worker install it.

        Only files named by the CURRENT release manifest are served, matched on basename.
        That is an allowlist, not a path join: `filename` never reaches the filesystem as a
        path, so `..` and absolute paths cannot escape the release directory, and the
        coordinator cannot be made to serve a file the operator did not bless.
        """
        release_path = app.state.update_release
        if not release_path:
            raise HTTPException(status_code=404, detail="no release channel configured")
        try:
            release = json.loads(Path(release_path).read_text())
            blessed = {
                Path(urlsplit(a["url"]).path).name
                for a in (release.get("artifacts") or {}).values() if a.get("url")
            }
        except Exception:
            _log.exception("could not read the release manifest at %s", release_path)
            raise HTTPException(status_code=404, detail="release manifest unreadable") from None
        if filename not in blessed:
            raise HTTPException(status_code=404, detail="not a released artifact")
        path = Path(release_path).parent / filename
        if not path.is_file():
            raise HTTPException(
                status_code=404,
                detail=f"{filename} is in the release manifest but missing on disk")
        return FileResponse(path, media_type="application/octet-stream",
                            filename=filename)

    @app.get("/worker/artifact")
    async def serve_worker_artifact(
        x_cbk_join_password: str | None = Header(default=None),
    ) -> FileResponse:
        """The blessed `cbk.pyz`, so every node runs the same build.

        Behind the join password even though the artifact is not secret — it is public
        Apache-2.0 code. That is not a claim of secrecy: the joining script holds the
        password anyway, so gating costs nothing, and it keeps the invariant simple —
        this coordinator serves files to no unauthenticated caller. Do not "fix" this to
        open on the grounds that the code is public.
        """
        _require_join_password(x_cbk_join_password)
        path = _artifact_path()
        if path is None:
            raise HTTPException(
                status_code=404,
                detail="no worker artifact configured (set CBK_WORKER_ARTIFACT)",
            )
        return FileResponse(str(path), media_type="application/octet-stream",
                            filename="cbk.pyz")

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
        caps, ladder = propose_capabilities(
            body.hw.ram_gb, body.hw.accelerator, body.hw.vram_gb)
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
        # Constant-time, for the reason auth.py:key_matches states about the operator
        # key: a plain `!=` leaks the key through response timing. The per-node key had
        # been left on `!=` while the operator key and the join password both used
        # compare_digest.
        if not key_matches(x_cbk_node_key, node.node_key):
            raise HTTPException(status_code=401, detail="bad node key")
        now = _now_iso()
        store = app.state.store

        # Is this build fit to run jobs? Protocol compatibility is necessary but NOT
        # sufficient — a worker can speak the contract correctly and still carry bugs that
        # produce plausible-looking wrong results, so the coordinator judges the reported
        # build version too (ADR 27).
        fitness = assess(
            policy_from_settings(settings),
            agent_version=body.agent_version,
            protocol_version=body.protocol_version,
        )
        if fitness.status != "ok":
            _log.warning("node %s is %s: %s", node_id, fitness.status, fitness.reason)

        store.record_heartbeat(
            node_id=node_id, mode=body.mode,
            installed=json.dumps(body.installed), loaded=json.dumps(body.loaded),
            queues=json.dumps(body.queues),
            jobs_done=body.stats.get("jobs_done"), tps=body.stats.get("tps"),
            load_s=body.stats.get("load_s"),
            last_heartbeat=now,
            agent_version=body.agent_version,
            agent_flavour=body.agent_flavour,
            protocol_version=body.protocol_version,
            fitness=fitness.status, fitness_reason=fitness.reason,
        )

        # Record observed artifacts + digests; a changed digest is a NEW artifact whose
        # stored ability is stale, so it earns a re-eval proposal (ADR 15).
        observed = {a: (body.digests or {}).get(a) for a in body.installed}
        notes: list[str] = []
        for artifact, old, new in store.observe_node_models(node_id, observed, now):
            # Fleet-wide, on one node's say-so, and deliberately left that way — but said
            # out loud, because it is the most destructive thing a heartbeat can cause.
            #
            # Ability belongs to the ARTIFACT, not to the node hosting it, so a genuine
            # upstream change really does stale every score for that name. The trust
            # assumption is that `installed`/`digests` are honest: an enrolled node can
            # drop the measured scores for an artifact it does not run, and routing then
            # cannot clear `min_ability` until the re-eval completes. ADR 26's
            # single-operator model accepts that — enrolling a node is already an act of
            # trust — but an operator watching the log should see it happen rather than
            # discover it as an unexplained 422 storm.
            #
            # Gating on "is this artifact known to the catalog or the matrix" was tried
            # and is worse than nothing: an unknown artifact has no scores to delete, so
            # the check blocks only the harmless case and waves through the harmful one.
            _log.warning(
                "node %s reports %s changed upstream (%s -> %s) — dropping its measured "
                "ability fleet-wide and re-queuing it for evaluation",
                node_id, artifact, old, new)
            # The artifact changed upstream, so it is a NEW artifact (ADR 15): it inherits
            # neither the stored score nor the measurements behind it. Merely raising a
            # proposal left the stale score driving routing forever, because nothing consumed
            # reeval proposals; dropping the score alone still let the next batch average the
            # new artifact's items with the old one's.
            store.supersede_artifact(artifact, SCALE_VERSION, now=now)
            if propose_reeval(store, node_id, artifact, old, new, now=now):
                notes.append(f"{artifact} changed upstream — re-evaluation proposed")

        # Does what this node ADVERTISES match what it can actually run? The registry's
        # `model:` is what clears a job's min_ability bar; the worker answers with its own
        # CBK_MODEL, for every tier it serves. Nothing reconciled the two, so a node
        # serving a tier whose model it hasn't got answers those jobs with something else
        # and reports success.
        #
        # Logged for the operator, NOT returned to the worker: this is a coordinator-side
        # configuration fact the worker can do nothing about, and it would repeat on every
        # heartbeat. `/nodes` carries the same list for checking the whole fleet at once.
        # Logged only when it CHANGES, for the same reason — a warning repeated every few
        # seconds is one nobody reads.
        mismatches = unservable_capabilities(
            app.state.fleet, json.loads(store.get_node(node_id).capabilities or "[]"),
            body.installed)
        if app.state.capability_warnings.get(node_id) != mismatches:
            app.state.capability_warnings[node_id] = mismatches
            for warning in mismatches:
                _log.warning("node %s %s", node_id, warning)

        # Close the loop on any action the previous response issued.
        if body.action_result is not None:
            notes += apply_action_result(store, node_id, body.action_result, now=now)

        # Hand out the next approved action, if one is permitted in this presence mode.
        action = next_action(store, store.get_node(node_id), mode=body.mode)

        # Offer a signed update when this node opted in and is not already current. The
        # worker verifies the signature against its pinned key before touching a byte;
        # without auto_update the operator updates it by hand and this stays null (ADR 13).
        update = None
        if fitness.status in ("stale", "quarantine") and bool(node.auto_update):
            update = _build_update_for(app, node, flavour=body.agent_flavour)
            if update is not None:
                notes.append(
                    f"update offered: {update['version']} for {update['rid']}")

        if fitness.status == "quarantine":
            action = None   # an unfit build installs nothing

        return {"update": update, "action": action, "fitness": fitness.to_wire(),
                "planner_notes": notes}

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
            {"artifact": r.artifact, "family": r.family, "params_b": r.params_b,
             "quant": r.quant, "size_gb": r.size_gb, "min_ram_gb": r.min_ram_gb,
             "source": r.source, "registry_ref": r.registry_ref,
             "expected_ability": r.expected_ability,
             # What the artifact CAN DO, as opposed to how well (ADR 37). null means not
             # curated — a job requiring it is refused, naming this artifact, rather than
             # served by a model nobody has checked.
             "context_tokens": r.context_tokens,
             "supports_tools": r.supports_tools,
             "supports_json_schema": r.supports_json_schema,
             "supports_vision": r.supports_vision}
            for r in app.state.store.list_catalog()]}

    @app.post("/catalog", status_code=201)
    async def post_catalog(entry: CatalogEntrySubmit) -> dict:
        """Add or update a catalog candidate.

        Upsert by `artifact`, so correcting a wrong size_gb is a re-POST rather than a
        delete-and-recreate. Editing an entry never touches measured ability: that is held
        per artifact+digest and is only ever earned by evaluation (ADR 15), so curating the
        catalog cannot promote anything into routing on its own.
        """
        app.state.store.upsert_catalog(added_at=_now_iso(), **entry.model_dump())
        return {"artifact": entry.artifact}

    @app.get("/proposals")
    async def get_proposals(status: str | None = None) -> dict:
        return {"proposals": [
            {"id": r.id, "kind": r.kind, "node_id": r.node_id,
             "artifact": r.artifact, "task_class": r.task_class,
             "rationale": r.rationale, "status": r.status,
             "created_at": r.created_at, "decided_at": r.decided_at}
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
        return {"id": row.id, "status": row.status, "decided_at": row.decided_at}

    @app.post("/nodes/{node_id}/policy")
    async def set_node_policy(node_id: str, body: NodePolicy) -> dict:
        """The owner's contract for this node: storage quota and whether installs may be
        applied without a human decision (opt-in, off by default).

        Takes a JSON body deliberately: as bare scalars these bound as *query parameters*,
        which made flipping the human-approval gate a one-line GET-shaped request.
        """
        disk_quota_gb, auto_approve = body.disk_quota_gb, body.auto_approve
        store = app.state.store
        node = store.get_node(node_id)
        if node is None:
            raise HTTPException(status_code=404, detail="unknown node")
        store.set_node_flags(node_id, disk_quota_gb=disk_quota_gb, auto_approve=auto_approve,
                             auto_update=body.auto_update)
        node = store.get_node(node_id)
        return {"node_id": node_id,
                "disk_quota_gb": quota_for(node.profile, node.disk_quota_gb),
                "auto_approve": bool(node.auto_approve),
                "auto_update": bool(node.auto_update)}

    @app.get("/nodes")
    async def list_nodes() -> dict:
        def view(n) -> dict:  # never expose node_key
            return {
                "node_id": n.node_id, "hostname": n.hostname,
                "os": n.os, "arch": n.arch, "profile": n.profile,
                "ram_gb": n.ram_gb, "accelerator": n.accelerator,
                # VRAM, not RAM, is what decides whether a model runs at device speed.
                # null on a CPU node, or where it could not be measured — which is NOT the
                # same as zero, and the fits gate treats the two differently.
                "vram_gb": n.vram_gb,
                # MEASURED on this machine from real jobs (heartbeat `stats.tps`), not
                # probed and not declared. Ability scores an artifact; this scores the
                # pairing — the same model is identical on ability and nothing alike in
                # speed on a GPU box versus a CPU one. null until the node finishes a job.
                "tps": n.tps, "jobs_done": n.jobs_done,
                # Measured seconds to bring a model up from cold — what the reservation
                # pre-warm lead is computed from, in place of a hardcoded five minutes.
                # null where the model server cannot report what is resident.
                "load_s": n.load_s,
                "capabilities": json.loads(n.capabilities or "[]"),
                # Tiers this node advertises but cannot actually honour, because the
                # registry's model for them is not among its installed artifacts. Empty is
                # the healthy case. This is the failure that looks fine from every other
                # angle: enrolment succeeds, heartbeats are green, jobs are answered — by
                # the wrong model.
                "capability_warnings": unservable_capabilities(
                    app.state.fleet,
                    json.loads(n.capabilities or "[]"),
                    json.loads(n.installed or "[]"),
                ),
                "mode": n.mode,
                # Observed from the node's model server, not configured (M6a).
                "installed": json.loads(n.installed or "[]"),
                "loaded": json.loads(n.loaded or "[]"),
                "last_heartbeat": n.last_heartbeat,
                # Version governance (ADR 27): what it runs and whether that is acceptable.
                "agent_version": n.agent_version,
                # Which runtime the node runs — decides which release artifact it can execute.
                # Absent rows predate the field; every worker is a Python one.
                "agent_flavour": n.agent_flavour or "python",
                "protocol_version": n.protocol_version,
                "fitness": n.fitness or "unknown",
                "fitness_reason": n.fitness_reason,
            }
        return {"nodes": [view(n) for n in app.state.store.list_nodes()]}

    return app


app = create_app()
