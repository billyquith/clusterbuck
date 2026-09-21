"""Load-test driver for the Performance dashboard page.

A stream of randomized queries against the coordinator's own real `/jobs` API, sustained
for a period long enough that node models actually warm up, with a per-reply adequacy
check scored the moment each reply lands.

Prompts and checks come from the shared tier-1 suite (`evaluation.SEED_SUITE`), drawn at
random per query — so a load run measures the same artifacts against the same items the
ability matrix does, and `pass_rate` here means what `pass_rate` means there. No LLM
judge: that tier is deferred project-wide, and this does not invent a new exception.

What this module uniquely measures is LOAD: p50/p90 latency, jobs per second, and the
served-vs-unassigned split. Those only exist when queries are driven concurrently for a
sustained period, which nothing else here does. Quality it now borrows rather than
reinvents.

No cloud escalation. Some requests won't be servable by the local fleet at all —
`routing.resolve_capability` already fails explicitly (HTTP 422) rather than silently
under-serving when no local artifact clears the requested `min_ability` (a deliberate,
already-shipped invariant). That's recorded here as an `unassigned` sample, not retried
against anything else.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from .config import settings
from .evaluation import TASK_CLASSES, items_for
from .ids import new_perf_run_id, new_perf_sample_id
from .models import PerfRunSubmit
from .orm.perf_run import PerfRun
from .orm.perf_sample import PerfSample
from .store import Store

_log = logging.getLogger("clusterbuck.perf")

Check = Callable[[str], tuple[bool, str]]


@dataclass(frozen=True)
class GeneratedQuery:
    category: str
    task_class: str
    min_ability: int
    prompt: str
    check: Check


Generator = Callable[[random.Random], GeneratedQuery]


def _wrap(check_fn: Callable[[str], bool], ok_detail: str, fail_detail: str) -> Check:
    def run(reply: str) -> tuple[bool, str]:
        ok = check_fn(reply or "")
        return ok, ok_detail if ok else fail_detail
    return run


# --- queries come from the shared tier-1 suite ------------------------------------------
#
# These used to be seven bespoke generators with their own prompts, their own checkers
# (self-described in the code as "deliberately approximate" and "explicitly a weak proxy")
# and their own hardcoded ability floors. That made a THIRD notion of "did the model get
# this right", alongside the ability matrix and node throughput — and the weakest of the
# three, measuring nothing the others measured and feeding nothing.
#
# The load driver is worth keeping: p50/p90 latency, jobs/s and the served-vs-unassigned
# split exist nowhere else, because they only appear when jobs are driven concurrently.
# The private scoring is not. So the run now draws its prompts from `SEED_SUITE` and
# scores them with the same checkers the ability matrix uses, which makes the Performance
# page's `pass_rate` mean the same thing as the matrix's for the same artifact.

# Ability floor per task class. Carried over from the retired generators deliberately: a
# default run has to produce BOTH served and unassigned samples on a fleet whose best
# local artifact sits around 4, which is what deploy/e2e/perf.sh asserts. Flattening these
# to one value would make every sample land the same way and the load test stop showing
# routing gaps at all.
_ABILITY_FLOOR = {"extract": 3, "summarize": 4, "reason": 6, "code": 6}


def _suite_generator(task_class: str) -> Generator:
    """Draw a random item of this task class from the shared suite."""
    items = items_for(task_class)
    if not items:
        raise ValueError(f"no suite items for task class {task_class!r}")

    def generate(rng: random.Random) -> GeneratedQuery:
        item = rng.choice(items)
        return GeneratedQuery(
            category=task_class,
            task_class=task_class,
            min_ability=_ABILITY_FLOOR[task_class],
            prompt=item.prompt,
            check=_wrap(item.check, "ok", f"failed the tier-1 {task_class} check"),
        )
    return generate


# One category per task class now, not seven hand-rolled ones. The name is kept because
# it is the config and UI contract (`PerfConfig.categories`, the form's checkboxes).
CATEGORIES: dict[str, Generator] = {tc: _suite_generator(tc) for tc in TASK_CLASSES}


@dataclass
class PerfConfig:
    label: str = "load test"
    categories: list[str] = field(default_factory=lambda: list(CATEGORIES))
    concurrency: int = 4
    duration_s: float = 60.0
    warmup_s: float = 15.0
    n_jobs: int | None = None  # caps total queries; alternative to duration_s alone
    min_ability_override: int | None = None
    pin_model: str | None = None
    poll_interval_s: float = 0.25
    job_timeout_s: float = 60.0
    seed: int | None = None


class PerfAborted(RuntimeError):
    """A run hit a real infrastructure error (not an ordinary unassigned/failed sample)."""


async def _submit_and_poll(
    client: httpx.AsyncClient, store: Store, *, run_id: str, phase: str,
    gq: GeneratedQuery, min_ability: int, pin_model: str | None,
    poll_interval_s: float, job_timeout_s: float, rng: random.Random,
) -> None:
    submitted_at = time.time()
    body: dict = {
        "task_class": gq.task_class, "min_ability": min_ability,
        "prompt": gq.prompt, "urgency": "waitable",
    }
    if pin_model:
        body["params"] = {"model": pin_model}

    resp = await client.post("/jobs", json=body)

    if resp.status_code == 422:
        store.add_perf_sample(
            id=new_perf_sample_id(), run_id=run_id, job_id=None, category=gq.category,
            task_class=gq.task_class, min_ability=min_ability, capability=None, node=None,
            phase=phase, submitted_at=submitted_at, completed_at=submitted_at,
            latency_s=None, tokens_in=None, tokens_out=None, outcome="unassigned",
            passed=None, detail=resp.json().get("detail"),
        )
        # An unassigned sample has no polling wait at all, so on a fully-unservable
        # config this loop would otherwise spin without ever truly yielding to the event
        # loop — starving other tasks (including a cancel request) of a turn to run.
        await asyncio.sleep(0)
        return

    if resp.status_code != 202:
        raise PerfAborted(f"POST /jobs returned {resp.status_code}: {resp.text[:200]}")

    job_id = resp.json()["id"]
    deadline = time.monotonic() + job_timeout_s
    result: dict | None = None
    while time.monotonic() < deadline:
        await asyncio.sleep(poll_interval_s * (0.7 + 0.6 * rng.random()))
        r = await client.get(f"/jobs/{job_id}")
        if r.status_code != 200:
            raise PerfAborted(f"GET /jobs/{job_id} returned {r.status_code}")
        j = r.json()
        if j["status"] in ("done", "failed", "expired"):
            result = j
            break

    completed_at = time.time()
    if result is None:
        store.add_perf_sample(
            id=new_perf_sample_id(), run_id=run_id, job_id=job_id, category=gq.category,
            task_class=gq.task_class, min_ability=min_ability, capability=None, node=None,
            phase=phase, submitted_at=submitted_at, completed_at=completed_at,
            latency_s=completed_at - submitted_at, tokens_in=None, tokens_out=None,
            outcome="timeout", passed=None, detail="poll timed out",
        )
        return

    usage = result.get("usage") or {}
    passed: bool | None = None
    detail: str | None = None
    if result["status"] == "done":
        content = ((result.get("result") or {}).get("choices") or [{}])[0]
        text = ((content.get("message") or {}).get("content")) or ""
        passed, detail = gq.check(text)

    store.add_perf_sample(
        id=new_perf_sample_id(), run_id=run_id, job_id=job_id, category=gq.category,
        task_class=gq.task_class, min_ability=min_ability,
        capability=result.get("capability"), node=result.get("worker"),
        phase=phase, submitted_at=submitted_at, completed_at=completed_at,
        latency_s=completed_at - submitted_at,
        tokens_in=usage.get("prompt_tokens"), tokens_out=usage.get("completion_tokens"),
        outcome=result["status"], passed=passed, detail=detail,
    )


async def run_perf_test(app, store: Store, run_id: str, config: PerfConfig) -> None:
    """Drive one run to completion (or until cancelled). Writes samples as they land and
    finalizes the run's status when done."""
    rng = random.Random(config.seed)
    headers = {"x-cbk-api-key": settings.api_key} if settings.api_key else {}
    transport = httpx.ASGITransport(app=app)
    started = time.monotonic()
    submitted_count = 0
    lock = asyncio.Lock()

    async def acquire_slot() -> bool:
        """Atomically check both budgets and reserve one slot. False ⇒ stop looping."""
        nonlocal submitted_count
        async with lock:
            if config.n_jobs is not None and submitted_count >= config.n_jobs:
                return False
            if time.monotonic() - started >= config.duration_s:
                return False
            submitted_count += 1
            return True

    async def worker(client: httpx.AsyncClient) -> None:
        while await acquire_slot():
            category = rng.choice(config.categories)
            gq = CATEGORIES[category](rng)
            min_ability = config.min_ability_override or gq.min_ability
            phase = "warmup" if time.monotonic() - started < config.warmup_s else "measure"
            await _submit_and_poll(
                client, store, run_id=run_id, phase=phase, gq=gq,
                min_ability=min_ability, pin_model=config.pin_model,
                poll_interval_s=config.poll_interval_s,
                job_timeout_s=config.job_timeout_s, rng=rng,
            )

    status = "done"
    async with httpx.AsyncClient(
        transport=transport, base_url="http://perf.internal", headers=headers,
        timeout=config.job_timeout_s + 10,
    ) as client:
        try:
            await asyncio.gather(*(worker(client) for _ in range(config.concurrency)))
        except asyncio.CancelledError:
            status = "cancelled"
            raise
        except PerfAborted:
            _log.exception("perf run %s aborted", run_id)
            status = "cancelled"
        except Exception:
            _log.exception("perf run %s crashed", run_id)
            status = "cancelled"
        finally:
            store.finish_perf_run(run_id, status, _now_iso())


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class UnknownCategory(ValueError):
    pass


def _snapshot(app) -> str:
    """What was available when a run started, so later runs can be compared."""
    fleet = app.state.fleet
    caps = (
        {name: {"model": c.model, "cloud": c.cloud}
         for name, c in fleet.capabilities.items()}
        if fleet else {}
    )
    nodes = [
        {"id": n.node_id, "mode": n.mode, "installed": json.loads(n.installed or "[]")}
        for n in app.state.store.list_nodes()
    ]
    return json.dumps({"capabilities": caps, "nodes": nodes})


def start_run(app, body: PerfRunSubmit) -> str:
    """Validate + kick off a run from a submitted config. Returns the new run id.

    Shared by the JSON API (POST /perf/runs) and the dashboard's htmx start-run form, so
    the two surfaces can't drift.
    """
    categories = body.categories or list(CATEGORIES)
    unknown = sorted(set(categories) - set(CATEGORIES))
    if unknown:
        raise UnknownCategory(f"unknown categories: {unknown}")
    config = PerfConfig(
        label=body.label, categories=categories, concurrency=body.concurrency,
        duration_s=body.duration_s, warmup_s=body.warmup_s, n_jobs=body.n_jobs,
        min_ability_override=body.min_ability_override, pin_model=body.pin_model,
    )
    run_id = new_perf_run_id()
    app.state.store.create_perf_run(
        id=run_id, label=body.label, config=json.dumps(dataclasses.asdict(config)),
        snapshot=_snapshot(app), started_at=_now_iso(),
    )
    task = asyncio.create_task(run_perf_test(app, app.state.store, run_id, config))
    app.state.perf_tasks[run_id] = task
    task.add_done_callback(lambda _t, rid=run_id: app.state.perf_tasks.pop(rid, None))
    return run_id


def summarize_run(samples: list[PerfSample]) -> dict:
    """Aggregate stats from `phase='measure'` samples only (warmup stays in the raw log
    but doesn't skew steady-state numbers)."""
    measured = [s for s in samples if s.phase == "measure"]
    served = [s for s in measured if s.outcome == "done"]
    unassigned = [s for s in measured if s.outcome == "unassigned"]
    scored = [s for s in served if s.passed is not None]
    passed = [s for s in scored if s.passed]
    latencies = sorted(s.latency_s for s in served if s.latency_s is not None)
    total_tokens = sum((s.tokens_in or 0) + (s.tokens_out or 0) for s in served)
    span = None
    if measured:
        span = max(s.completed_at or s.submitted_at for s in measured) - min(
            s.submitted_at for s in measured
        )

    def pct(p: float) -> float | None:
        if not latencies:
            return None
        idx = min(len(latencies) - 1, int(len(latencies) * p))
        return round(latencies[idx], 3)

    return {
        "n_warmup": len(samples) - len(measured),
        "n_measured": len(measured),
        "n_served": len(served),
        "n_unassigned": len(unassigned),
        "n_failed": len([s for s in measured if s.outcome in ("failed", "expired", "timeout")]),
        "pass_rate": round(len(passed) / len(scored), 3) if scored else None,
        "p50_latency_s": pct(0.5), "p90_latency_s": pct(0.9),
        "mean_latency_s": round(sum(latencies) / len(latencies), 3) if latencies else None,
        "jobs_per_s": round(len(served) / span, 3) if span else None,
        "tokens_per_s": round(total_tokens / span, 1) if span else None,
        "by_category": _by_key(measured, "category"),
    }


def perf_run_view(store: Store, row: PerfRun) -> dict:
    """One run's metadata + full aggregate stats (incl. percentiles/by-category), for the
    single-run detail view and the single-run JSON API. Loads every sample of this one
    run — proportionate for one run at a time, not for a list of many."""
    samples = store.perf_run_samples(row.id)
    return {
        "id": row.id, "label": row.label, "status": row.status,
        "config": json.loads(row.config),
        "snapshot": json.loads(row.snapshot) if row.snapshot else None,
        "started_at": row.started_at, "finished_at": row.finished_at,
        **summarize_run(samples),
    }


def perf_run_list_view(store: Store, row: PerfRun) -> dict:
    """One run's metadata + aggregate stats computed in SQL, for the runs list — a run
    with tens of thousands of samples must not mean loading them all on every 3s poll of
    the list. No percentiles (that needs the sorted sample list; see perf_run_view)."""
    s = store.perf_run_stats(row.id)
    span = None
    if s["span_start"] is not None and s["span_end"] is not None:
        span = s["span_end"] - s["span_start"]
    n_scored = s["n_scored"] or 0
    return {
        "id": row.id, "label": row.label, "status": row.status,
        "started_at": row.started_at, "finished_at": row.finished_at,
        "n_warmup": s["n_warmup"] or 0, "n_measured": s["n_measured"] or 0,
        "n_served": s["n_served"] or 0, "n_unassigned": s["n_unassigned"] or 0,
        "n_failed": s["n_failed"] or 0,
        "pass_rate": round((s["n_passed"] or 0) / n_scored, 3) if n_scored else None,
        "mean_latency_s": (round(s["mean_latency_s"], 3)
                            if s["mean_latency_s"] is not None else None),
        "jobs_per_s": round((s["n_served"] or 0) / span, 3) if span else None,
        "tokens_per_s": round((s["total_tokens"] or 0) / span, 1) if span else None,
    }


def _by_key(samples: list[PerfSample], key: str) -> list[dict]:
    groups: dict[str, list[PerfSample]] = {}
    for s in samples:
        groups.setdefault(getattr(s, key), []).append(s)
    out = []
    for k, rows in sorted(groups.items()):
        served = [r for r in rows if r.outcome == "done"]
        scored = [r for r in served if r.passed is not None]
        passed = [r for r in scored if r.passed]
        latencies = sorted(r.latency_s for r in served if r.latency_s is not None)
        out.append({
            key: k, "n": len(rows), "n_served": len(served),
            "n_unassigned": len([r for r in rows if r.outcome == "unassigned"]),
            "pass_rate": round(len(passed) / len(scored), 3) if scored else None,
            "mean_latency_s": (round(sum(latencies) / len(latencies), 3)
                               if latencies else None),
        })
    return out
