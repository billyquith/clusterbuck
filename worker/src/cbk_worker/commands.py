"""The seven verbs: work | submit | status | fleet | enroll | pause | resume."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
from typing import Any

import httpx
from redis.asyncio import Redis

from . import probe
from .cli import Out
from .config import (
    AGENT_FLAVOUR,
    AGENT_VERSION,
    PROTOCOL_VERSION,
    WorkerConfig,
    admin_headers,
    normalise_redis_url,
    server_url,
    update_public_key_pem,
)
from .inventory import ModelInventory
from .model_client import ModelClient
from .model_manager import ModelManager
from .models import (
    ActionResult,
    Fitness,
    HeartbeatRequest,
    ModelAction,
    NodeState,
    UpdateManifest,
)
from .presence import PresenceLadder
from .registry import RegistryClient, default_state_path, load_state, save_state
from .update import Outcome, UpdateApplier
from .work_loop import WorkLoop, queue_names

# --- cbk work -------------------------------------------------------------------------


def _apply_fitness(fitness: Fitness | None) -> bool:
    """Apply the coordinator's verdict. True ⇒ this worker must not claim jobs.

    Enforcement is COOPERATIVE: workers claim straight from Redis, so the coordinator cannot
    hard-block one that ignores this (ADR 27).
    """
    if fitness is None:
        return False
    if fitness.status == "quarantine":
        Out.warn(f"QUARANTINED by coordinator — not claiming jobs: {fitness.reason}. "
                 f"Update to {fitness.current_version or 'the current release'} "
                 f"(this agent is {AGENT_VERSION}).")
        return True
    if fitness.status == "stale":
        Out.warn(f"version {AGENT_VERSION} is behind "
                 f"{fitness.current_version or 'current'}: {fitness.reason}. Still serving.")
    return False


async def _try_update(applier: UpdateApplier, manifest_body: dict[str, Any],
                      loop: WorkLoop) -> None:
    """Verify and apply an offered update.

    Stops claiming first: completing the swap while holding a claimed job would strand it
    until the reaper reclaims it.
    """
    was_paused = loop.paused
    loop.paused = True
    result = await applier.apply(UpdateManifest.from_wire(manifest_body))
    if result.outcome is not Outcome.APPLIED:
        Out.warn(f"update {result.outcome.value}: {result.detail}")
        loop.paused = was_paused        # not updating after all — resume as before


async def _execute_action(manager: ModelManager, action: ModelAction) -> ActionResult:
    """Carry out one approved install/remove and report the outcome."""
    Out.dim(f"model {action.kind}: {action.artifact} (proposal {action.proposal_id})")
    if action.kind == "install":
        ok, error = await manager.install(action.registry_ref or action.artifact)
    elif action.kind == "remove":
        ok, error = await manager.remove(action.artifact)
    else:
        ok, error = False, f"unknown action kind '{action.kind}'"
    if ok:
        Out.good(f"model {action.kind} ok: {action.artifact}")
    else:
        Out.error(f"model {action.kind} failed: {error}")
    return ActionResult(proposal_id=action.proposal_id, ok=ok, error=error)


async def _heartbeat_loop(registry: RegistryClient, loop: WorkLoop, ladder: PresenceLadder,
                          inventory: ModelInventory, manager: ModelManager,
                          applier: UpdateApplier, state_path, cfg: WorkerConfig,
                          stop: asyncio.Event) -> None:
    """Re-read the persisted mode, drive the ladder, take a model inventory, report."""
    pending_result: ActionResult | None = None
    while not stop.is_set():
        try:
            state = load_state(state_path)
            if state is not None:
                effective = ladder.update(state.mode)
                loop.paused = effective == "paused"
                caps = ladder.capabilities()
                await loop.set_capabilities(caps)

                # Observed reality, not configuration: what this node's model server
                # actually has, and what is warm right now.
                installed = await inventory.installed()
                loaded = await inventory.loaded()
                digests = await inventory.digests()
                # The work loop needs the same inventory: a job carries the artifact whose
                # ability cleared its bar, and answering with a different model would report
                # success at a quality nobody checked. Fed from here rather than probed per
                # job so inference never waits on the model server's catalogue.
                loop.set_installed(installed)
                # Which models are warm right now. The work loop needs it to tell a COLD
                # job from a warm one, which is the only way to measure how long this
                # machine takes to bring a model up.
                loop.set_resident(loaded)

                # `stats.tps` is typed `number` in the contract with no null allowed,
                # so omit it entirely until this worker has actually measured something
                # rather than asserting a speed of zero it has not observed.
                stats: dict[str, object] = {"jobs_done": loop.jobs_done}
                if loop.tps is not None:
                    stats["tps"] = loop.tps
                # Same rule as tps: omitted until measured, never asserted as zero. A node
                # that has never observed a cold start has no load time, and reporting 0
                # would tell the coordinator it can warm instantly.
                if loop.load_s is not None:
                    stats["load_s"] = loop.load_s

                resp = await registry.heartbeat(state.node_id, state.node_key,
                    HeartbeatRequest(
                        mode=effective, installed=installed, loaded=loaded,
                        digests=digests or None, queues=queue_names(caps),
                        stats=stats,
                        protocol_version=PROTOCOL_VERSION,
                        agent_version=AGENT_VERSION,
                        agent_flavour=AGENT_FLAVOUR,
                        action_result=pending_result,
                    ))
                pending_result = None       # reported; do not repeat it

                # The coordinator judges whether this build is fit to run jobs. A quarantined
                # worker stops claiming: a version with known-bad behaviour producing
                # plausible-looking wrong results is worse than an idle node.
                quarantined = _apply_fitness(resp.fitness)
                loop.paused = quarantined or effective == "paused"

                # A signed update, if offered and this node opted in. Applying replaces the
                # process image, so nothing after this runs on success.
                if resp.update:
                    await _try_update(applier, resp.update, loop)

                # Execute an approved model action, if one was issued. The worker never
                # decides this — it only carries out an approved proposal. A quarantined
                # worker installs nothing.
                if not quarantined and resp.action is not None:
                    pending_result = await _execute_action(manager, resp.action)
        except asyncio.CancelledError:
            raise           # shutdown, not a failed beat
        except Exception:
            # A missed heartbeat is transient by nature — an unreachable coordinator, a
            # restarting model server. Retry on the next beat rather than taking the worker
            # down; jobs already claimed keep running regardless.
            pass

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=cfg.heartbeat_s)


async def run_work(args: argparse.Namespace) -> int:
    cfg = WorkerConfig.from_environment().with_overrides(
        capabilities=args.capabilities, model=args.model)

    # Enrolled mode: if a node identity exists, take the id and ladder from it and heartbeat.
    # Otherwise the worker stays purely env-configured.
    state_path = args.state if getattr(args, "state", None) else default_state_path()
    state = load_state(state_path)
    ladder: PresenceLadder | None = None
    if state is not None:
        ladder = PresenceLadder(state.ladder, state.capabilities, cfg.ladder_hysteresis_s)
        ladder.update(state.mode)
        cfg = cfg.__class__(**{**cfg.__dict__,
                               "worker_id": state.node_id,
                               "capabilities": tuple(ladder.capabilities())})

    stop = asyncio.Event()
    running = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, ValueError):
            running.add_signal_handler(sig, stop.set)

    redis = Redis.from_url(normalise_redis_url(cfg.redis_url), decode_responses=True)
    # Inference can be slow; the pull path slower still. Separate clients, separate patience.
    async with httpx.AsyncClient(timeout=600.0) as infer_http, \
               httpx.AsyncClient(timeout=30.0) as beat_http, \
               httpx.AsyncClient(timeout=3600.0, follow_redirects=True) as mgr_http:
        loop = WorkLoop(redis, ModelClient(infer_http, cfg), cfg, log=Out.dim)
        inventory = ModelInventory(beat_http, cfg.model_server_url, cfg.model_manager)
        manager = ModelManager(mgr_http, inventory.native_base, cfg.model_manager)
        applier = UpdateApplier(mgr_http, update_public_key_pem(), log=Out.info)

        tasks = [asyncio.create_task(loop.run(stop))]
        if state is not None and ladder is not None:
            tasks.append(asyncio.create_task(_heartbeat_loop(
                RegistryClient(beat_http, state.server), loop, ladder, inventory,
                manager, applier, state_path, cfg, stop)))
        try:
            await asyncio.gather(*tasks)
        finally:
            stop.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await redis.aclose()
    return 0


# --- admin verbs ----------------------------------------------------------------------


async def run_submit(args: argparse.Namespace) -> int:
    body: dict[str, Any] = {"prompt": args.prompt, "urgency": args.urgency,
                            "privacy": args.privacy}
    if args.capability:
        body["capability"] = args.capability
    if args.task_class:
        body["task_class"] = args.task_class
    if args.min_ability is not None:
        body["min_ability"] = args.min_ability
    if not args.capability and not args.task_class:
        Out.error("give either --capability or --task-class (with --min-ability)")
        return 1

    async with httpx.AsyncClient(timeout=30.0, headers=admin_headers()) as http:
        resp = await http.post(f"{server_url(args.server)}/jobs", json=body)
        if resp.status_code >= 400:
            Out.error(f"submit failed: HTTP {resp.status_code} {resp.text}")
            return 1
        out = resp.json()
    # The 202 body is {id, result_key, status} (protocols.md §1b) — `id` is the handle the
    # user needs for `cbk status`, so printing anything else makes the verb useless.
    job_id = out.get("id")
    Out.good(f"submitted {job_id}")
    Out.dim(f"status: {out.get('status')}  poll with: cbk status {job_id}")
    return 0


async def run_status(args: argparse.Namespace) -> int:
    async with httpx.AsyncClient(timeout=30.0, headers=admin_headers()) as http:
        resp = await http.get(f"{server_url(args.server)}/jobs/{args.job_id}")
        if resp.status_code == 404:
            Out.error(f"unknown job {args.job_id}")
            return 1
        resp.raise_for_status()
        body = resp.json()

    # The poll body is FLAT (protocols.md §1b): `result` IS the completion object, and
    # worker/error/attempts sit beside it. Reading `result` as a wrapper found none of
    # them and printed the status line alone — the answer itself never reached the user.
    status = body.get("status", "?")
    Out.line(f"{args.job_id}: {status}")
    if worker := body.get("worker"):
        Out.dim(f"worker: {worker}")
    if (attempts := body.get("attempts") or 0) > 1:
        Out.dim(f"attempts: {attempts}")
    if error := body.get("error"):
        Out.error(f"error: {error}")
    completion = body.get("result") or {}
    for choice in completion.get("choices") or []:
        content = (choice.get("message") or {}).get("content")
        if content:
            Out.line(content)
    return 0


async def run_fleet(args: argparse.Namespace) -> int:
    base = server_url(args.server)
    async with httpx.AsyncClient(timeout=30.0, headers=admin_headers()) as http:
        fleet = await http.get(f"{base}/fleet")
        fleet.raise_for_status()
        body = fleet.json()
        nodes_resp = await http.get(f"{base}/nodes")
        nodes = nodes_resp.json().get("nodes", []) if nodes_resp.status_code < 400 else []

    # /fleet returns capabilities as an OBJECT keyed by capability name, and the
    # capability→node mapping only in the other direction (each node lists what it serves),
    # so invert it here. Iterating the object as a list of rows yielded bare string keys and
    # died on the first `.get`.
    caps: dict[str, dict[str, Any]] = body.get("capabilities") or {}
    serving: dict[str, list[str]] = {}
    for node in body.get("nodes") or []:
        for cap in node.get("capabilities") or []:
            serving.setdefault(cap, []).append(node.get("id", "?"))
    if caps:
        Out.info("capabilities")
        Out.table(["capability", "model", "nodes"],
                  [[name, spec.get("model", "-"), ", ".join(serving.get(name, [])) or "-"]
                   for name, spec in caps.items()])
    if nodes:
        Out.line()
        Out.info("nodes")
        Out.table(["node", "flavour", "version", "mode", "fitness", "last heartbeat"],
                  [[n.get("node_id", "?"), n.get("agent_flavour", "?"),
                    n.get("agent_version") or "unknown", n.get("mode") or "-",
                    n.get("fitness") or "unknown", n.get("last_heartbeat") or "never"]
                   for n in nodes])
    if not caps and not nodes:
        Out.dim("fleet is empty")
    return 0


async def run_enroll(args: argparse.Namespace) -> int:
    server = server_url(args.server)
    state_path = args.state or default_state_path()
    req = probe.build(args.token, args.profile)
    Out.dim(f"probed ram={req.hw.ram_gb}GB accel={req.hw.accelerator} "
            f"disk={req.hw.disk_free_gb}GB arch={req.arch}")

    async with httpx.AsyncClient(timeout=30.0) as http:
        try:
            body = await RegistryClient(http, server).enroll(req)
        except Exception as e:
            Out.error(f"enroll failed: {e}")
            return 1

    proposed = body.get("proposed") or {}
    save_state(state_path, NodeState(
        node_id=body["node_id"], node_key=body["node_key"], server=server,
        capabilities=proposed.get("capabilities") or [], ladder=proposed.get("ladder"),
        mode="active",
    ))
    Out.good(f"enrolled as {body['node_id']} · capabilities: "
             f"{', '.join(proposed.get('capabilities') or [])}")
    Out.dim(f"identity saved to {state_path}")
    return 0


def run_mode(args: argparse.Namespace, mode: str) -> int:
    """`cbk pause` / `cbk resume` — the owner's fast-eviction control (ADR 10).

    Flips the persisted presence mode; a running `cbk work` re-reads it each heartbeat and
    stops or starts claiming within seconds.
    """
    from dataclasses import replace

    path = args.state or default_state_path()
    state = load_state(path)
    if state is None:
        Out.error("not enrolled — run `cbk enroll --token …` first")
        return 1
    save_state(path, replace(state, mode=mode))
    Out.good(f"mode → {mode} for {state.node_id}")
    return 0
