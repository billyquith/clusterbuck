"""The pull loop against a real Redis (protocols.md §2, ADR 20).

Streams semantics are the whole point of this component, so these run against a real broker
rather than a fake: consumer-group creation, claim, ack, and the pending-entries list are
exactly what a fake would get wrong.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from cbk_worker.config import WorkerConfig
from cbk_worker.model_client import ModelClient
from cbk_worker.models import Job
from cbk_worker.work_loop import (
    TIER_ORDER,
    URGENT_TIER,
    WorkLoop,
    queue_names,
    stream_key,
)

_OK = {
    "id": "c1",
    "choices": [{"index": 0, "message": {"role": "assistant", "content": "pong"}}],
    "usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4},
}

pytestmark = pytest.mark.usefixtures("redis_client")


def _cfg(**kw) -> WorkerConfig:
    defaults = {"worker_id": "node-test", "consumer_group": "cbk-workers",
                "capabilities": ("8b-extract",), "poll_s": 0.01}
    return WorkerConfig(**{**defaults, **kw})


def _model(http_handler=None) -> tuple[httpx.AsyncClient, ModelClient]:
    def ok(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_OK)

    client = httpx.AsyncClient(transport=httpx.MockTransport(http_handler or ok))
    return client, ModelClient(client, _cfg())


def _job_wire(job_id="job_1", capability="8b-extract", **kw) -> str:
    return json.dumps({
        "id": job_id, "created_at": "2026-07-28T10:00:00Z", "capability": capability,
        "prompt": "ping", "params": {}, "urgency": "waitable", "privacy": "local_only",
        "result_key": f"res:{job_id}", "attempts": 0, "max_attempts": 3, **kw,
    })


async def test_a_queued_job_is_claimed_run_acked_and_its_result_stored(redis_client):
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire()})

    assert await loop.poll_once() is True

    stored = json.loads(await redis_client.get("res:job_1"))
    assert stored["status"] == "done" and stored["worker"] == "node-test"
    assert stored["completion"]["choices"][0]["message"]["content"] == "pong"
    assert stored["usage"]["total_tokens"] == 4
    # Acked ⇒ gone from the pending-entries list, so the reaper will not redeliver it.
    pending = await redis_client.xpending(stream_key("8b-extract"), cfg.consumer_group)
    assert pending["pending"] == 0
    await http.aclose()


async def test_a_failed_job_still_gets_a_terminal_result_and_is_acked(redis_client):
    """A failure is terminal, not a redelivery: the caller must learn it failed, and the
    reaper must not hand the same doomed job round the fleet."""
    def boom(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="model server exploded")

    cfg = _cfg()
    http, model = _model(boom)
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("job_fail")})

    await loop.poll_once()

    stored = json.loads(await redis_client.get("res:job_fail"))
    assert stored["status"] == "failed" and stored["error"]
    assert "completion" not in stored
    pending = await redis_client.xpending(stream_key("8b-extract"), cfg.consumer_group)
    assert pending["pending"] == 0
    await http.aclose()


async def test_the_result_carries_the_configured_ttl(redis_client):
    cfg = _cfg(result_ttl_s=1234)
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("job_ttl")})
    await loop.poll_once()
    ttl = await redis_client.ttl("res:job_ttl")
    assert 0 < ttl <= 1234
    await http.aclose()


async def test_paused_worker_claims_nothing_and_leaves_the_job_queued(redis_client):
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("job_paused")})

    loop.paused = True
    assert await loop.poll_once() is False
    assert await redis_client.get("res:job_paused") is None

    # Unpausing picks it up — the job waited, it was not lost.
    loop.paused = False
    assert await loop.poll_once() is True
    assert await redis_client.get("res:job_paused") is not None
    await http.aclose()


async def test_one_job_per_capability_per_pass_so_a_busy_queue_cannot_starve_another(
        redis_client):
    """Draining one stream fully before looking at the next would let a flooded small-model
    queue monopolise a node that also serves the big-model queue."""
    cfg = _cfg(capabilities=("8b-extract", "32b-reason"))
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    for i in range(3):
        await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire(f"small_{i}")})
    await redis_client.xadd(stream_key("32b-reason"),
                            {"job": _job_wire("big_0", capability="32b-reason")})

    await loop.poll_once()      # one pass

    # The big job ran on the very first pass, despite three small jobs queued ahead of it.
    assert await redis_client.get("res:big_0") is not None
    assert await redis_client.get("res:small_0") is not None
    assert await redis_client.get("res:small_1") is None
    await http.aclose()


async def test_ensure_groups_is_idempotent_and_survives_a_restart(redis_client):
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await loop.ensure_groups()          # BUSYGROUP must be tolerated, not raised
    groups = await redis_client.xinfo_groups(stream_key("8b-extract"))
    assert [g["name"] for g in groups] == ["cbk-workers"]
    await http.aclose()


async def test_set_capabilities_subscribes_the_new_queue_live(redis_client):
    """The presence ladder swaps capabilities on a running worker (active ⇄ away), so the
    group for a newly served tier has to be created on the fly."""
    cfg = _cfg(capabilities=("8b-extract",))
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()

    await loop.set_capabilities(["8b-extract", "70b-reason"])
    assert loop.capabilities == ("8b-extract", "70b-reason")
    await redis_client.xadd(stream_key("70b-reason"),
                            {"job": _job_wire("job_big", capability="70b-reason")})
    assert await loop.poll_once() is True
    assert await redis_client.get("res:job_big") is not None
    await http.aclose()


async def test_a_malformed_entry_is_acked_rather_than_poisoning_the_queue(redis_client):
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"not-a-job": "garbage"})

    await loop.poll_once()
    pending = await redis_client.xpending(stream_key("8b-extract"), cfg.consumer_group)
    assert pending["pending"] == 0, "an entry with no job field stayed pending forever"
    await http.aclose()


async def test_run_exits_promptly_when_asked_to_stop(redis_client):
    cfg = _cfg(poll_s=30.0)     # a long idle poll must not delay shutdown
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    stop = asyncio.Event()
    task = asyncio.create_task(loop.run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=2.0)
    await http.aclose()


# --- urgency tiers (ADR 34) ---------------------------------------------------------


def test_queue_names_reports_both_tiers():
    """This list is the coordinator's rollout evidence: it starts tiering a capability
    only once every node serving it reports that it reads the urgent stream. A worker that
    did not list it would (correctly) never be tiered."""
    names = queue_names(("8b-extract",))
    assert stream_key("8b-extract", URGENT_TIER) in names
    assert stream_key("8b-extract") in names


def test_the_base_stream_name_is_unchanged():
    """Load-bearing in both directions: a coordinator that does not tier writes to
    `q:<cap>`, and renaming it would strand every job already queued."""
    assert stream_key("8b-extract") == "q:8b-extract"
    assert stream_key("8b-extract", None) == "q:8b-extract"
    assert stream_key("8b-extract", URGENT_TIER) == "q:8b-extract:urgent"
    assert TIER_ORDER == (URGENT_TIER, None), "urgent is read first"


async def test_groups_are_created_on_both_tiers(redis_client):
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    for tier in TIER_ORDER:
        groups = await redis_client.xinfo_groups(stream_key("8b-extract", tier))
        assert [g["name"] for g in groups] == ["cbk-workers"]
    await http.aclose()


async def test_urgent_work_is_served_before_a_patient_backlog(redis_client):
    """The whole point of the tier, and what ADR 24 deferred: on a worker that is already
    awake, urgency now decides order rather than only whether a machine gets woken."""
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("patient")})
    await redis_client.xadd(stream_key("8b-extract", URGENT_TIER),
                            {"job": _job_wire("now", urgency="urgent")})

    await loop.poll_once()

    assert await redis_client.get("res:now") is not None, "urgent tier read first"
    assert await redis_client.get("res:patient") is None, "backlog waits its turn"

    await loop.poll_once()
    assert await redis_client.get("res:patient") is not None
    await http.aclose()


async def test_the_base_stream_is_read_when_the_urgent_tier_is_empty(redis_client):
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("only")})

    assert await loop.poll_once() is True
    assert await redis_client.get("res:only") is not None
    await http.aclose()


async def test_a_busy_urgent_tier_still_cannot_starve_another_capability(redis_client):
    """The fairness property from above, re-asserted across tiers.

    Reading urgent-then-base happens INSIDE the one-job-per-capability discipline, so a
    flooded urgent stream on one capability must not monopolise a node that also serves
    another. Tiering changing that would be a regression, not a feature.
    """
    cfg = _cfg(capabilities=("8b-extract", "32b-reason"))
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    for i in range(3):
        await redis_client.xadd(stream_key("8b-extract", URGENT_TIER),
                                {"job": _job_wire(f"rush_{i}", urgency="urgent")})
    await redis_client.xadd(stream_key("32b-reason"),
                            {"job": _job_wire("big_0", capability="32b-reason")})

    await loop.poll_once()

    assert await redis_client.get("res:rush_0") is not None
    assert await redis_client.get("res:big_0") is not None, "not starved by the rush"
    assert await redis_client.get("res:rush_1") is None, "still one per capability"
    await http.aclose()


async def test_a_job_is_acked_on_the_tier_it_came_from(redis_client):
    """Acking the wrong stream would leave the entry pending forever, and the reaper would
    then reclaim and re-run work that had already succeeded."""
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract", URGENT_TIER),
                            {"job": _job_wire("job_ack", urgency="urgent")})

    await loop.poll_once()

    pending = await redis_client.xpending(
        stream_key("8b-extract", URGENT_TIER), cfg.consumer_group)
    assert pending["pending"] == 0, "acked on the urgent tier, not the base one"
    await http.aclose()


async def test_a_malformed_urgent_entry_is_acked_on_its_own_tier(redis_client):
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract", URGENT_TIER),
                            {"not-a-job": "garbage"})

    await loop.poll_once()

    pending = await redis_client.xpending(
        stream_key("8b-extract", URGENT_TIER), cfg.consumer_group)
    assert pending["pending"] == 0
    await http.aclose()



# --- the pinned artifact must be the one that answers -----------------------------------

def _job_pinned_to(artifact: str) -> Job:
    return Job.from_wire({
        "id": "j1", "created_at": "t", "capability": "8b-extract",
        "messages": [{"role": "user", "content": "hi"}],
        "params": {"model": artifact}, "result_key": "r:j1",
    })


def _loop_with(installed, model_name="llama3.2:3b"):
    cfg = WorkerConfig(redis_url="redis://x", model_name=model_name,
                       capabilities=("8b-extract",))
    loop = WorkLoop(redis=None, model=None, cfg=cfg, log=lambda _m: None)
    loop.set_installed(installed)
    return loop


def test_a_pin_this_node_cannot_serve_is_refused():
    """The coordinator pins the artifact whose ability cleared the job's floor. Answering
    with a different model reports success at a quality nobody checked — the silent
    under-serve the pin exists to end."""
    loop = _loop_with(["llama3.2:3b"])
    reason = loop._refuse_reason(_job_pinned_to("qwen2.5:32b"))
    assert reason and "qwen2.5:32b" in reason


def test_a_pin_this_node_holds_is_served():
    loop = _loop_with(["llama3.2:3b", "qwen2.5:32b"])
    assert loop._refuse_reason(_job_pinned_to("qwen2.5:32b")) is None


def test_an_implicit_latest_tag_still_matches():
    """Must agree with the coordinator's own normalisation, or a pin it believes valid is
    refused here and the job fails for no real reason."""
    assert _loop_with(["llama3.2:latest"])._refuse_reason(_job_pinned_to("llama3.2")) is None
    assert _loop_with(["llama3.2"])._refuse_reason(_job_pinned_to("llama3.2:latest")) is None


def test_an_unknown_inventory_refuses_nothing():
    """An empty inventory means the model server did not answer, not that it holds
    nothing. Refusing on that would take the node offline over a transient blip."""
    assert _loop_with([])._refuse_reason(_job_pinned_to("anything:9b")) is None


def test_an_unpinned_job_is_never_refused():
    """Jobs submitted before the coordinator pinned artifacts, and any other client that
    does not pin, still run on the node's configured model."""
    loop = _loop_with(["llama3.2:3b"])
    job = Job.from_wire({"id": "j", "created_at": "t", "capability": "8b-extract",
                         "prompt": "hi", "result_key": "r"})
    assert loop._refuse_reason(job) is None


def _loop_measuring(samples):
    loop = _loop_with([])
    for s in samples:
        loop._tps_samples.append(s)
    return loop


def test_a_job_with_no_floor_is_never_refused_on_speed():
    loop = _loop_measuring([1.0])
    job = Job.from_wire({"id": "j", "created_at": "t", "capability": "8b-extract",
                         "prompt": "hi", "result_key": "r"})
    assert loop._refuse_reason(job) is None


async def test_a_model_swap_clears_the_throughput_window(redis_client):
    """A window that survives a swap reports the OLD model's speed as the new one's —
    flattering a big model that just landed, libelling a small one that replaced it, and
    poisoning the load estimate that subtracts it."""
    cfg = WorkerConfig(redis_url="redis://x", model_name="small:1b", capabilities=("c",),
                       consumer_group="g", worker_id="w")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=_OK))) as http:
        loop = WorkLoop(redis_client, ModelClient(http, cfg), cfg, log=lambda _m: None)
        await loop.ensure_groups()

        async def run(pinned: str | None) -> None:
            params = {"model": pinned} if pinned else {}
            await redis_client.xadd(stream_key("c"), {"job": json.dumps({
                "id": f"j-{pinned}", "created_at": "t", "capability": "c",
                "prompt": "hi", "params": params, "result_key": f"r-{pinned}"})})
            await loop.poll_once()

        await run(None)
        await run(None)
        assert len(loop._tps_samples) == 2, "no baseline measured"

        await run("big:70b")          # a job pinned to a different artifact
        # One sample, belonging to the new artifact — the window was emptied, not appended
        # to. It measures the new model immediately rather than after 20 jobs of drift.
        assert len(loop._tps_samples) == 1
        assert loop._tps_model == "big:70b"


# --- a node that slept through its own job (B2) ---------------------------------------


async def test_a_late_copy_does_not_overwrite_an_answer_that_already_landed(redis_client):
    """The lid-closed-mid-job case, from the sleeping node's side.

    A machine that sleeps mid-inference is not dead. The coordinator's reaper reclaims the
    entry after `reaper_min_idle_ms` and another worker answers; hours later this node
    wakes, finishes the generation it was suspended in the middle of, and writes. Writing
    unconditionally overwrote a terminal answer the client may already have read — and
    where the reaper had dead-lettered the job, it turned a `failed` the client was told
    about into a silent `done`. Both copies are valid answers to one job, so the tie goes
    to whichever landed first and terminal stays terminal.
    """
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("job_slept")})
    # Whoever the reaper handed it to got there first.
    winner = {"job_id": "job_slept", "status": "done", "worker": "node-other",
              "completion": {"choices": [{"index": 0, "message": {
                  "role": "assistant", "content": "answered while you were away"}}]}}
    await redis_client.set("res:job_slept", json.dumps(winner))

    assert await loop.poll_once() is True

    stored = json.loads(await redis_client.get("res:job_slept"))
    assert stored["worker"] == "node-other", "the first answer must survive"
    assert stored["completion"]["choices"][0]["message"]["content"] == \
        "answered while you were away"
    await http.aclose()


async def test_a_dead_letter_is_not_quietly_turned_into_a_success(redis_client):
    """The worse half of the same bug: the client was TOLD this job failed."""
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("job_dl")})
    await redis_client.set("res:job_dl", json.dumps({
        "job_id": "job_dl", "status": "failed", "worker": "cbk-reaper",
        "error": "abandoned by its worker and retried 3 times (max_attempts=3)",
    }))

    await loop.poll_once()

    assert json.loads(await redis_client.get("res:job_dl"))["status"] == "failed"
    await http.aclose()


async def test_a_late_copy_is_still_acked(redis_client):
    """Very likely a no-op — the reclaiming reaper acked the old entry when it requeued —
    but if it is not, leaving the entry pending gives the reaper something to churn on for
    a job that already has an answer."""
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("job_ack")})
    await redis_client.set("res:job_ack", json.dumps(
        {"job_id": "job_ack", "status": "done", "worker": "node-other"}))

    await loop.poll_once()

    pending = await redis_client.xpending(stream_key("8b-extract"), cfg.consumer_group)
    assert pending["pending"] == 0
    await http.aclose()


async def test_the_ordinary_path_still_writes_its_result(redis_client):
    """The guard must not become a refusal to answer: with nothing there already, the
    worker's own result is what lands, TTL and all."""
    cfg = _cfg(result_ttl_s=1234)
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("job_fresh")})

    await loop.poll_once()

    stored = json.loads(await redis_client.get("res:job_fresh"))
    assert stored["worker"] == "node-test" and stored["status"] == "done"
    assert 0 < await redis_client.ttl("res:job_fresh") <= 1234
    await http.aclose()
