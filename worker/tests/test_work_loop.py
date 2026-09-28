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



async def _claim(loop) -> bool:
    """Claim whatever is waiting AND let it finish.

    `poll_once` STARTS jobs rather than awaiting them, so a node can run more than one at
    a time. A test that asserts on a result therefore has to wait for the job it just
    claimed; `drain` is the same wait the loop performs on shutdown.
    """
    took = await loop.poll_once()
    await loop.drain()
    return took


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

    assert await _claim(loop) is True

    stored = json.loads(await redis_client.get("res:job_1"))
    assert stored["status"] == "done" and stored["worker"] == "node-test"
    assert stored["completion"]["choices"][0]["message"]["content"] == "pong"
    assert stored["usage"]["total_tokens"] == 4
    # Acked ⇒ gone from the pending-entries list, so the reaper will not redeliver it.
    pending = await redis_client.xpending(stream_key("8b-extract"), cfg.consumer_group)
    assert pending["pending"] == 0
    await http.aclose()


def _raises(exc_type):
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc_type("simulated", request=request)
    return handler


@pytest.mark.parametrize("handler, code", [
    (_raises(httpx.ConnectError), "model_server_unreachable"),
    (_raises(httpx.ReadTimeout), "model_server_timeout"),
    (lambda r: httpx.Response(500, text="exploded"), "model_server_error"),
    (lambda r: httpx.Response(200, text="<html>not json</html>"), "model_server_error"),
    (lambda r: httpx.Response(400, json={"error": "context length exceeded"}),
     "model_request_rejected"),
    (lambda r: httpx.Response(200, json={**_OK, "model": "some-other-model"}),
     "model_substituted"),
])
async def test_a_failed_job_says_why_as_a_code(redis_client, handler, code):
    """The client must be able to tell a dead server from a broken one from a wrong
    model without reading the message — each is a different thing to tell a user."""
    http, model = _model(handler)
    loop = WorkLoop(redis_client, model, _cfg(), log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {
        "job": _job_wire("job_code", params={"model": "llama3.2:3b"})})
    await _claim(loop)
    stored = json.loads(await redis_client.get("res:job_code"))
    assert stored["status"] == "failed"
    assert stored["error_code"] == code, stored["error"]
    await http.aclose()


async def test_a_dead_model_server_cools_the_capability_down(redis_client):
    """Without this, a node whose model server is down drains its whole backlog on that
    capability in milliseconds per job — thousands of terminal `failed` results an hour —
    before a healthy sibling node ever gets a look at any of them. Reaching
    `server_down_threshold` consecutive `model_server_unreachable` results must stop this
    node claiming from the capability until the cooldown elapses, without touching a
    DIFFERENT capability it also serves."""
    cfg = _cfg(capabilities=("8b-extract", "32b-reason"),
              server_down_threshold=2, server_down_cooldown_s=1000.0)
    http, model = _model(_raises(httpx.ConnectError))
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()

    for i in range(2):
        await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire(f"dead_{i}")})
        await _claim(loop)
    for i in range(2):
        stored = json.loads(await redis_client.get(f"res:dead_{i}"))
        assert stored["error_code"] == "model_server_unreachable"

    # The threshold is now reached: a THIRD job on the same capability must not even be
    # claimed, let alone failed.
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("dead_2")})
    took = await _claim(loop)
    assert took is False, "the capability should be on cooldown, not claiming"
    assert await redis_client.get("res:dead_2") is None, "left queued, not failed"

    # A different capability this same node serves is entirely unaffected.
    await redis_client.xadd(stream_key("32b-reason"),
                            {"job": _job_wire("other", capability="32b-reason")})
    await _claim(loop)
    stored = json.loads(await redis_client.get("res:other"))
    assert stored["error_code"] == "model_server_unreachable", "still served, just also down"
    await http.aclose()


async def test_a_success_resets_the_down_streak(redis_client):
    """A single good answer after some failures must not count towards the threshold —
    a flaky-but-alive server is not the scenario the cooldown exists for."""
    calls = {"n": 0}

    def flaky(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("simulated", request=request)
        return httpx.Response(200, json=_OK)

    cfg = _cfg(server_down_threshold=2, server_down_cooldown_s=1000.0)
    http, model = _model(flaky)
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()

    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("mix_0")})
    await _claim(loop)  # fails: streak 1
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("mix_1")})
    await _claim(loop)  # succeeds: streak reset to 0

    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("mix_2")})
    took = await _claim(loop)
    assert took is True, "one prior failure must not have armed the cooldown"
    await http.aclose()


async def test_a_pin_this_node_lacks_fails_as_artifact_not_installed(redis_client):
    http, model = _model()
    loop = WorkLoop(redis_client, model, _cfg(), log=lambda _: None)
    loop.set_installed(["llama3.2:3b"])
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {
        "job": _job_wire("job_pin", params={"model": "qwen2.5:32b"})})
    await _claim(loop)
    stored = json.loads(await redis_client.get("res:job_pin"))
    assert stored["error_code"] == "artifact_not_installed"
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

    await _claim(loop)

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
    await _claim(loop)
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
    assert await _claim(loop) is False
    assert await redis_client.get("res:job_paused") is None

    # Unpausing picks it up — the job waited, it was not lost.
    loop.paused = False
    assert await _claim(loop) is True
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

    await _claim(loop)      # one pass

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
    assert await _claim(loop) is True
    assert await redis_client.get("res:job_big") is not None
    await http.aclose()


async def test_a_malformed_entry_is_acked_rather_than_poisoning_the_queue(redis_client):
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"not-a-job": "garbage"})

    await _claim(loop)
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

    await _claim(loop)

    assert await redis_client.get("res:now") is not None, "urgent tier read first"
    assert await redis_client.get("res:patient") is None, "backlog waits its turn"

    await _claim(loop)
    assert await redis_client.get("res:patient") is not None
    await http.aclose()


async def test_the_base_stream_is_read_when_the_urgent_tier_is_empty(redis_client):
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("only")})

    assert await _claim(loop) is True
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

    await _claim(loop)

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

    await _claim(loop)

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

    await _claim(loop)

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
            await _claim(loop)

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

    assert await _claim(loop) is True

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

    await _claim(loop)

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

    await _claim(loop)

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

    await _claim(loop)

    stored = json.loads(await redis_client.get("res:job_fresh"))
    assert stored["worker"] == "node-test" and stored["status"] == "done"
    assert 0 < await redis_client.ttl("res:job_fresh") <= 1234
    await http.aclose()


async def test_a_broker_outage_does_not_kill_the_loop_but_is_reported(redis_client):
    """The inversion of an earlier decision, and the condition that earned it.

    This loop used to let a broker error kill the process on purpose, because exiting
    took the heartbeat down with it — and the coordinator reads a recent heartbeat naming
    a capability's streams as proof somebody is serving them, so a worker that survived a
    dead broker would vouch for a queue it could not read and quietly starve it.

    `broker_ok` closes that without the restart, so the loop can stay up: the heartbeat
    reports an empty `queues` while this is false, which is literally true and which the
    coordinator already reads as "not serving".
    """
    from redis.exceptions import ConnectionError as RedisConnectionError

    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()

    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RedisConnectionError("broker went away")
        stop.set()
        return False

    stop = asyncio.Event()
    loop.poll_once = flaky
    await loop.run(stop)                       # survives rather than raising

    assert calls["n"] >= 2, "it kept going after the outage"
    await http.aclose()


async def test_the_broker_flag_tracks_the_outage_and_the_recovery(redis_client):
    """One flag, both edges: the heartbeat needs to stop vouching and then start again."""
    from redis.exceptions import ConnectionError as RedisConnectionError

    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    assert loop.broker_ok is True

    state = {"down": True}

    async def flaky():
        if state["down"]:
            raise RedisConnectionError("nope")
        return False

    loop.poll_once = flaky
    stop = asyncio.Event()

    async def drive():
        await asyncio.sleep(0.05)
        assert loop.broker_ok is False, "an outage must stop this node vouching"
        state["down"] = False
        await asyncio.sleep(0.05)
        assert loop.broker_ok is True, "and it must resume the moment the broker returns"
        stop.set()

    await asyncio.gather(loop.run(stop), drive())
    await http.aclose()


async def test_a_shutdown_still_stops_the_loop_during_an_outage(redis_client):
    """Surviving a broker must not mean ignoring SIGTERM."""
    from redis.exceptions import ConnectionError as RedisConnectionError

    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)

    async def always_down():
        raise RedisConnectionError("nope")

    loop.poll_once = always_down
    stop = asyncio.Event()
    stop.set()
    await asyncio.wait_for(loop.run(stop), timeout=5)
    await http.aclose()


# --- the owner takes the machine back, mid-job (C1) -----------------------------------


async def test_evicting_leaves_no_result_and_leaves_the_entry_claimable(redis_client):
    """The property that matters most, and the one a careless fix would break.

    An evicted job has not FAILED — it was interrupted — so it must not get a terminal
    result. `_process` turns any `Exception` into a `failed` result, which is right for a
    job that cannot be served and catastrophic for this one: the client would be handed a
    permanent error because somebody sat down at a laptop. Nothing written, nothing acked,
    entry still pending, and the coordinator's reaper requeues it on the path an abandoned
    job already uses.
    """
    started = asyncio.Event()

    async def hang(request: httpx.Request) -> httpx.Response:
        started.set()
        await asyncio.sleep(30)                      # a long generation
        return httpx.Response(200, json=_OK)

    cfg = _cfg()
    http, model = _model(hang)
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("job_eviction")})

    polling = asyncio.create_task(loop.poll_once())
    await asyncio.wait_for(started.wait(), timeout=5)

    assert loop.evict() is True
    assert await asyncio.wait_for(polling, timeout=5) is True

    assert await redis_client.get("res:job_eviction") is None, \
        "an interrupted job must not be answered — it has to be retryable"
    pending = await redis_client.xpending(stream_key("8b-extract"), cfg.consumer_group)
    assert pending["pending"] == 1, "still claimed, so the reaper will requeue it"
    await http.aclose()


async def test_evicting_when_nothing_is_running_is_a_no_op(redis_client):
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    assert loop.evict() is False
    await http.aclose()


async def test_a_job_completed_before_the_evict_is_not_mistaken_for_one(redis_client):
    """Why the cancelled task is remembered by identity rather than a boolean.

    A flag set just as a job finished would still be standing when the next job started,
    and that job would read its own ordinary completion as an eviction — losing a result
    that was already paid for.
    """
    cfg = _cfg()
    http, model = _model()
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("job_one")})

    assert await _claim(loop) is True
    assert loop.evict() is False                      # too late; it already finished
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("job_two")})
    assert await _claim(loop) is True

    assert json.loads(await redis_client.get("res:job_two"))["status"] == "done"
    await http.aclose()


async def test_a_shutdown_cancellation_is_not_swallowed_as_an_eviction(redis_client):
    """A genuine cancel must stay a cancel, or the process stops exiting when asked.

    Cancelling the JOB task rather than the poll: jobs now run as their own tasks, so
    that is what a hard shutdown actually cancels. The distinction under test is
    unchanged and is the reason `_evicted_tasks` exists — a cancellation nobody asked
    for via `evict()` must propagate as `CancelledError` and NOT be re-labelled
    `Evicted`, which `_process` handles by leaving the entry for the reaper.
    """
    started = asyncio.Event()

    async def hang(request: httpx.Request) -> httpx.Response:
        started.set()
        await asyncio.sleep(30)
        return httpx.Response(200, json=_OK)

    cfg = _cfg()
    http, model = _model(hang)
    loop = WorkLoop(redis_client, model, cfg, log=lambda _: None)
    await loop.ensure_groups()
    await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire("job_shutdown")})

    assert await loop.poll_once() is True
    await asyncio.wait_for(started.wait(), timeout=5)

    running = next(iter(loop._running))
    running.cancel()                                  # shutdown, nobody called evict()
    with pytest.raises(asyncio.CancelledError):
        await running

    assert await redis_client.get("res:job_shutdown") is None, \
        "a shutdown mid-generation answers nothing; the reaper owns the job"
    await http.aclose()


def test_an_eviction_can_never_be_caught_as_a_failure():
    """Structural, not behavioural: `Evicted` is not an `Exception` at all, so no
    `except Exception` anywhere can turn an interruption into a terminal answer."""
    from cbk_worker.work_loop import Evicted

    assert issubclass(Evicted, BaseException)
    assert not issubclass(Evicted, Exception)


# --- concurrency: a ceiling the operator sets, a limit the machine allows -----------


def _loop(redis_client, model, **cfgkw):
    return WorkLoop(redis_client, model, _cfg(**cfgkw), log=lambda _: None)


@pytest.mark.parametrize("profile,mode,ceiling,expected", [
    # A dedicated box exists to serve: nobody is waiting for it, so it runs the ceiling
    # whatever the "presence" says — the mode on such a node describes nothing anyone
    # feels. Holding slots back is the same pure loss as dropping its weights on a pause.
    ("dedicated", "active", 4, 4),
    ("dedicated", "away", 4, 4),
    # Somebody's machine. At `away` it looks idle and the ladder is already climbing to
    # heavier models, so take the ceiling; at `active` a person is using it, and one job
    # at a time is what the node always did.
    ("shared", "away", 4, 4),
    ("shared", "active", 4, 1),
    ("background", "active", 4, 1),
    # Unknown profile yields, for the reason `yields_to_a_person` gives: being wrong that
    # way costs throughput, the other way costs somebody their laptop.
    (None, "active", 4, 1),
    (None, "away", 4, 4),
    # Paused takes everything, either way. Claiming stops; what is in hand finishes or is
    # evicted, depending on the profile.
    ("dedicated", "paused", 4, 0),
    ("shared", "paused", 4, 0),
    # The ceiling is still a ceiling.
    ("dedicated", "away", 1, 1),
])
def test_effective_limit_follows_the_profile_and_the_owner(
    redis_client, profile, mode, ceiling, expected
):
    loop = _loop(redis_client, None, max_concurrent_jobs=ceiling)
    loop.set_presence(mode, profile)
    assert loop.effective_limit == expected


async def test_the_default_is_one_at_a_time_so_an_upgrade_changes_no_node(redis_client):
    """The ceiling defaults to 1 deliberately: raising it is an operator's decision about
    their own hardware, not something an upgrade does to every node in a fleet."""
    http, model = _model()
    loop = _loop(redis_client, model)
    loop.set_presence("away", "dedicated")
    assert loop.effective_limit == 1
    await http.aclose()


async def test_a_dedicated_node_runs_jobs_concurrently(redis_client):
    """The capacity finding this exists for. Every model server clusterbuck targets can
    serve concurrent requests, and vLLM's continuous batching gains are near-linear — but
    the worker awaited each job inline, so fleet throughput was `number of nodes`, not
    `nodes x concurrency`, and a fast accelerator sat mostly idle.
    """
    running = 0
    peak = 0
    release = asyncio.Event()

    async def slow(request: httpx.Request) -> httpx.Response:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await release.wait()
        running -= 1
        return httpx.Response(200, json=_OK)

    http, model = _model(slow)
    loop = _loop(redis_client, model, max_concurrent_jobs=3)
    loop.set_presence("away", "dedicated")
    await loop.ensure_groups()
    for i in range(3):
        await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire(f"c_{i}")})

    # Three passes, because the fairness rule still takes one job per capability per pass.
    for _ in range(3):
        await loop.poll_once()
    await asyncio.sleep(0.05)
    assert peak == 3, f"expected 3 jobs in flight at once, saw {peak}"

    release.set()
    await loop.drain()
    for i in range(3):
        assert await redis_client.get(f"res:c_{i}") is not None
    await http.aclose()


async def test_an_active_owner_holds_a_shared_node_to_one_job(redis_client):
    """The other half, and the one that matters more: the worker must not get greedy on
    a machine somebody is sitting at."""
    running = 0
    peak = 0
    release = asyncio.Event()

    async def slow(request: httpx.Request) -> httpx.Response:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await release.wait()
        running -= 1
        return httpx.Response(200, json=_OK)

    http, model = _model(slow)
    loop = _loop(redis_client, model, max_concurrent_jobs=4)
    loop.set_presence("active", "shared")
    await loop.ensure_groups()
    for i in range(3):
        await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire(f"s_{i}")})

    first = asyncio.ensure_future(loop.poll_once())
    await asyncio.sleep(0.05)
    assert peak == 1, f"a shared node with its owner present took {peak} jobs at once"

    release.set()
    await first
    await loop.drain()
    await http.aclose()


async def test_an_eviction_stops_every_job_not_just_the_newest(redis_client):
    """`evict` held ONE task. On a node running several, cancelling only the most recent
    would hold the GPU for exactly as long as the one it did stop — and an eviction is a
    person reaching for their own laptop."""
    started = asyncio.Event()
    count = 0

    async def hang(request: httpx.Request) -> httpx.Response:
        nonlocal count
        count += 1
        if count >= 2:
            started.set()
        await asyncio.sleep(30)
        return httpx.Response(200, json=_OK)

    http, model = _model(hang)
    loop = _loop(redis_client, model, max_concurrent_jobs=2)
    loop.set_presence("away", "dedicated")
    await loop.ensure_groups()
    for i in range(2):
        await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire(f"e_{i}")})

    for _ in range(2):
        await loop.poll_once()
    await asyncio.wait_for(started.wait(), timeout=5)

    assert loop.evict() is True
    await loop.drain()

    for i in range(2):
        assert await redis_client.get(f"res:e_{i}") is None, \
            "an evicted job must not be answered — both of them"
    pending = await redis_client.xpending(stream_key("8b-extract"), "cbk-workers")
    assert pending["pending"] == 2, "both still claimed, so the reaper requeues both"
    await http.aclose()


async def test_throughput_is_only_sampled_from_a_job_that_ran_alone(redis_client):
    """Both figures are wall-clock over tokens, so a job sharing the accelerator reads as
    slower than the node is — and `load_s` subtracts generation from wall time USING tps,
    so a deflated tps inflates every load estimate and over-warms every reservation.
    Under concurrency the honest answer is fewer samples, not faster-looking ones."""
    release = asyncio.Event()

    async def slow(request: httpx.Request) -> httpx.Response:
        await release.wait()
        return httpx.Response(200, json=_OK)

    http, model = _model(slow)
    loop = _loop(redis_client, model, max_concurrent_jobs=2)
    loop.set_presence("away", "dedicated")
    await loop.ensure_groups()
    for i in range(2):
        await redis_client.xadd(stream_key("8b-extract"), {"job": _job_wire(f"t_{i}")})

    for _ in range(2):
        await loop.poll_once()
    await asyncio.sleep(0.02)
    release.set()
    await loop.drain()

    assert loop.jobs_done == 2, "both jobs ran"
    assert loop.tps is None, "neither job had the machine to itself, so neither measured"
    await http.aclose()
