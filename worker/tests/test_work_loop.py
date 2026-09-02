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
