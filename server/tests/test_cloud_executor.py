"""The coordinator's in-process cloud executor (ADR 30): no provider key ever reaches a
worker, so cloud-backed jobs are drained and executed here instead of over the wire.
"""

from __future__ import annotations

import json

import pytest

from clusterbuck.cloud_executor import (
    CONSUMER_ID,
    CloudExecutor,
    cloud_capabilities,
    provider_of,
)
from clusterbuck.fleet import CapabilitySpec, Fleet
from clusterbuck.ids import new_ids
from clusterbuck.models import JobRecord, Message, Privacy, Urgency
from clusterbuck.queue import Queue

CAP = "claude-sonnet"
ARTIFACT = "anthropic/claude-3-5-sonnet-20241022"


class _FakeResponse:
    def __init__(self, body: dict) -> None:
        self._body = body

    def model_dump(self) -> dict:
        return self._body


@pytest.fixture()
def fleet(monkeypatch) -> Fleet:
    monkeypatch.setenv("CBK_TEST_PROVIDER_KEY", "sk-test-123")
    return Fleet(capabilities={
        CAP: CapabilitySpec(queue=f"q:{CAP}", model=ARTIFACT, cloud=True,
                            api_key_env="CBK_TEST_PROVIDER_KEY",
                            price_in_per_1k=0.003, price_out_per_1k=0.015),
    })


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


def _enqueue(queue_obj, *, prompt="hi", params=None, privacy=Privacy.cloud_ok) -> JobRecord:
    job_id, result_key = new_ids()
    record = JobRecord(
        id=job_id, created_at="t", capability=CAP,
        messages=[Message(role="user", content=prompt)],
        params=params or {}, urgency=Urgency.waitable, privacy=privacy,
        result_key=result_key,
    )
    return record


def test_cloud_capabilities_lists_only_no_host_cloud_entries(fleet):
    fleet.capabilities["hosted"] = CapabilitySpec(
        queue="q:hosted", model_server="https://gw.example.invalid/v1", model="m", cloud=True)
    fleet.capabilities["local"] = CapabilitySpec(
        queue="q:local", model_server="http://localhost:1/v1", model="m")
    assert cloud_capabilities(fleet) == [CAP]


def test_provider_of_parses_the_litellm_prefix():
    assert provider_of("anthropic/claude-3-5-sonnet") == "anthropic"
    assert provider_of("no-prefix-model") == "cloud"


async def test_successful_call_writes_done_result_and_acks(fleet, queue, monkeypatch):
    record = _enqueue(queue)
    await queue.enqueue(record.to_wire())

    captured = {}

    async def fake_acompletion(**kwargs):
        captured.update(kwargs)
        return _FakeResponse({
            "model": ARTIFACT,
            "choices": [{"message": {"role": "assistant", "content": "hello"}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
        })

    monkeypatch.setattr("clusterbuck.cloud_executor.litellm.acompletion", fake_acompletion)

    executor = CloudExecutor(queue, fleet, consumer_group="cbk-workers")
    assert await executor.poll_once() is True

    assert captured["model"] == ARTIFACT
    assert captured["api_key"] == "sk-test-123"
    assert captured["messages"] == [{"role": "user", "content": "hi"}]
    assert captured["stream"] is False

    result = await queue.read_result(record.result_key)
    assert result["status"] == "done"
    assert result["worker"] == "cloud:anthropic"
    assert result["usage"]["prompt_tokens"] == 5
    assert result["completion"]["choices"][0]["message"]["content"] == "hello"

    stats = await queue.depth(CAP, "cbk-workers")
    assert stats["pending"] == 0  # acked


async def test_params_model_override_pins_the_artifact_under_test(fleet, queue, monkeypatch):
    """The eval harness pins an exact artifact via params.model — the executor must honour
    that override the same way model_client.py does for a real worker."""
    pinned = "anthropic/claude-3-5-haiku-20241022"
    record = _enqueue(queue, params={"model": pinned})
    await queue.enqueue(record.to_wire())

    captured = {}

    async def fake_acompletion(**kwargs):
        captured.update(kwargs)
        return _FakeResponse({"model": pinned, "choices": [], "usage": None})

    monkeypatch.setattr("clusterbuck.cloud_executor.litellm.acompletion", fake_acompletion)
    await CloudExecutor(queue, fleet, consumer_group="cbk-workers").poll_once()
    assert captured["model"] == pinned


async def test_job_params_cannot_override_the_api_key_or_base(fleet, queue, monkeypatch):
    """A job's own params must never redirect this call to a different endpoint or key —
    only the capability's own api_key_env, resolved server-side, is used (ADR 30)."""
    record = _enqueue(queue, params={
        "api_key": "sk-attacker-supplied", "api_base": "https://evil.example.invalid",
        "temperature": 0.2,
    })
    await queue.enqueue(record.to_wire())

    captured = {}

    async def fake_acompletion(**kwargs):
        captured.update(kwargs)
        return _FakeResponse({"model": ARTIFACT, "choices": [], "usage": None})

    monkeypatch.setattr("clusterbuck.cloud_executor.litellm.acompletion", fake_acompletion)
    await CloudExecutor(queue, fleet, consumer_group="cbk-workers").poll_once()

    assert captured["api_key"] == "sk-test-123"
    assert "api_base" not in captured
    assert captured["temperature"] == 0.2  # ordinary inference params still pass through


async def test_missing_api_key_fails_the_job_without_calling_the_provider(fleet, queue, monkeypatch):
    monkeypatch.delenv("CBK_TEST_PROVIDER_KEY", raising=False)
    record = _enqueue(queue)
    await queue.enqueue(record.to_wire())

    called = False

    async def fake_acompletion(**kwargs):
        nonlocal called
        called = True
        raise AssertionError("must not call the provider without a key")

    monkeypatch.setattr("clusterbuck.cloud_executor.litellm.acompletion", fake_acompletion)
    await CloudExecutor(queue, fleet, consumer_group="cbk-workers").poll_once()

    assert called is False
    result = await queue.read_result(record.result_key)
    assert result["status"] == "failed"
    assert "CBK_TEST_PROVIDER_KEY" in result["error"]


async def test_ensure_groups_and_consumer_id(fleet, queue):
    executor = CloudExecutor(queue, fleet, consumer_group="cbk-workers")
    await executor.ensure_groups()
    assert executor.capabilities == [CAP]
    assert CONSUMER_ID  # a fixed, non-empty consumer name


# --- a provider call that outlived its own claim (B2) ---------------------------------


async def test_a_late_copy_does_not_overwrite_an_answer_that_already_landed(
    fleet, queue, monkeypatch
):
    """The same first-writer-wins rule as the worker's loop, for the same reason.

    This executor is not immune to being reclaimed out from under itself: a provider call
    slower than `reaper_min_idle_ms` is all it takes for the reaper to requeue the entry
    and something else to answer. Whichever copy finishes second must not overwrite a
    terminal answer a client may already have read.
    """
    record = _enqueue(queue)
    await queue.enqueue(record.to_wire())

    async def fake_acompletion(**kwargs):
        return _FakeResponse({
            "model": ARTIFACT,
            "choices": [{"message": {"role": "assistant", "content": "late"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        })

    monkeypatch.setattr("clusterbuck.cloud_executor.litellm.acompletion", fake_acompletion)
    await queue.write_result(record.result_key, {
        "job_id": record.id, "status": "done", "worker": "node-other",
        "completion": {"choices": [{"message": {
            "role": "assistant", "content": "answered first"}}]},
    })

    executor = CloudExecutor(queue, fleet, consumer_group="cbk-workers", log=lambda _: None)
    assert await executor.poll_once() is True

    result = await queue.read_result(record.result_key)
    assert result["worker"] == "node-other", "the first answer must survive"
    assert result["completion"]["choices"][0]["message"]["content"] == "answered first"
    # Acked regardless: a refused write means somebody else answered, so leaving the entry
    # pending would only give the reaper something to churn on.
    summary = await queue.client.xpending(f"q:{CAP}", "cbk-workers")
    assert summary["pending"] == 0


async def test_write_result_offers_both_modes(queue):
    """The mechanism, not the policy. `write_result` still has an unconditional mode —
    the last-write-wins default — and an `only_if_absent` one.

    This used to be named ..._are_still_unconditional and justified the reaper and the
    backstops passing the default, on the grounds that "the reaper reads the blob first".
    That reasoning was wrong: a check separated from its write by an await is not atomic,
    and those paths could overwrite a real completion with `failed`. Every terminal write
    is now `only_if_absent`; the unconditional mode survives only as the default here.
    """
    await queue.write_result("res:x", {"job_id": "x", "status": "done", "worker": "a"})
    await queue.write_result("res:x", {"job_id": "x", "status": "failed", "worker": "b"})

    assert (await queue.read_result("res:x"))["worker"] == "b"

    assert await queue.write_result(
        "res:y", {"job_id": "y", "status": "done", "worker": "a"}, only_if_absent=True)
    assert not await queue.write_result(
        "res:y", {"job_id": "y", "status": "done", "worker": "b"}, only_if_absent=True)
    assert (await queue.read_result("res:y"))["worker"] == "a"


async def test_job_params_cannot_reach_litellm_through_an_alias(fleet, queue, monkeypatch):
    """The hole the `api_key`/`api_base` test above could not see.

    Params are splatted as KWARGS here (`litellm.acompletion(**request)`), not dropped
    into a JSON body the way the worker does it — so any key a job supplies becomes a
    named argument to LiteLLM. `litellm.completion` declares `base_url` and
    `extra_headers` and takes `**kwargs` besides, so the old five-key denylist blocked
    `api_base` while `base_url` went straight through: a submitted job could point this
    coordinator at its own endpoint and be handed the real provider key.

    Hence an allowlist. A denylist would have to enumerate every alias LiteLLM has now
    and every one it grows later; this fails closed on both.
    """
    record = _enqueue(queue, params={
        "base_url": "https://evil.example.invalid/v1",
        "extra_headers": {"x-exfil": "https://evil.example.invalid"},
        "custom_llm_provider": "attacker",
        "api_version": "2020-01-01",
        "mock_response": "not the model's words",
        "temperature": 0.2,
    })
    await queue.enqueue(record.to_wire())

    captured = {}

    async def fake_acompletion(**kwargs):
        captured.update(kwargs)
        return _FakeResponse({"model": ARTIFACT, "choices": [], "usage": None})

    monkeypatch.setattr("clusterbuck.cloud_executor.litellm.acompletion", fake_acompletion)
    await CloudExecutor(queue, fleet, consumer_group="cbk-workers").poll_once()

    for leaked in ("base_url", "extra_headers", "custom_llm_provider", "api_version",
                   "mock_response"):
        assert leaked not in captured, f"{leaked} reached litellm"
    assert captured["api_key"] == "sk-test-123"
    assert captured["temperature"] == 0.2, "ordinary inference params still pass through"
