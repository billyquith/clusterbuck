"""The OpenAI-compatible inference call (protocols.md §3)."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from cbk_worker.config import WorkerConfig
from cbk_worker.model_client import ModelClient, _refuse_substituted_model
from cbk_worker.models import Job

_OK = {
    "id": "c1", "object": "chat.completion",
    "choices": [{"index": 0, "message": {"role": "assistant", "content": "pong"},
                 "finish_reason": "stop"}],
    "usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4},
}


def _capture() -> tuple[httpx.MockTransport, list[dict[str, Any]]]:
    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json
        seen.append(json.loads(request.content))
        return httpx.Response(200, json=_OK)

    return httpx.MockTransport(handler), seen


def _job(**kw) -> Job:
    base = {"id": "job_1", "capability": "8b-extract", "result_key": "res:job_1"}
    return Job.from_wire({**base, **kw})


async def test_prompt_becomes_a_user_message_and_usage_is_returned():
    transport, seen = _capture()
    cfg = WorkerConfig(model_name="llama3.2:3b")
    async with httpx.AsyncClient(transport=transport) as http:
        completion, usage = await ModelClient(http, cfg).complete(_job(prompt="ping"))
    assert seen[0]["messages"] == [{"role": "user", "content": "ping"}]
    assert seen[0]["stream"] is False
    assert completion["choices"][0]["message"]["content"] == "pong"
    assert usage["total_tokens"] == 4


async def test_explicit_messages_win_over_prompt():
    transport, seen = _capture()
    async with httpx.AsyncClient(transport=transport) as http:
        await ModelClient(http, WorkerConfig()).complete(_job(
            prompt="ignored",
            messages=[{"role": "system", "content": "be terse"},
                      {"role": "user", "content": "hello"}]))
    assert [m["role"] for m in seen[0]["messages"]] == ["system", "user"]
    assert seen[0]["messages"][1]["content"] == "hello"


async def test_a_job_can_pin_the_model_and_the_pin_wins():
    """Load-bearing for the eval harness (model-evaluation.md).

    The harness dispatches eval work as ordinary jobs with the artifact under measurement
    pinned per job. Params are therefore applied AFTER the configured model — if that order
    were reversed the node's default model would answer and the wrong artifact would be
    scored, silently and plausibly.
    """
    transport, seen = _capture()
    cfg = WorkerConfig(model_name="llama3.2:3b")     # what this node normally serves
    async with httpx.AsyncClient(transport=transport) as http:
        await ModelClient(http, cfg).complete(
            _job(prompt="x", params={"model": "qwen2.5:32b", "temperature": 0}))
    assert seen[0]["model"] == "qwen2.5:32b", "the per-job pin was overwritten by config"
    assert seen[0]["temperature"] == 0


async def test_config_model_is_used_when_the_job_pins_nothing():
    transport, seen = _capture()
    async with httpx.AsyncClient(transport=transport) as http:
        await ModelClient(http, WorkerConfig(model_name="llama3.2:3b")).complete(
            _job(prompt="x"))
    assert seen[0]["model"] == "llama3.2:3b"


async def test_response_format_is_dropped_as_a_hint_only():
    transport, seen = _capture()
    async with httpx.AsyncClient(transport=transport) as http:
        await ModelClient(http, WorkerConfig()).complete(
            _job(prompt="x", params={"response_format": {"type": "json_object"},
                                     "max_tokens": 64}))
    # Not every model server understands it, and passing it through made compliant servers
    # 400 (protocols.md §3). max_tokens still goes.
    assert "response_format" not in seen[0]
    assert seen[0]["max_tokens"] == 64


async def test_http_error_propagates_so_the_loop_can_record_a_failed_result():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="model server exploded")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        try:
            await ModelClient(http, WorkerConfig()).complete(_job(prompt="x"))
        except httpx.HTTPStatusError:
            return
    raise AssertionError("a 500 from the model server was swallowed")


async def test_url_is_built_from_the_openai_base():
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json=_OK)

    cfg = WorkerConfig(model_server_url="http://localhost:11434/v1/")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        await ModelClient(http, cfg).complete(_job(prompt="x"))
    assert seen == ["http://localhost:11434/v1/chat/completions"]


async def test_params_cannot_hijack_the_request_envelope():
    """`params` is forwarded verbatim so the worker stays out of the way of whatever the
    model server supports — but not the five keys that are the envelope rather than an
    inference knob. `stream` is the one that bites: this path reads a single JSON body, so a
    streamed response fails to parse and the job comes back `failed` for what the client
    meant as a preference."""
    transport, seen = _capture()
    async with httpx.AsyncClient(transport=transport) as http:
        await ModelClient(http, WorkerConfig(model_name="cfg-model")).complete(_job(
            prompt="ping",
            params={"stream": True, "messages": [{"role": "user", "content": "hijacked"}],
                    "response_format": "json_object", "temperature": 0.2}))
    sent = seen[0]
    assert sent["stream"] is False
    assert sent["messages"] == [{"role": "user", "content": "ping"}]
    assert "response_format" not in sent
    assert sent["temperature"] == 0.2      # real params still pass straight through


# --- this node's own model server auth (worker fix, ADR 30) ---

def _capture_headers() -> tuple[httpx.MockTransport, list[httpx.Headers]]:
    seen: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers)
        return httpx.Response(200, json=_OK)

    return httpx.MockTransport(handler), seen


async def test_no_authorization_header_when_unconfigured(monkeypatch):
    """Unchanged default: a plain local model server (e.g. Ollama) needs no key, and never
    got one before this fix either — only its absence (never ANY header) was the bug."""
    monkeypatch.delenv("CBK_MODEL_SERVER_API_KEY", raising=False)
    transport, seen = _capture_headers()
    async with httpx.AsyncClient(transport=transport) as http:
        await ModelClient(http, WorkerConfig()).complete(_job(prompt="x"))
    assert "authorization" not in seen[0]


async def test_authorization_header_sent_when_configured(monkeypatch):
    """The actual gap: a node's own model server behind an authenticated gateway could not
    be reached at all, because no code path ever sent an Authorization header."""
    monkeypatch.setenv("CBK_MODEL_SERVER_API_KEY", "sk-node-local")
    transport, seen = _capture_headers()
    async with httpx.AsyncClient(transport=transport) as http:
        await ModelClient(http, WorkerConfig()).complete(_job(prompt="x"))
    assert seen[0]["authorization"] == "Bearer sk-node-local"


async def test_job_params_cannot_supply_or_override_the_key_or_base(monkeypatch):
    """A job's `params` must never be able to redirect this call to a different endpoint or
    substitute a different key — that would let any client exfiltrate whatever key is
    configured for this node, or make the worker call an arbitrary host."""
    monkeypatch.setenv("CBK_MODEL_SERVER_API_KEY", "sk-node-local")
    transport, seen = _capture()
    header_transport, header_seen = _capture_headers()

    async with httpx.AsyncClient(transport=transport) as http:
        await ModelClient(http, WorkerConfig()).complete(_job(
            prompt="x", params={"api_key": "sk-attacker", "api_base": "https://evil.invalid"}))
    assert "api_key" not in seen[0]
    assert "api_base" not in seen[0]

    async with httpx.AsyncClient(transport=header_transport) as http:
        await ModelClient(http, WorkerConfig()).complete(_job(
            prompt="x", params={"api_key": "sk-attacker"}))
    # The node's OWN configured key still wins — the job's params never substitute one.
    assert header_seen[0]["authorization"] == "Bearer sk-node-local"


# --- the model server itself substituting (found live) ----------------------------------


def _body(model):
    return {"model": model, "choices": [{"message": {"role": "assistant", "content": "x"}}]}


def test_a_substituted_model_is_refused():
    """LM Studio returns HTTP 200 for a model it does not have — including an id that
    exists nowhere — and answers with whatever is loaded. Observed live: an EMBEDDING model
    recorded 7.0 at code, which were another model's answers filed under the wrong name."""
    with pytest.raises(RuntimeError) as e:
        _refuse_substituted_model("text-embedding-nomic-embed-text-v1.5",
                                  _body("qwen/qwen3.5-9b"))
    assert "qwen/qwen3.5-9b" in str(e.value) and "text-embedding" in str(e.value)


def test_the_model_that_was_asked_for_passes():
    assert _refuse_substituted_model("qwen/qwen3.5-9b", _body("qwen/qwen3.5-9b")) is None


def test_an_implicit_latest_tag_is_not_a_substitution():
    assert _refuse_substituted_model("llama3.2", _body("llama3.2:latest")) is None
    assert _refuse_substituted_model("llama3.2:latest", _body("llama3.2")) is None


def test_a_server_that_echoes_nothing_is_not_accused():
    """Not every server echoes `model`, and absent is unknown rather than wrong — the same
    rule every other gate here follows."""
    assert _refuse_substituted_model("m:7b", {"choices": []}) is None
    assert _refuse_substituted_model("m:7b", {"model": ""}) is None
    assert _refuse_substituted_model("m:7b", {"model": None}) is None
    assert _refuse_substituted_model("m:7b", "not a dict") is None


async def test_a_substituting_server_fails_the_job_rather_than_answering():
    """End to end through the client: the reply parses fine and says 200, and is still
    refused, because the name it came back under is not the one whose ability cleared the
    job's bar."""
    cfg = WorkerConfig(redis_url="redis://x", model_name="configured:1b",
                       model_server_url="http://ms/v1")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_body("something-else:70b"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        job = Job.from_wire({"id": "j", "created_at": "t", "capability": "c",
                             "prompt": "hi", "params": {"model": "pinned:7b"},
                             "result_key": "r"})
        with pytest.raises(RuntimeError, match="something-else:70b"):
            await ModelClient(http, cfg).complete(job)
