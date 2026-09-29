"""Sync plane (protocols.md §1a): capability alias → LiteLLM → model server.

Routes through a real LiteLLM Router to the zero-weight fake model server, proving the
adopted sync gateway is wired correctly without reimplementing OpenAI routing (ADR 5).
The worker is deliberately absent — the sync path bypasses it.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
from clusterbuck.api import create_app
from clusterbuck.fleet import CapabilitySpec, Fleet
from clusterbuck.sync import build_router
from fastapi.testclient import TestClient

FAKE_SERVER = Path(__file__).resolve().parents[1] / "tools" / "fake_model_server.py"


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture()
def fake_model():
    port = _free_port()
    proc = subprocess.Popen([sys.executable, str(FAKE_SERVER), "--port", str(port)])
    base = f"http://127.0.0.1:{port}/v1"
    try:
        for _ in range(50):
            try:
                httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=0.5)
                break
            except httpx.HTTPError:
                time.sleep(0.1)
        else:
            raise RuntimeError("fake model server did not start")
        yield base
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.fixture()
def sync_client(fake_model, tmp_path):
    fleet = tmp_path / "fleet.yaml"
    fleet.write_text(
        "nodes: []\n"
        "capabilities:\n"
        "  8b-extract:\n"
        "    queue: 'q:8b-extract'\n"
        f"    model_server: '{fake_model}'\n"
        "    model: 'fake'\n"
    )
    # Redis is untouched by the sync path; a dummy URL is fine (client connects lazily).
    app = create_app(
        redis_url="redis://localhost:6379/15",
        db_path=str(tmp_path / "t.db"),
        fleet_path=str(fleet),
        start_scheduler=False,
    )
    with TestClient(app) as c:
        yield c


def test_chat_completions_routes_to_model_server(sync_client):
    resp = sync_client.post(
        "/v1/chat/completions",
        json={
            "model": "8b-extract",
            "messages": [{"role": "user", "content": "sync ping"}],
            "temperature": 0.1,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["choices"][0]["message"]["content"] == "[fake:fake] echo: sync ping"
    assert "usage" in body


def test_models_lists_capabilities(sync_client):
    data = sync_client.get("/v1/models").json()
    ids = {m["id"] for m in data["data"]}
    assert ids == {"8b-extract"}


def test_unknown_capability_is_the_clients_mistake_not_an_outage(sync_client):
    resp = sync_client.post(
        "/v1/chat/completions",
        json={"model": "does-not-exist", "messages": [{"role": "user", "content": "x"}]},
    )
    assert resp.status_code == 404
    err = resp.json()["error"]
    assert err["code"] == "capability_not_found"
    assert err["retryable"] is False
    assert err["param"] == "model"


def _app_with(tmp_path, capabilities: str):
    fleet = tmp_path / "fleet.yaml"
    fleet.write_text("nodes: []\ncapabilities:\n" + capabilities)
    return create_app(redis_url="redis://localhost:6379/15", db_path=str(tmp_path / "t.db"),
                      fleet_path=str(fleet), start_scheduler=False)


def test_a_dead_model_server_reads_as_unreachable_not_as_an_error(tmp_path):
    """LiteLLM reports a refused connection as InternalServerError; the cause is what counts."""
    app = _app_with(tmp_path, f"  gone:\n    queue: 'q:gone'\n"
                              f"    model_server: 'http://127.0.0.1:{_free_port()}/v1'\n"
                              f"    model: 'fake'\n")
    with TestClient(app) as c:
        resp = c.post("/v1/chat/completions",
                      json={"model": "gone", "messages": [{"role": "user", "content": "x"}]})
    assert resp.status_code == 502
    err = resp.json()["error"]
    assert err["code"] == "model_server_unreachable"
    assert err["retryable"] is True
    assert (err["capability"], err["model"]) == ("gone", "fake")


def test_a_dead_server_says_whether_async_is_worth_trying(tmp_path):
    """A node that can be woken makes `POST /jobs` a real alternative; no node, no hint."""
    dead = f"http://127.0.0.1:{_free_port()}/v1"
    app = _app_with(tmp_path, f"  gone:\n    queue: 'q:gone'\n    model_server: '{dead}'\n"
                              f"    model: 'fake'\n"
                              f"  lone:\n    queue: 'q:lone'\n    model_server: '{dead}'\n"
                              f"    model: 'fake'\n")
    app_text = (tmp_path / "fleet.yaml").read_text().replace(
        "nodes: []", "nodes:\n  - {id: sleeper, mac: 'aa:bb:cc:dd:ee:ff', "
                     "capabilities: [gone]}")
    (tmp_path / "fleet.yaml").write_text(app_text)
    app = create_app(redis_url="redis://localhost:6379/15", db_path=str(tmp_path / "u.db"),
                     fleet_path=str(tmp_path / "fleet.yaml"), start_scheduler=False)
    msg = [{"role": "user", "content": "x"}]
    with TestClient(app) as c:
        woken = c.post("/v1/chat/completions", json={"model": "gone", "messages": msg})
        alone = c.post("/v1/chat/completions", json={"model": "lone", "messages": msg})
    assert woken.json()["error"]["use_async"] is True
    assert alone.json()["error"].get("use_async") is not True


def test_a_failing_model_server_reads_as_an_error(tmp_path):
    port = _free_port()
    proc = subprocess.Popen([sys.executable, str(FAKE_SERVER), "--port", str(port),
                             "--fail-chat", "500"])
    try:
        for _ in range(50):
            try:
                httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=0.5)
                break
            except httpx.HTTPError:
                time.sleep(0.1)
        app = _app_with(tmp_path, f"  sick:\n    queue: 'q:sick'\n"
                                  f"    model_server: 'http://127.0.0.1:{port}/v1'\n"
                                  f"    model: 'fake'\n")
        with TestClient(app) as c:
            resp = c.post("/v1/chat/completions", json={
                "model": "sick", "messages": [{"role": "user", "content": "x"}]})
    finally:
        proc.terminate()
        proc.wait(timeout=5)
    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "model_server_error"


def test_the_stock_openai_sdk_reads_the_code(sync_client):
    """The promise of §1a: an unmodified SDK gets `.code` out of a refusal."""
    import openai

    resp = sync_client.post(
        "/v1/chat/completions",
        json={"model": "nope", "messages": [{"role": "user", "content": "x"}]},
    )
    err = openai.OpenAI(api_key="x", base_url="http://unused")._make_status_error(
        "refused", body=resp.json(), response=httpx.Response(
            resp.status_code, request=httpx.Request("POST", "http://unused")))
    assert isinstance(err, openai.NotFoundError)
    assert err.code == "capability_not_found"


def test_a_registered_but_unservable_alias_is_misconfigured(monkeypatch, tmp_path):
    monkeypatch.delenv("CBK_TEST_PROVIDER_KEY", raising=False)
    app = _app_with(tmp_path, "  cheap:\n    queue: 'q:cheap'\n"
                              "    model_server: 'http://127.0.0.1:9/v1'\n    model: 'fake'\n"
                              "  keyless:\n    queue: 'q:keyless'\n    model: 'anthropic/x'\n"
                              "    cloud: true\n    api_key_env: CBK_TEST_PROVIDER_KEY\n")
    with TestClient(app) as c:
        resp = c.post("/v1/chat/completions",
                      json={"model": "keyless", "messages": [{"role": "user", "content": "x"}]})
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "capability_misconfigured"


def test_build_router_does_not_retry_behind_the_clients_back():
    fleet = Fleet(capabilities={
        "a": CapabilitySpec(queue="q:a", model="fake", model_server="http://127.0.0.1:9/v1"),
    })
    assert build_router(fleet).num_retries == 0


# --- registered provider accounts on the sync plane (ADR 30) ---

def test_build_router_excludes_unkeyed_provider_account(monkeypatch):
    monkeypatch.delenv("CBK_TEST_PROVIDER_KEY", raising=False)
    fleet = Fleet(capabilities={
        "claude-sonnet": CapabilitySpec(queue="q:claude-sonnet", model="anthropic/claude-x",
                                        cloud=True, api_key_env="CBK_TEST_PROVIDER_KEY"),
    })
    router = build_router(fleet)
    assert router is None  # no other capability, and this one has no usable key


def test_build_router_includes_keyed_provider_account(monkeypatch):
    monkeypatch.setenv("CBK_TEST_PROVIDER_KEY", "sk-test-123")
    fleet = Fleet(capabilities={
        "claude-sonnet": CapabilitySpec(queue="q:claude-sonnet", model="anthropic/claude-x",
                                        cloud=True, api_key_env="CBK_TEST_PROVIDER_KEY"),
    })
    router = build_router(fleet)
    assert router is not None
    params = router.model_list[0]["litellm_params"]
    # No api_base and no "openai/" prefix: a no-host capability is called natively, not
    # treated as a local OpenAI-compatible endpoint.
    assert params["model"] == "anthropic/claude-x"
    assert params["api_key"] == "sk-test-123"
    assert params.get("api_base") is None


@pytest.mark.parametrize("bad_messages", ["hello", [1, 2], [{"role": "user"}], [], None])
def test_a_malformed_messages_body_is_the_clients_mistake_not_an_outage(
    sync_client, bad_messages
):
    """LiteLLM's own internal TypeError/AttributeError on a wrongly-shaped body got
    wrapped as an APIConnectionError and misclassified as the model server being
    unreachable — 502, `retryable: true` — so a client obeying `retryable` retried a
    request that was always going to fail, believing an outage was happening on the LAN
    when the request itself was malformed. This never reaches the model server at all
    now: the fake server would answer 200 to anything, so a non-422 here would mean the
    request got through."""
    resp = sync_client.post("/v1/chat/completions",
                            json={"model": "8b-extract", "messages": bad_messages})
    assert resp.status_code == 422, resp.text
    err = resp.json()["error"]
    assert err["code"] == "invalid_request"
    assert err["retryable"] is False
    assert err["param"] == "messages"


def test_response_format_and_tools_of_the_wrong_type_are_rejected(sync_client):
    msgs = [{"role": "user", "content": "x"}]
    for bad_body in (
        {"model": "8b-extract", "messages": msgs, "response_format": 5},
        {"model": "8b-extract", "messages": msgs, "tools": "not-a-list"},
    ):
        resp = sync_client.post("/v1/chat/completions", json=bad_body)
        assert resp.status_code == 422, resp.text
        assert resp.json()["error"]["code"] == "invalid_request"


def test_invalid_json_answers_in_the_envelope_not_a_bare_500(sync_client):
    resp = sync_client.post(
        "/v1/chat/completions",
        content=b"{not json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["error"]["code"] == "invalid_request"


def test_streaming_is_refused_rather_than_silently_answered_wrong(sync_client):
    """M1 is non-streaming. Silently ignoring `stream: true` and answering with a normal
    JSON body meant the stock SDK's streaming iterator yielded zero chunks and raised
    nothing — a silently wrong answer, not a refusal."""
    resp = sync_client.post(
        "/v1/chat/completions",
        json={"model": "8b-extract", "messages": [{"role": "user", "content": "x"}],
              "stream": True},
    )
    assert resp.status_code == 422, resp.text
    err = resp.json()["error"]
    assert err["code"] == "invalid_request"
    assert err["param"] == "stream"


def test_max_completion_tokens_is_forwarded(sync_client):
    """Newer SDKs send this in place of `max_tokens`; dropped, output length was
    unbounded rather than honouring what the client actually asked for."""
    router = sync_client.app.state.sync_router
    seen = {}
    real = router.acompletion

    async def capture(*args, **kwargs):
        seen.update(kwargs)
        return await real(*args, **kwargs)

    router.acompletion = capture
    try:
        resp = sync_client.post(
            "/v1/chat/completions",
            json={"model": "8b-extract", "messages": [{"role": "user", "content": "x"}],
                  "max_completion_tokens": 5},
        )
    finally:
        router.acompletion = real
    assert resp.status_code == 200, resp.text
    assert seen.get("max_completion_tokens") == 5


def test_sync_disabled_without_fleet(tmp_path):
    app = create_app(
        redis_url="redis://localhost:6379/15",
        db_path=str(tmp_path / "t.db"),
        fleet_path=str(tmp_path / "missing.yaml"),
        start_scheduler=False,
    )
    with TestClient(app) as c:
        resp = c.post(
            "/v1/chat/completions",
            json={"model": "8b-extract", "messages": [{"role": "user", "content": "x"}]},
        )
        assert resp.status_code == 503
        assert resp.json()["error"]["code"] == "fleet_not_configured"


# --- metering and the budget gate (design.md §8) ----------------------------------------


def test_a_sync_completion_is_metered_without_its_text(sync_client):
    """The sync plane used to be invisible to cost tracking: no row, no avoided spend, no
    budget. A row now — and nothing in it that a person wrote or a model said."""
    secret, store = "the-launch-code-is-4321", sync_client.app.state.store
    resp = sync_client.post(
        "/v1/chat/completions",
        json={"model": "8b-extract", "messages": [{"role": "user", "content": secret}]})
    assert resp.status_code == 200, resp.text
    rows = store.recent_usage()
    assert len(rows) == 1
    row = rows[0]
    assert row.job_id.startswith("sync-")
    assert (row.capability, row.venue, row.node, row.outcome) == (
        "8b-extract", "local", "sync", "done")
    assert row.tokens_in > 0
    answer = resp.json()["choices"][0]["message"]["content"]
    for value in row.model_dump().values():
        assert secret not in str(value) and (not answer or answer not in str(value))


def _cloud_fleet_client(fake_model, tmp_path, monkeypatch):
    monkeypatch.setenv("CBK_T_SYNC_KEY", "sk-test")
    fleet = tmp_path / "fleet.yaml"
    fleet.write_text(
        "capabilities:\n"
        "  8b-extract:\n"
        f"    model_server: '{fake_model}'\n"
        "    model: 'fake'\n"
        "    cloud_fallback: claude\n"
        "  claude:\n"
        "    model: 'anthropic/claude-sonnet-5'\n"
        "    cloud: true\n"
        "    api_key_env: CBK_T_SYNC_KEY\n"
    )
    app = create_app(redis_url="redis://localhost:6379/15", db_path=str(tmp_path / "t.db"),
                     fleet_path=str(fleet), start_scheduler=False)
    return TestClient(app)


def _spend(store, amount):
    from datetime import UTC, datetime

    store.record_usage(job_id="spent", ts="t", capability="claude", model=None, node=None,
                       venue="cloud", tokens_in=0, tokens_out=0, outcome="done",
                       cost=amount, day=datetime.now(UTC).strftime("%Y-%m-%d"))


def test_the_local_only_router_has_no_way_to_spend(fake_model, tmp_path, monkeypatch):
    with _cloud_fleet_client(fake_model, tmp_path, monkeypatch) as c:
        full, local = c.app.state.sync_router, c.app.state.sync_router_local
        assert set(full.get_model_names()) == {"8b-extract", "claude"}
        assert full.fallbacks == [{"8b-extract": ["claude"]}]
        assert local.get_model_names() == ["8b-extract"]
        assert not local.fallbacks


def test_an_exhausted_budget_refuses_cloud_and_serves_locally(fake_model, tmp_path,
                                                              monkeypatch):
    import dataclasses

    from clusterbuck.config import settings

    monkeypatch.setattr("clusterbuck.config.settings",
                        dataclasses.replace(settings, cloud_budget_monthly=1.0))
    with _cloud_fleet_client(fake_model, tmp_path, monkeypatch) as c:
        _spend(c.app.state.store, 5.0)
        full = c.app.state.sync_router

        async def must_not_be_called(*a, **kw):  # the router that can reach a provider
            raise AssertionError("the full router was used past the budget")

        full.acompletion = must_not_be_called
        msg = [{"role": "user", "content": "x"}]
        resp = c.post("/v1/chat/completions", json={"model": "claude", "messages": msg})
        assert resp.status_code == 422, resp.text
        assert resp.json()["error"]["code"] == "cloud_budget_exhausted"
        resp = c.post("/v1/chat/completions", json={"model": "8b-extract", "messages": msg})
        assert resp.status_code == 200, resp.text


def test_a_fallback_answer_is_metered_to_the_tier_that_answered(fake_model, tmp_path,
                                                               monkeypatch):
    """After a fallback the answering deployment is not the one the client named, and
    only the former's price (and venue) is true."""
    with _cloud_fleet_client(fake_model, tmp_path, monkeypatch) as c:
        router = c.app.state.sync_router

        class _Resp:
            def __init__(self):
                self._hidden_params = {
                    "additional_headers": {"x-litellm-model-group": "claude"}}

            def model_dump(self):
                return {"id": "x", "object": "chat.completion", "created": 0,
                        "model": "claude-sonnet-5",
                        "choices": [{"index": 0, "finish_reason": "stop",
                                     "message": {"role": "assistant", "content": "hi"}}],
                        "usage": {"prompt_tokens": 1000, "completion_tokens": 100,
                                  "total_tokens": 1100}}

        async def answered_by_fallback(*a, **kw):
            return _Resp()

        router.acompletion = answered_by_fallback
        resp = c.post("/v1/chat/completions",
                      json={"model": "8b-extract", "messages": [{"role": "user",
                                                                 "content": "x"}]})
        assert resp.status_code == 200, resp.text
        row = c.app.state.store.recent_usage()[0]
        assert (row.capability, row.venue, row.node, row.cost_source) == (
            "claude", "cloud", "cloud:anthropic", "litellm")
        assert row.cost > 0


def test_json_mode_is_sent_as_the_equivalent_schema(sync_client):
    """LM Studio 400s `{"type": "json_object"}`; the same meaning goes out as a schema,
    exactly as the worker sends it on the async plane."""
    router = sync_client.app.state.sync_router
    seen, real = {}, router.acompletion

    async def capture(*args, **kwargs):
        seen.update(kwargs)
        return await real(*args, **kwargs)

    router.acompletion = capture
    try:
        resp = sync_client.post("/v1/chat/completions", json={
            "model": "8b-extract", "messages": [{"role": "user", "content": "x"}],
            "response_format": {"type": "json_object"}})
    finally:
        router.acompletion = real
    assert resp.status_code == 200, resp.text
    assert seen["response_format"]["type"] == "json_schema"
    assert seen["response_format"]["json_schema"]["schema"] == {"type": "object"}


def _capturing_model_server():
    """A model server that records each request body — to see what reached the wire."""
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    seen: list[dict] = []

    class _H(BaseHTTPRequestHandler):
        def do_POST(self):
            seen.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            body = json.dumps({
                "id": "x", "object": "chat.completion", "created": 0, "model": "fake",
                "choices": [{"index": 0, "finish_reason": "stop",
                             "message": {"role": "assistant", "content": "hi"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), _H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}/v1", seen


async def test_a_refused_param_is_dropped_on_the_fallback_too(monkeypatch):
    """The 30b-reason → cloud case: the tier the client named accepts `temperature`, the
    account it falls back to refuses it. Stripping the request up front would be wrong for
    the first and cannot reach the second — the drop has to ride on the deployment."""
    srv, base, seen = _capturing_model_server()
    # The provider account is called natively; this points LiteLLM's OpenAI client here.
    monkeypatch.setenv("OPENAI_BASE_URL", base)
    monkeypatch.setenv("CBK_T_SYNC_KEY", "sk-test")
    try:
        fleet = Fleet(capabilities={
            "local": CapabilitySpec(model="fake", model_server="http://127.0.0.1:9/v1",
                                    cloud_fallback="strict"),
            "strict": CapabilitySpec(model="openai/strict-model", cloud=True,
                                     api_key_env="CBK_T_SYNC_KEY",
                                     drop_params=["temperature"]),
        })
        router = build_router(fleet)
        await router.acompletion(model="local", temperature=0.25, max_tokens=5,
                                 messages=[{"role": "user", "content": "x"}])
        await router.acompletion(model="strict", temperature=0,
                                 messages=[{"role": "user", "content": "x"}])
    finally:
        srv.shutdown()
    assert len(seen) == 2
    assert all("temperature" not in body for body in seen)
    assert seen[0]["max_tokens"] == 5  # only the named param goes
