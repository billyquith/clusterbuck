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


def test_unknown_capability_errors(sync_client):
    resp = sync_client.post(
        "/v1/chat/completions",
        json={"model": "does-not-exist", "messages": [{"role": "user", "content": "x"}]},
    )
    assert resp.status_code == 502


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
