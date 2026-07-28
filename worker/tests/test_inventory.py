"""Model discovery (ADR 25): portable /v1/models, plus the Ollama-only extras, degrading
rather than failing when the model server cannot answer."""

from __future__ import annotations

import httpx

from cbk_worker.inventory import ModelInventory

_MODELS = {"object": "list", "data": [{"id": "qwen2.5:32b"}, {"id": "llama3.2:3b"}]}
_PS = {"models": [{"name": "qwen2.5:32b", "size": 1}]}
_TAGS = {"models": [{"name": "llama3.2:3b", "digest": "sha256:aaa"},
                    {"name": "qwen2.5:32b", "digest": "sha256:bbb"}]}


def _routed(routes: dict[str, object], record: list[str] | None = None) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if record is not None:
            record.append(request.url.path)
        for path, body in routes.items():
            if request.url.path == path:
                if isinstance(body, int):
                    return httpx.Response(body)
                return httpx.Response(200, json=body)
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def _inv(transport, manager="auto", url="http://m:11434/v1") -> ModelInventory:
    return ModelInventory(httpx.AsyncClient(transport=transport), url, manager)


def test_native_base_strips_the_openai_v1_suffix():
    assert _inv(_routed({})).native_base == "http://m:11434"
    assert ModelInventory(None, "http://m:8080", "auto").native_base == "http://m:8080"


async def test_installed_comes_from_the_portable_endpoint_sorted():
    seen: list[str] = []
    inv = _inv(_routed({"/v1/models": _MODELS}, seen))
    assert await inv.installed() == ["llama3.2:3b", "qwen2.5:32b"]
    # The generic OpenAI path, not a vendor one: every server we support serves it.
    assert seen == ["/v1/models"]


async def test_loaded_and_digests_use_the_ollama_adapter():
    inv = _inv(_routed({"/api/ps": _PS, "/api/tags": _TAGS}))
    assert await inv.loaded() == ["qwen2.5:32b"]
    assert await inv.digests() == {"llama3.2:3b": "sha256:aaa", "qwen2.5:32b": "sha256:bbb"}


async def test_manager_none_reports_no_vendor_extras():
    """`none` means discovery via /v1/models only — no vendor endpoints are even tried."""
    seen: list[str] = []
    inv = _inv(_routed({"/v1/models": _MODELS, "/api/ps": _PS, "/api/tags": _TAGS}, seen),
               manager="none")
    assert await inv.installed() == ["llama3.2:3b", "qwen2.5:32b"]
    assert await inv.loaded() == []
    assert await inv.digests() == {}
    assert seen == ["/v1/models"], f"probed vendor endpoints anyway: {seen}"


async def test_a_down_model_server_degrades_instead_of_failing_the_beat():
    """Inventory reporting must never take a worker down: an unreachable server reports
    nothing, and the heartbeat still goes out."""
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    inv = _inv(httpx.MockTransport(handler))
    assert await inv.installed() == []
    assert await inv.loaded() == []
    assert await inv.digests() == {}


async def test_http_errors_and_junk_payloads_degrade_too():
    inv = _inv(_routed({"/v1/models": 500, "/api/ps": 404, "/api/tags": 503}))
    assert await inv.installed() == []
    assert await inv.loaded() == []
    assert await inv.digests() == {}

    junk = _inv(_routed({"/v1/models": {"unexpected": True},
                         "/api/tags": {"models": [{"name": "x"}, {"digest": "y"}]}}))
    assert await junk.installed() == []
    assert await junk.digests() == {}      # entries missing name or digest are skipped
