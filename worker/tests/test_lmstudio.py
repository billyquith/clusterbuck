"""The LM Studio adapter, and the detection that makes it reachable.

`auto` used to mean "Ollama", so an LM Studio node left on the default probed Ollama
paths, got 404s, and reported nothing warm and no digests — indistinguishable from a
healthy node with nothing loaded. That is why `stats.load_s` was never measurable there,
why reservation pre-warm falls back to a constant on those nodes, and why `cbk pause`
had nothing to unload.

**Provenance of the fixtures below: LM Studio's published API documentation, not a live
node** — none was reachable when this was written. They are the shapes to re-check first
against a real 0.4.x box. That uncertainty is also why every parse in the adapter fails
closed to "unknown" rather than to a partial answer: a wrong positive would let
`work_loop._was_cold` record warm jobs as cold starts and poison the measured load time,
where "unknown" is already handled everywhere as "the adapter cannot say".
"""

from __future__ import annotations

import httpx
import pytest

from cbk_worker.inventory import LMSTUDIO, NONE, OLLAMA, ModelInventory
from cbk_worker.model_manager import ModelManager

BASE = "http://127.0.0.1:1234"

# GET /api/v1/models on LM Studio 0.4.0+: every DOWNLOADED model, with the loaded ones
# carrying a non-empty `loaded_instances`.
V1_MODELS = {"models": [
    {"key": "qwen/qwen3-30b-a3b", "type": "llm", "quantization": "Q4_K_M",
     "loaded_instances": [{"instance_id": "qwen/qwen3-30b-a3b", "context_length": 8192}]},
    {"key": "deepseek-r1", "type": "llm", "loaded_instances": []},
]}

# GET /api/v0/models on 0.3.6+: a flat `state` instead, and no unload endpoint anywhere.
V0_MODELS = {"data": [
    {"id": "qwen/qwen3-30b-a3b", "object": "model", "type": "llm", "state": "loaded"},
    {"id": "deepseek-r1", "object": "model", "type": "llm", "state": "not-loaded"},
]}


def _server(routes: dict[str, object], record: list | None = None):
    """A model server that answers only the paths it is given; everything else 404s."""
    def handler(request: httpx.Request) -> httpx.Response:
        if record is not None:
            record.append((request.method, request.url.path, request.read().decode()))
        body = routes.get(request.url.path)
        if body is None:
            return httpx.Response(404, text="not found")
        return httpx.Response(200, json=body)
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# --- detection --------------------------------------------------------------------


async def test_auto_finds_lm_studio_instead_of_assuming_ollama():
    """The defect this whole change exists for: a node on the default `auto`."""
    http = _server({"/api/v1/models": V1_MODELS})
    assert await ModelInventory(http, f"{BASE}/v1", "auto").flavour() == LMSTUDIO
    await http.aclose()


async def test_auto_still_finds_ollama():
    http = _server({"/api/ps": {"models": []}})
    assert await ModelInventory(http, f"{BASE}/v1", "auto").flavour() == OLLAMA
    await http.aclose()


async def test_auto_recognises_an_older_lm_studio_by_its_v0_api():
    """0.3.6-era: it can say what is resident even though it cannot unload."""
    http = _server({"/api/v0/models": V0_MODELS})
    assert await ModelInventory(http, f"{BASE}/v1", "auto").flavour() == LMSTUDIO
    await http.aclose()


async def test_nothing_answering_is_unknown_not_ollama():
    """The old behaviour's real failure: assuming, and reporting the assumption's
    404s as facts about the node."""
    http = _server({})
    assert await ModelInventory(http, f"{BASE}/v1", "auto").flavour() == NONE
    await http.aclose()


async def test_a_failed_probe_is_retried_but_a_successful_one_is_remembered():
    """A model server that starts after the worker must still be found; one that has
    already answered is not going to turn into a different product."""
    routes: dict[str, object] = {}
    calls: list = []
    http = _server(routes, calls)
    inv = ModelInventory(http, f"{BASE}/v1", "auto")

    assert await inv.flavour() == NONE          # nothing up yet
    routes["/api/v1/models"] = V1_MODELS        # LM Studio starts
    assert await inv.flavour() == LMSTUDIO      # re-probed, found

    before = len(calls)
    assert await inv.flavour() == LMSTUDIO      # and now cached
    assert len(calls) == before
    await http.aclose()


async def test_an_explicit_setting_is_taken_at_its_word():
    """An operator who names an adapter gets it — including `none` to opt out."""
    http = _server({"/api/ps": {"models": []}})
    assert await ModelInventory(http, f"{BASE}/v1", "lmstudio").flavour() == LMSTUDIO
    assert await ModelInventory(http, f"{BASE}/v1", "none").flavour() == NONE
    await http.aclose()


# --- residency --------------------------------------------------------------------


async def test_loaded_reads_the_v1_instances():
    """`loaded_instances: []` is downloaded-but-cold, which is the distinction the whole
    method exists to draw — and what makes a cold start provable on these nodes."""
    http = _server({"/api/v1/models": V1_MODELS})
    inv = ModelInventory(http, f"{BASE}/v1", "lmstudio")
    assert await inv.loaded() == ["qwen/qwen3-30b-a3b"]
    await http.aclose()


async def test_loaded_falls_back_to_the_v0_state_field():
    http = _server({"/api/v0/models": V0_MODELS})
    inv = ModelInventory(http, f"{BASE}/v1", "lmstudio")
    assert await inv.loaded() == ["qwen/qwen3-30b-a3b"]
    await http.aclose()


@pytest.mark.parametrize("body", [
    {"models": "not-a-list"},
    {"unexpected": []},
    {"models": [{"no_key": 1, "loaded_instances": [{"instance_id": "x"}]}]},
])
async def test_an_unexpected_shape_reads_as_unknown_not_as_nothing_loaded(body):
    """A wrong positive is worse than no answer: `work_loop._was_cold` treats empty as
    "the adapter cannot say" and never as evidence, so failing closed keeps a shape
    change from silently recording every warm job as a cold start."""
    http = _server({"/api/v1/models": body})
    inv = ModelInventory(http, f"{BASE}/v1", "lmstudio")
    assert await inv.loaded() == []
    await http.aclose()


async def test_lm_studio_reports_no_digests():
    """It publishes a quantization and a size but no content hash, so there is nothing
    to compare and the honest answer is none — not a fabricated one."""
    http = _server({"/api/v1/models": V1_MODELS})
    assert await ModelInventory(http, f"{BASE}/v1", "lmstudio").digests() == {}
    await http.aclose()


# --- unloading ------------------------------------------------------------------------


async def test_unload_posts_to_the_v1_endpoint():
    calls: list = []
    http = _server({"/api/v1/models/unload": {"instance_id": "qwen/qwen3-30b-a3b"}}, calls)
    manager = ModelManager(http, BASE, "lmstudio")

    assert await manager.unload("qwen/qwen3-30b-a3b") == (True, None)
    method, path, body = calls[-1]
    assert (method, path) == ("POST", "/api/v1/models/unload")
    assert "qwen/qwen3-30b-a3b" in body and "instance_id" in body
    await http.aclose()


async def test_an_older_lm_studio_says_it_needs_an_upgrade():
    """A 404 here means "this build predates the endpoint", which is a different thing
    from a failure and deserves an answer the owner can act on."""
    http = _server({"/api/v0/models": V0_MODELS})
    manager = ModelManager(http, BASE, "lmstudio")

    ok, error = await manager.unload("qwen/qwen3-30b-a3b")
    assert ok is False
    assert "0.4.0" in error and "stays resident" in error
    await http.aclose()


async def test_the_manager_follows_the_inventorys_detection():
    """One place decides what this node is running, so the two cannot disagree about it
    — the manager is a pure actuator."""
    calls: list = []
    http = _server({"/api/v1/models": V1_MODELS,
                    "/api/v1/models/unload": {"instance_id": "x"}}, calls)
    inventory = ModelInventory(http, f"{BASE}/v1", "auto")
    manager = ModelManager(http, BASE, "auto", flavour=inventory.flavour)

    assert await manager.flavour() == LMSTUDIO
    assert await manager.unload("qwen/qwen3-30b-a3b") == (True, None)
    assert any(p == "/api/v1/models/unload" for _m, p, _b in calls)
    await http.aclose()


async def test_ollama_is_unaffected():
    """The existing path keeps its exact shape: no unload endpoint, `keep_alive: 0`."""
    calls: list = []
    http = _server({"/api/generate": {"done": True}}, calls)
    manager = ModelManager(http, BASE, "ollama")

    assert await manager.unload("qwen2.5:32b") == (True, None)
    _method, path, body = calls[-1]
    assert path == "/api/generate"
    assert '"keep_alive": 0' in body or '"keep_alive":0' in body
    await http.aclose()
