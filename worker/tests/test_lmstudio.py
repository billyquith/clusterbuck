"""The LM Studio adapter, and the detection that makes it reachable.

`auto` used to mean "Ollama", so an LM Studio node left on the default probed Ollama
paths, got 404s, and reported nothing warm and no digests — indistinguishable from a
healthy node with nothing loaded. That is why `stats.load_s` was never measurable there,
why reservation pre-warm falls back to a constant on those nodes, and why `cbk pause`
had nothing to unload.

**Provenance of the fixtures below: captured from a live LM Studio 0.4.x node on
2026-09-23**, replacing the doc-derived guesses this file shipped with. Putting it on
real hardware found two things no amount of reading the docs would have:

* LM Studio answers an endpoint it does NOT have with **HTTP 200 and an `{"error": ...}`
  body**, so probing Ollama's `/api/ps` came back "positive" and classified the node as
  Ollama — which is why it still reported `loaded: []` after the adapter shipped.
* A `loaded_instances` entry keys its identifier **`id`**, not `instance_id`, and the
  same model is typed `llm` by v1 and `vlm` by v0.

Every parse still fails closed to "unknown" rather than to a partial answer: a wrong
positive would let `work_loop._was_cold` record warm jobs as cold starts and poison the
measured load time, where "unknown" is already handled everywhere as "the adapter cannot
say".
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
    {"type": "llm", "publisher": "qwen", "key": "qwen/qwen3.5-9b",
     "display_name": "Qwen3.5 9B", "architecture": "qwen3_5",
     "quantization": {"name": "4bit", "bits_per_weight": 4},
     "size_bytes": 5977265552, "params_string": "9B",
     # Note `id`, not `instance_id` — the docs' unload example uses the latter as a
     # request FIELD name, which is not what the listing calls the value.
     "loaded_instances": [{"id": "qwen/qwen3.5-9b",
                           "config": {"context_length": 51456, "parallel": 4}}],
     "max_context_length": 262144},
    {"type": "llm", "publisher": "deepseek", "key": "deepseek-r1",
     "loaded_instances": []},
]}

# GET /api/v0/models on 0.3.6+: a flat `state` instead, and no unload endpoint anywhere.
V0_MODELS = {"object": "list", "data": [
    # `vlm` here for the very model v1 calls `llm`, so `type` is not a usable filter.
    {"id": "qwen/qwen3.5-9b", "object": "model", "type": "vlm", "publisher": "qwen",
     "arch": "qwen3_5", "compatibility_type": "mlx", "quantization": "4bit",
     "state": "loaded", "max_context_length": 262144, "loaded_context_length": 51456},
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
    assert await inv.loaded() == ["qwen/qwen3.5-9b"]
    await http.aclose()


async def test_loaded_falls_back_to_the_v0_state_field():
    http = _server({"/api/v0/models": V0_MODELS})
    inv = ModelInventory(http, f"{BASE}/v1", "lmstudio")
    assert await inv.loaded() == ["qwen/qwen3.5-9b"]
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
    http = _server({"/api/v1/models/unload": {"instance_id": "qwen/qwen3.5-9b"}}, calls)
    manager = ModelManager(http, BASE, "lmstudio")

    assert await manager.unload("qwen/qwen3.5-9b") == (True, None)
    method, path, body = calls[-1]
    assert (method, path) == ("POST", "/api/v1/models/unload")
    assert "qwen/qwen3.5-9b" in body and "instance_id" in body
    await http.aclose()


async def test_an_older_lm_studio_says_it_needs_an_upgrade():
    """A 404 here means "this build predates the endpoint", which is a different thing
    from a failure and deserves an answer the owner can act on."""
    http = _server({"/api/v0/models": V0_MODELS})
    manager = ModelManager(http, BASE, "lmstudio")

    ok, error = await manager.unload("qwen/qwen3.5-9b")
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
    assert await manager.unload("qwen/qwen3.5-9b") == (True, None)
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


async def test_install_on_an_lm_studio_node_says_it_cannot_rather_than_404ing():
    """`can_manage` reads the DETECTED server, not the configured word, so it agrees with
    `unload` about what is on the other end. Judging it from `auto` alone meant an
    LM Studio node answered "yes" and then reported Ollama's `HTTP 404` from a pull path
    that was never going to exist — a confusing failure where "this adapter cannot
    install" is the plain truth."""
    http = _server({"/api/v1/models": V1_MODELS})
    inventory = ModelInventory(http, f"{BASE}/v1", "auto")
    manager = ModelManager(http, BASE, "auto", flavour=inventory.flavour)

    assert await manager.can_manage() is False
    ok, error = await manager.install("qwen/qwen3.5-9b")
    assert ok is False
    assert "lmstudio" in error and "manually" in error
    assert "404" not in error
    await http.aclose()


async def test_ollama_can_still_install():
    http = _server({"/api/ps": {"models": []}, "/api/pull": {"status": "success"}})
    inventory = ModelInventory(http, f"{BASE}/v1", "auto")
    manager = ModelManager(http, BASE, "auto", flavour=inventory.flavour)

    assert await manager.can_manage() is True
    assert await manager.install("qwen2.5:32b") == (True, None)
    await http.aclose()


async def test_an_undetected_auto_manages_nothing_rather_than_guessing_ollama():
    """The trap the detection work left behind.

    `ModelManager`'s no-inventory fallback still resolved `auto` to Ollama, so any future
    two-argument construction would silently restore the very assumption 68ded30 removed
    — and restore it in the one place that then talks to the wrong native API. `auto`
    means nobody has looked yet, and something that cannot detect should manage nothing
    rather than manage the wrong thing.
    """
    http = _server({"/api/ps": {"models": []}, "/api/generate": {}})
    manager = ModelManager(http, BASE, "auto")          # deliberately no flavour provider

    assert await manager.flavour() == NONE
    assert await manager.can_manage() is False
    ok, error = await manager.unload("qwen2.5:32b")
    assert ok is False and "cannot unload" in error
    await http.aclose()


# --- what only a real node showed -----------------------------------------------------

# LM Studio's reply to an endpoint it does not have. Status 200, body an error — captured
# live: `GET /api/tags` -> 200 {"error":"Unexpected endpoint or method. (GET /api/tags)"}
LMSTUDIO_NO_SUCH_ENDPOINT = {"error": "Unexpected endpoint or method. (GET /api/tags)"}


async def test_an_lm_studio_node_is_not_mistaken_for_ollama():
    """The bug that survived the doc-derived tests and shipped to a real node.

    LM Studio answers a path it does not serve with HTTP 200 and an `{"error": ...}`
    body. The probe asked "did `/api/ps` respond?", which is yes on a box with no Ollama
    within reach — so the node was classified Ollama, `loaded()` went to `/api/ps`, found
    no `models` key and returned `[]`. Exactly the symptom the adapter was written to
    cure, reintroduced one layer up. The question has to be "did it respond with the
    SHAPE that path returns".
    """
    http = _server({
        "/api/ps": LMSTUDIO_NO_SUCH_ENDPOINT,      # 200, but not an answer
        "/api/tags": LMSTUDIO_NO_SUCH_ENDPOINT,
        "/api/v1/models": V1_MODELS,
    })
    inv = ModelInventory(http, f"{BASE}/v1", "auto")

    assert await inv.flavour() == LMSTUDIO
    assert await inv.loaded() == ["qwen/qwen3.5-9b"]
    await http.aclose()


async def test_a_200_carrying_an_error_is_never_treated_as_data():
    """Applies to every native read, not just the probe: an error body must not be
    mistaken for an empty inventory, which would read as "nothing is loaded" rather
    than "the adapter cannot say"."""
    http = _server({"/api/ps": LMSTUDIO_NO_SUCH_ENDPOINT})
    inv = ModelInventory(http, f"{BASE}/v1", "ollama")     # forced, so no probing
    assert await inv.loaded() == []
    assert await inv.digests() == {}
    await http.aclose()


async def test_a_real_ollama_still_answers_the_probe():
    """The shape check must not reject the thing it is looking for."""
    http = _server({"/api/ps": {"models": [{"name": "qwen2.5:32b"}]}})
    inv = ModelInventory(http, f"{BASE}/v1", "auto")
    assert await inv.flavour() == OLLAMA
    assert await inv.loaded() == ["qwen2.5:32b"]
    await http.aclose()
