"""Discovers what models this node's model server actually has (fleet-management.md →
Dynamic registry). Replaces hand-maintained `fleet.yaml` model lists with observed reality.

**Installed** comes from the generic OpenAI-compatible `GET /v1/models`, which Ollama,
LM Studio, vLLM and llama.cpp-server all serve — so one portable code path, no vendor SDK,
consistent with protocols.md §3.

**Loaded** (warm right now) and **digests** are NOT in the OpenAI standard, so they need a
small per-server adapter (Ollama: `/api/ps`, `/api/tags`; LM Studio: `/api/v1/models`, or
`/api/v0/models` on older builds). Anything the adapter cannot answer degrades to
"unknown" (empty) rather than failing the heartbeat — inventory reporting must never take
a worker down. Scanning vendor model directories on disk is deliberately NOT the primary
mechanism: those layouts are undocumented internals, and a blob the server has not
registered is not servable anyway.

**Which server this is gets DISCOVERED, not assumed.** `auto` used to mean "Ollama", so an
LM Studio node left on the default probed Ollama paths, got 404s, and reported nothing
warm and no digests — indistinguishable from a healthy node with nothing loaded, and the
reason `stats.load_s` could never be measured there. `auto` now probes for the API that is
actually answering. A failed probe is **not** cached: a model server that starts after the
worker must be found on a later beat, whereas a server that answered once is not going to
turn into a different product.
"""

from __future__ import annotations

from typing import Any

import httpx

# What a node's model server speaks natively, beyond the portable OpenAI endpoint.
OLLAMA, LMSTUDIO, NONE = "ollama", "lmstudio", "none"


class ModelInventory:
    def __init__(self, client: httpx.AsyncClient, model_server_url: str,
                 manager: str) -> None:
        self._client = client
        self._url = model_server_url.rstrip("/")
        self._manager = manager
        # A successful detection, remembered. Only ever set to a real answer, so an
        # unreachable model server is re-probed rather than written off for the life of
        # the process.
        self._detected: str | None = None

    @property
    def native_base(self) -> str:
        """Native (non-OpenAI) base URL: strip the trailing /v1 the OpenAI path adds."""
        return self._url[:-3].rstrip("/") if self._url.endswith("/v1") else self._url

    async def flavour(self) -> str:
        """Which native API is on the other end: `ollama` | `lmstudio` | `none`.

        An explicit `CBK_MODEL_MANAGER` is taken at its word — an operator who names one
        gets it, including `none` to opt out of native calls entirely. Only `auto` probes.
        """
        if self._manager in (OLLAMA, LMSTUDIO, NONE):
            return self._manager
        if self._detected is None:
            self._detected = await self._probe()
        return self._detected or NONE

    @staticmethod
    def _carries(body: Any, key: str) -> bool:
        """Whether a probe response is the SHAPE that path is supposed to return."""
        return isinstance(body, dict) and isinstance(body.get(key), list)

    async def _probe(self) -> str | None:
        """Ask each server for something only it serves. None = could not tell, retry later.

        Shape-matched, not merely status-matched, and that is the whole lesson of putting
        this on real hardware: LM Studio answers an endpoint it does not have with HTTP
        200 and an `{"error": ...}` body, so "did `/api/ps` respond?" said yes on a node
        with no Ollama within reach. The question has to be "did it respond with a
        `models` LIST", which an error body does not.

        Ordered as well, because the two products share the `models` key on different
        paths: Ollama serves it at `/api/ps`, LM Studio at `/api/v1/models`. The v0 path
        is tried last so a 0.3.6-era LM Studio is still recognised, since it can report
        residency even though it cannot unload.
        """
        if self._carries(await self._get_json(f"{self.native_base}/api/ps"), "models"):
            return OLLAMA
        if self._carries(
            await self._get_json(f"{self.native_base}/api/v1/models"), "models"
        ):
            return LMSTUDIO
        if self._carries(
            await self._get_json(f"{self.native_base}/api/v0/models"), "data"
        ):
            return LMSTUDIO
        return None  # nothing answered; unknown, and deliberately not remembered

    async def _get_json(self, url: str) -> Any | None:
        # Only `Exception` is caught here and below: asyncio.CancelledError is a
        # BaseException, so shutdown still propagates instead of being mistaken for a
        # model server that is merely down.
        try:
            resp = await self._client.get(url)
            if resp.status_code >= 400:
                return None
            body = resp.json()
        except Exception:
            return None
        # **A 200 is not an answer.** LM Studio replies to an endpoint it does not have
        # with HTTP 200 and `{"error": "Unexpected endpoint or method. (GET /api/tags)"}`,
        # which is how probing for Ollama's paths on an LM Studio node came back positive
        # and got the whole node classified as Ollama. Observed on a live 0.4.x box; no
        # amount of reading the docs would have shown it.
        if isinstance(body, dict) and body.get("error"):
            return None
        return body

    async def installed(self) -> list[str]:
        """Models the server can serve, via the portable OpenAI endpoint."""
        body = await self._get_json(f"{self._url}/models")
        ids: list[str] = []
        if isinstance(body, dict) and isinstance(body.get("data"), list):
            for m in body["data"]:
                if isinstance(m, dict) and isinstance(m.get("id"), str):
                    ids.append(m["id"])
        return sorted(ids)

    async def loaded(self) -> list[str]:
        """Models warm in memory right now (vendor-specific; empty = unknown).

        Empty means UNKNOWN throughout, never "nothing is loaded" — `work_loop._was_cold`
        depends on that, and a wrong positive here would record warm jobs as cold starts
        and poison the measured load time. So every parse below is all-or-nothing: an
        unexpected shape yields no names rather than the subset it happened to recognise.
        """
        flavour = await self.flavour()
        if flavour == OLLAMA:
            return await self._ollama_loaded()
        if flavour == LMSTUDIO:
            return await self._lmstudio_loaded()
        return []

    async def _ollama_loaded(self) -> list[str]:
        body = await self._get_json(f"{self.native_base}/api/ps")
        names: list[str] = []
        if isinstance(body, dict) and isinstance(body.get("models"), list):
            for m in body["models"]:
                if isinstance(m, dict) and isinstance(m.get("name"), str):
                    names.append(m["name"])
        return sorted(names)

    async def _lmstudio_loaded(self) -> list[str]:
        """LM Studio's two REST generations, newest first.

        `/api/v1/models` (0.4.0+) returns every DOWNLOADED model with a
        `loaded_instances` array — non-empty is what makes it resident, so an empty array
        is "on disk and cold", which is exactly the distinction this method exists to
        draw. `/api/v0/models` (0.3.6+) instead carries a flat `state` of
        `loaded`/`not-loaded`.

        Shapes CONFIRMED against a live LM Studio 0.4.x node (2026-09-23), having first
        been transcribed from the published docs. Two things the docs did not show: a
        `loaded_instances` entry keys its identifier `id`, not `instance_id`; and the
        same model is typed `llm` by v1 and `vlm` by v0, so `type` is not a reliable
        filter. Every branch still fails closed to "unknown", because a wrong positive
        here would record warm jobs as cold starts.
        """
        body = await self._get_json(f"{self.native_base}/api/v1/models")
        if isinstance(body, dict) and isinstance(body.get("models"), list):
            names = []
            for m in body["models"]:
                if not isinstance(m, dict) or not isinstance(m.get("key"), str):
                    continue
                if isinstance(m.get("loaded_instances"), list) and m["loaded_instances"]:
                    names.append(m["key"])
            return sorted(names)

        body = await self._get_json(f"{self.native_base}/api/v0/models")
        if isinstance(body, dict) and isinstance(body.get("data"), list):
            names = []
            for m in body["data"]:
                if not isinstance(m, dict) or not isinstance(m.get("id"), str):
                    continue
                if m.get("state") == "loaded":
                    names.append(m["id"])
            return sorted(names)

        return []

    async def digests(self) -> dict[str, str]:
        """artifact → content digest, where the server exposes it.

        Digests are what make "this model was updated upstream" detectable — and, because
        ability is pinned to an artifact (ADR 15), what forces re-measurement when one
        changes rather than letting a stale score drive routing.

        Ollama only: LM Studio publishes a quantization and a size but no content hash,
        so there is nothing here to compare and the honest answer is none.
        """
        if await self.flavour() != OLLAMA:
            return {}
        body = await self._get_json(f"{self.native_base}/api/tags")
        out: dict[str, str] = {}
        if isinstance(body, dict) and isinstance(body.get("models"), list):
            for m in body["models"]:
                if (isinstance(m, dict) and isinstance(m.get("name"), str)
                        and isinstance(m.get("digest"), str)):
                    out[m["name"]] = m["digest"]
        return out
