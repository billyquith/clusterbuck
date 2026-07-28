"""Discovers what models this node's model server actually has (fleet-management.md →
Dynamic registry). Replaces hand-maintained `fleet.yaml` model lists with observed reality.

**Installed** comes from the generic OpenAI-compatible `GET /v1/models`, which Ollama,
LM Studio, vLLM and llama.cpp-server all serve — so one portable code path, no vendor SDK,
consistent with protocols.md §3.

**Loaded** (warm right now) and **digests** are NOT in the OpenAI standard, so they need a
small per-server adapter (Ollama: `/api/ps`, `/api/tags`). Anything the adapter cannot answer
degrades to "unknown" (empty) rather than failing the heartbeat — inventory reporting must
never take a worker down. Scanning vendor model directories on disk is deliberately NOT the
primary mechanism: those layouts are undocumented internals, and a blob the server has not
registered is not servable anyway.
"""

from __future__ import annotations

from typing import Any

import httpx


class ModelInventory:
    def __init__(self, client: httpx.AsyncClient, model_server_url: str,
                 manager: str) -> None:
        self._client = client
        self._url = model_server_url.rstrip("/")
        self._manager = manager

    @property
    def native_base(self) -> str:
        """Native (non-OpenAI) base URL: strip the trailing /v1 the OpenAI path adds."""
        return self._url[:-3].rstrip("/") if self._url.endswith("/v1") else self._url

    @property
    def _is_ollama(self) -> bool:
        return self._manager in ("ollama", "auto")

    async def _get_json(self, url: str) -> Any | None:
        # Only `Exception` is caught here and below: asyncio.CancelledError is a
        # BaseException, so shutdown still propagates instead of being mistaken for a
        # model server that is merely down.
        try:
            resp = await self._client.get(url)
            if resp.status_code >= 400:
                return None
            return resp.json()
        except Exception:
            return None

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
        """Models warm in memory right now (vendor-specific; empty = unknown)."""
        if not self._is_ollama:
            return []
        body = await self._get_json(f"{self.native_base}/api/ps")
        names: list[str] = []
        if isinstance(body, dict) and isinstance(body.get("models"), list):
            for m in body["models"]:
                if isinstance(m, dict) and isinstance(m.get("name"), str):
                    names.append(m["name"])
        return sorted(names)

    async def digests(self) -> dict[str, str]:
        """artifact → content digest, where the server exposes it.

        Digests are what make "this model was updated upstream" detectable — and, because
        ability is pinned to an artifact (ADR 15), what forces re-measurement when one
        changes rather than letting a stale score drive routing.
        """
        if not self._is_ollama:
            return {}
        body = await self._get_json(f"{self.native_base}/api/tags")
        out: dict[str, str] = {}
        if isinstance(body, dict) and isinstance(body.get("models"), list):
            for m in body["models"]:
                if (isinstance(m, dict) and isinstance(m.get("name"), str)
                        and isinstance(m.get("digest"), str)):
                    out[m["name"]] = m["digest"]
        return out
