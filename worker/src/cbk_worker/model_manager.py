"""Installs and removes model artifacts on this node.

**This is the one place clusterbuck necessarily leaves the generic OpenAI wire protocol**
(ADR 25). "Pull a model" has no OpenAI-standard endpoint: Ollama has `POST /api/pull`,
llama.cpp has no concept of it (you hand it a GGUF path), and vLLM fetches at launch. So
installation lives behind this small pluggable adapter, kept strictly separate from the
inference path — which stays vendor-neutral (protocols.md §3).

The worker never *decides* to install anything; it executes an action the coordinator issued
for an already-approved proposal (fleet-management.md → three gates).
"""

from __future__ import annotations

import httpx


def _trim(s: str, limit: int = 160) -> str:
    return s if len(s) <= limit else s[:limit]


class ModelManager:
    def __init__(self, client: httpx.AsyncClient, native_base: str, manager: str) -> None:
        self._client = client
        self._base = native_base.rstrip("/")
        self._manager = manager

    @property
    def can_manage(self) -> bool:
        """Whether this node can install/remove models itself."""
        return self._manager in ("ollama", "auto")

    async def install(self, registry_ref: str) -> tuple[bool, str | None]:
        if not self.can_manage:
            return (False, f"model manager '{self._manager}' cannot install; "
                           f"install {registry_ref} manually")
        try:
            # Ollama streams NDJSON progress; stream=false returns a single final object.
            resp = await self._client.post(f"{self._base}/api/pull",
                                          json={"model": registry_ref, "stream": False})
            text = resp.text
            if resp.status_code >= 400:
                return (False, f"pull failed: HTTP {resp.status_code} {_trim(text)}")
            body = resp.json()
            # Ollama reports a terminal {"status":"success"} — or an {"error":...}.
            if isinstance(body, dict) and body.get("error"):
                return (False, f"pull failed: {body['error']}")
            return (True, None)
        except Exception as e:
            return (False, f"pull failed: {e}")

    async def unload(self, artifact: str) -> tuple[bool, str | None]:
        """Drop a model out of memory, returning RAM to the owner. Disk is untouched.

        Deliberately not `remove`, which deletes the weights: the owner wanting their
        machine back for an hour must not cost a multi-gigabyte re-download afterwards.
        This is the RAM half of `cbk pause` (ADR 10 — "the owner always wins"), and on a
        shared 16 GB box it is the half that actually matters.

        Vendor-specific, like the rest of this adapter, and more so: Ollama has no unload
        endpoint at all. The documented mechanism is an empty generate carrying
        `keep_alive: 0`, which tells the server to evict the model as soon as it is done —
        which, with no prompt, is immediately. llama.cpp and vLLM hold one model for the
        life of the process and cannot do this at any price, so there the honest answer is
        that the weights stay resident until their server is stopped.
        """
        if not self.can_manage:
            return (False, f"model manager '{self._manager}' cannot unload; {artifact} "
                           f"stays resident until its model server evicts it")
        try:
            resp = await self._client.post(f"{self._base}/api/generate",
                                           json={"model": artifact, "keep_alive": 0})
            if resp.status_code >= 400:
                return (False, f"unload failed: HTTP {resp.status_code} {_trim(resp.text)}")
            return (True, None)
        except Exception as e:
            return (False, f"unload failed: {e}")

    async def remove(self, artifact: str) -> tuple[bool, str | None]:
        """Remove an artifact, returning disk to the owner."""
        if not self.can_manage:
            return (False, f"model manager '{self._manager}' cannot remove; "
                           f"remove {artifact} manually")
        try:
            resp = await self._client.request("DELETE", f"{self._base}/api/delete",
                                              json={"model": artifact})
            if resp.status_code >= 400:
                return (False, f"remove failed: HTTP {resp.status_code} {_trim(resp.text)}")
            return (True, None)
        except Exception as e:
            return (False, f"remove failed: {e}")
