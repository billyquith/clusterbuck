"""Installs, removes and unloads model artifacts on this node.

**This is the one place clusterbuck necessarily leaves the generic OpenAI wire protocol**
(ADR 25). "Pull a model" has no OpenAI-standard endpoint: Ollama has `POST /api/pull`,
llama.cpp has no concept of it (you hand it a GGUF path), and vLLM fetches at launch. So
installation lives behind this small pluggable adapter, kept strictly separate from the
inference path — which stays vendor-neutral (protocols.md §3).

Neither does "drop this model out of memory", and there the servers disagree more
sharply: Ollama has no unload endpoint at all and is asked with `keep_alive: 0`, LM Studio
grew a real one in 0.4.0 (`POST /api/v1/models/unload`) and had none before that, and
llama.cpp and vLLM hold one model for the life of the process and cannot do it at any
price. A server that cannot is told so plainly rather than reporting a success — the
owner needs to know their RAM is not coming back.

This module is a pure actuator: it is told which artifact to act on and does it. Which
server it is talking to comes from the inventory, which is what discovered it.

The worker never *decides* to install anything; it executes an action the coordinator issued
for an already-approved proposal (fleet-management.md → three gates).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import httpx

from .inventory import LMSTUDIO, OLLAMA


def _trim(s: str, limit: int = 160) -> str:
    return s if len(s) <= limit else s[:limit]


class ModelManager:
    def __init__(self, client: httpx.AsyncClient, native_base: str, manager: str,
                 flavour: Callable[[], Awaitable[str]] | None = None) -> None:
        self._client = client
        self._base = native_base.rstrip("/")
        self._manager = manager
        # Supplied by the inventory, which owns detection so the two cannot disagree about
        # what this node is running. Absent ⇒ fall back to the configured value, which is
        # what the older two-argument construction meant.
        self._flavour = flavour

    async def flavour(self) -> str:
        if self._flavour is not None:
            return await self._flavour()
        return self._manager if self._manager != "auto" else OLLAMA

    @property
    def can_manage(self) -> bool:
        """Whether this node can install/remove models itself.

        Installation stays Ollama-only. LM Studio 0.4.0 does have
        `POST /api/v1/models/download`, but pulling weights is gated on an approved
        proposal and a disk quota (fleet-management.md → three gates), and wiring a
        second vendor into that path is a separate change from giving the owner their
        memory back.
        """
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
        flavour = await self.flavour()
        if flavour == OLLAMA:
            return await self._post_unload(
                f"{self._base}/api/generate", {"model": artifact, "keep_alive": 0})
        if flavour == LMSTUDIO:
            # `instance_id` names the loaded instance; LM Studio's own documented example
            # passes a plain model key for it, which is what the inventory reports back
            # as resident, so that is what is sent. A node running several instances of
            # one model under distinct ids would need those ids instead — it would fail
            # here rather than unload the wrong one, which is the safe direction.
            return await self._post_unload(
                f"{self._base}/api/v1/models/unload", {"instance_id": artifact},
                absent=(f"this LM Studio has no unload endpoint (needs 0.4.0 or newer); "
                        f"{artifact} stays resident"))
        return (False, f"model manager '{flavour}' cannot unload; {artifact} "
                       f"stays resident until its model server evicts it")

    async def _post_unload(self, url: str, payload: dict,
                           absent: str | None = None) -> tuple[bool, str | None]:
        """POST an unload and report the outcome. A 404 is its own answer where it means
        "this build predates the endpoint" rather than "something went wrong"."""
        try:
            resp = await self._client.post(url, json=payload)
            if resp.status_code == 404 and absent is not None:
                return (False, absent)
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
