"""Speaks the OpenAI-compatible wire protocol (POST /v1/chat/completions) to whatever model
server runs on this node — Ollama, llama.cpp, vLLM, LM Studio (protocols.md §3).

No vendor SDK: the request and response are plain JSON, so the servers are interchangeable.
"""

from __future__ import annotations

from typing import Any

import httpx

from .config import WorkerConfig
from .models import Job


class ModelClient:
    def __init__(self, client: httpx.AsyncClient, cfg: WorkerConfig) -> None:
        self._client = client
        self._cfg = cfg

    async def complete(self, job: Job) -> tuple[dict[str, Any], dict[str, Any] | None]:
        """Run one completion. Returns (completion, usage-or-None)."""
        if job.messages:
            messages = [{"role": m.get("role", ""), "content": m.get("content", "")}
                        for m in job.messages]
        else:
            messages = [{"role": "user", "content": job.prompt or ""}]

        request: dict[str, Any] = {
            "model": self._cfg.model_name,
            "messages": messages,
            "stream": False,
        }

        # Inference params pass straight through (temperature, max_tokens, …).
        #
        # Applied AFTER "model" is set, and that ordering is load-bearing: a job may pin the
        # artifact it must run on (the eval harness pins one per job — model-evaluation.md),
        # and the pin arrives in `params`. Setting the configured model afterwards would
        # silently score the wrong model.
        for name, value in (job.params or {}).items():
            if name == "response_format":
                continue        # a hint only (protocols.md §3)
            request[name] = value

        url = self._cfg.model_server_url.rstrip("/") + "/chat/completions"
        resp = await self._client.post(url, json=request)
        resp.raise_for_status()
        body = resp.json()
        usage = body.get("usage") if isinstance(body, dict) else None
        return body, usage
