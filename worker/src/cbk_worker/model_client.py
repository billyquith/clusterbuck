"""Speaks the OpenAI-compatible wire protocol (POST /v1/chat/completions) to whatever model
server runs on this node — Ollama, llama.cpp, vLLM, LM Studio (protocols.md §3).

No vendor SDK: the request and response are plain JSON, so the servers are interchangeable.
"""

from __future__ import annotations

from typing import Any

import httpx

from .config import WorkerConfig
from .models import Job

# `params` is forwarded verbatim so the worker stays out of the way of whatever the model
# server supports — but three keys are the request envelope, not inference parameters, and
# letting a job set them breaks the loop rather than tuning it:
#
#   stream    — this path reads one JSON body; a streamed response fails to parse, so the
#               job returns `failed` for a param the client meant as a preference.
#   messages  — assembled above from the job's own messages/prompt.
#   response_format — a hint only (protocols.md §3), deliberately not passed on.
_PARAMS_NOT_FORWARDED = {"stream", "messages", "response_format"}


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
            if name in _PARAMS_NOT_FORWARDED:
                continue
            request[name] = value

        url = self._cfg.model_server_url.rstrip("/") + "/chat/completions"
        resp = await self._client.post(url, json=request)
        resp.raise_for_status()
        body = resp.json()
        usage = body.get("usage") if isinstance(body, dict) else None
        return body, usage
