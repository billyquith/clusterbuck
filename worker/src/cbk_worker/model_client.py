"""Speaks the OpenAI-compatible wire protocol (POST /v1/chat/completions) to whatever model
server runs on this node — Ollama, llama.cpp, vLLM, LM Studio (protocols.md §3).

No vendor SDK: the request and response are plain JSON, so the servers are interchangeable.
"""

from __future__ import annotations

from typing import Any

import httpx

from .config import WorkerConfig, model_server_api_key
from .failure import JobFailure
from .models import Job
from .naming import artifact_aliases

# `params` is forwarded verbatim so the worker stays out of the way of whatever the model
# server supports — but five keys are the request envelope, not inference parameters, and
# letting a job set them breaks the loop rather than tuning it:
#
#   stream    — this path reads one JSON body; a streamed response fails to parse, so the
#               job returns `failed` for a param the client meant as a preference.
#   messages  — assembled above from the job's own messages/prompt.
#   response_format — forwarded ONLY when the job carries `requires.json_schema`; see
#               below. Dropped otherwise, as it always was.
#   api_key, api_base — this node's OWN model server and its key (if any) are node-local
#               config (CBK_MODEL_SERVER_URL / CBK_MODEL_SERVER_API_KEY), never a per-job
#               value. Letting a job set either would let any client redirect this worker's
#               HTTP call to an arbitrary endpoint and/or exfiltrate whatever key is
#               configured for it — the same class of hole ADR 26 closed for the HTTP API.
_PARAMS_NOT_FORWARDED = {"stream", "messages", "response_format", "api_key", "api_base"}


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

        # Structured output, once routing has earned the right to ask for it.
        #
        # `response_format` was dropped unconditionally, and the reason was sound at the
        # time: local servers do not reliably honour it, and a model that ignores the flag
        # returns prose to a caller who asked for JSON — a silent wrong answer, which is
        # worse than an explicit refusal. But dropping it left the `8b-extract` tier with
        # no way to ask for the one thing it exists to produce, and the two planes
        # disagreeing: `sync.py` forwarded the field while this path discarded it, so the
        # same job against the same model behaved differently depending on which door it
        # came in.
        #
        # `requires.json_schema` resolves that. It is a HARD requirement the coordinator
        # filters on before comparing ability (ADR 37), so by the time a job reaches here
        # carrying it, the artifact pinned on it is declared capable of honouring the
        # flag. The original objection — asking a model that cannot do it — is exactly
        # what that filter removes. Without the requirement the field stays a dropped
        # hint, because nothing has checked.
        if job.wants_structured_output:
            request["response_format"] = (
                (job.params or {}).get("response_format") or {"type": "json_object"}
            )

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
        key = model_server_api_key()
        headers = {"Authorization": f"Bearer {key}"} if key else None
        resp = await self._client.post(url, json=request, headers=headers)
        resp.raise_for_status()
        body = resp.json()
        _refuse_malformed_completion(body)
        _refuse_substituted_model(request["model"], body)
        usage = body.get("usage") if isinstance(body, dict) else None
        return body, usage


def _refuse_malformed_completion(body: Any) -> None:
    """Fail rather than record `done` for a 200 that is not a completion at all.

    `raise_for_status()` above only catches a non-2xx status; a model server can answer
    200 with `{}`, `[]`, `{"choices": []}`, or `{"error": "..."}` — a proxy returning an
    error as a 200, a server not fully warm yet, a route that exists but is not this one
    — and every one of those was written as a SUCCESSFUL result. `completion` is
    required to satisfy `contract/result.schema.json`, so a client indexing
    `choices[0]` crashed on a job the coordinator had told it was `done`. This is exactly
    the case §3 already has a code for: the server answered with a malformed response.
    """
    if (isinstance(body, dict) and isinstance(body.get("choices"), list)
        and len(body["choices"]) >= 1
        and all(isinstance(c, dict) and "message" in c for c in body["choices"])):
        return
    raise JobFailure(
        "model_server_error",
        f"model server answered 200 with no usable completion "
        f"(missing/empty/malformed `choices`): {body!r:.200}"
    )


def _refuse_substituted_model(requested: str, body: Any) -> None:
    """Fail if the model server answered with a model other than the one asked for.

    The last hole in the pinning chain, and a real one: LM Studio returns HTTP 200 for a
    request naming a model it does not have — including an id that exists nowhere — and
    answers with whatever happens to be loaded. Observed live, with a nonsense id, against
    a fleet whose ability matrix then recorded an EMBEDDING model scoring 7.0 at code.
    Those were another model's answers filed under the wrong name.

    The coordinator pins the artifact whose measured ability cleared the job's bar, and the
    worker refuses a pin it does not have installed — but neither can see past the model
    server. This can: the OpenAI response echoes the model that actually ran, so the
    substitution is detectable in the reply the server itself sent.

    Silent when the field is absent or unparseable: not every server echoes it, and a
    missing field is unknown rather than wrong — the same rule the other gates follow.
    Aliases are normalised, so an implicit `:latest` is not mistaken for a substitution.
    """
    if not isinstance(body, dict):
        return
    served = body.get("model")
    if not isinstance(served, str) or not served.strip():
        return
    if artifact_aliases(served) & artifact_aliases(requested):
        return
    raise JobFailure(
        "model_substituted",
        f"model server answered with {served!r} but the job pinned {requested!r}. "
        f"Refusing a result measured against, or attributed to, the wrong model."
    )
