"""The sync plane (protocols.md §1a): an OpenAI-compatible endpoint backed by LiteLLM.

clusterbuck adopts LiteLLM for route-now / health-check / load-balance / cloud-fallback
and does NOT reimplement OpenAI routing (ADR 5). A capability alias (e.g. `8b-extract`)
maps to a model-server deployment; LiteLLM calls that OpenAI-compatible endpoint DIRECTLY
— the async worker is not in the sync path.

Cloud fallback is opt-in (CBK_CLOUD_FALLBACK_MODEL). Default off ⇒ the sync plane is
local-only. Note the asymmetry with the async plane: the OpenAI request shape carries no
per-request privacy field, so sync cloud control is coarse (config-level, default-off)
rather than the per-job invariant async jobs get (decisions.md).

M1 is non-streaming; SSE streaming is a follow-up.
"""

from __future__ import annotations

import logging

import litellm
from fastapi import APIRouter, Request
from litellm import Router

from .errors import CbkError, classify_upstream
from .fleet import Fleet, resolve_api_key

_log = logging.getLogger("clusterbuck.sync")

# Local/custom models aren't in LiteLLM's cost map and drop_params keeps unknown params
# from erroring; quiet the resulting per-call warnings so server logs stay clean.
litellm.suppress_debug_info = True
litellm.drop_params = True
logging.getLogger("LiteLLM").setLevel(logging.ERROR)

# Params we forward from the client body to the model server.
_PASSTHROUGH = {
    "temperature", "max_tokens", "max_completion_tokens", "top_p", "stop",
    "presence_penalty", "frequency_penalty", "response_format", "n", "seed", "tools",
    "tool_choice", "user", "logprobs", "parallel_tool_calls",
}

_NOOP_KEY = "sk-noop"  # local servers ignore the key; LiteLLM requires one present


def build_router(fleet: Fleet, cloud_fallback_model: str | None = None,
                 timeout_s: float | None = None) -> Router | None:
    """Build a LiteLLM Router from the fleet's capabilities. None if there's nothing to serve.

    ADR 23 stands: this stays config-level for the sync plane. A no-host provider account
    (ADR 30, `model_server is None`) is included here too — the sync plane may as well use
    the same registered accounts — but it is unmetered (usage.py never captures a sync
    completion) and therefore ungated by the cloud budget (budget.py), same as the older
    single `CBK_CLOUD_FALLBACK_MODEL` always was.
    """
    model_list: list[dict] = []
    for name, spec in fleet.capabilities.items():
        if spec.model_server is None:
            # A registered provider account: no api_base, LiteLLM resolves the provider
            # from the "<provider>/<model>" string itself. Missing key ⇒ excluded rather
            # than a broken deployment entry (fail closed for routing, not for boot).
            key = resolve_api_key(spec)
            if not key:
                _log.warning(
                    "cloud capability %r has no usable API key (%s unset) — excluded from "
                    "the sync plane", name, spec.api_key_env or "no api_key_env configured",
                )
                continue
            model_list.append({
                "model_name": name,
                "litellm_params": {"model": spec.model, "api_key": key},
            })
            continue
        model_list.append({
            "model_name": name,
            "litellm_params": {
                "model": f"openai/{spec.model}",
                "api_base": spec.model_server,
                "api_key": resolve_api_key(spec) or _NOOP_KEY,
            },
        })

    fallbacks = None
    if cloud_fallback_model:
        model_list.append({
            "model_name": "cloud-fallback",
            # Provider + key come from the standard env (e.g. OPENAI_API_KEY);
            # LiteLLM owns that.
            "litellm_params": {"model": cloud_fallback_model},
        })
        fallbacks = [{name: ["cloud-fallback"]} for name in fleet.capabilities]

    if not model_list:
        return None
    # No hidden retries. LiteLLM retried a failed call twice by default, so a dead model
    # server cost an interactive client three connection attempts before it heard anything
    # — and then heard nothing it could act on. The refusal now says `retryable`, and
    # whether to retry is the client's decision, made with a person waiting. Fallbacks are
    # a separate mechanism and still apply.
    kwargs: dict = {"num_retries": 0}
    if fallbacks:
        kwargs["fallbacks"] = fallbacks
    # LiteLLM's own default is 6000s — long enough that a hung upstream reads as the
    # coordinator itself having stopped answering, to a person who is waiting right now.
    # This is what actually makes `model_server_timeout` reachable on the sync plane at
    # all; without it, the client's own (or a browser's) timeout fires first, with
    # nothing in `code`/`retryable` for it to act on.
    if timeout_s is not None:
        kwargs["timeout"] = timeout_s
    return Router(model_list=model_list, **kwargs)


sync_routes = APIRouter()


@sync_routes.post("/v1/chat/completions")
async def chat_completions(request: Request) -> dict:
    router: Router | None = getattr(request.app.state, "sync_router", None)
    fleet: Fleet | None = getattr(request.app.state, "fleet", None)
    if router is None or fleet is None:
        raise CbkError("fleet_not_configured",
                       "sync plane not configured (no fleet.yaml, or nothing in it servable)",
                       503)

    try:
        body = await request.json()
    except ValueError as e:
        # Starlette/json.loads raising a plain `ValueError` on invalid JSON reached no
        # handler in errors.py (neither an `HTTPException` nor `RequestValidationError`),
        # so it fell through as a bare 500 — the one thing every OTHER malformed-request
        # path here already avoids.
        raise CbkError("invalid_request", f"request body is not valid JSON: {e}", 422) \
            from e
    if not isinstance(body, dict):
        raise CbkError("invalid_request", "request body must be a JSON object", 422)

    if body.get("stream"):
        # Silently ignored otherwise: `_PASSTHROUGH` has no `stream`, so a client asking
        # for one got a normal 200 JSON body back instead — the stock SDK's streaming
        # iterator then yields zero chunks and raises nothing, which is a silently WRONG
        # answer, not a refusal. M1 is genuinely non-streaming (module docstring); this
        # is that limitation stated to the caller instead of hidden from them.
        raise CbkError("invalid_request",
                       "streaming is not supported by this endpoint (non-streaming only)",
                       422, param="stream")

    model = body.get("model")
    messages = body.get("messages")
    if not isinstance(model, str) or not model:
        raise CbkError("invalid_request", "`model` is required and must be a string", 422,
                       param="model")
    if not _looks_like_messages(messages):
        raise CbkError(
            "invalid_request",
            "`messages` is required and must be a list of {role, content} objects", 422,
            param="messages",
        )
    if "response_format" in body and not isinstance(body["response_format"], dict):
        raise CbkError("invalid_request", "`response_format` must be an object", 422,
                       param="response_format")
    if "tools" in body and not isinstance(body["tools"], list):
        raise CbkError("invalid_request", "`tools` must be an array", 422, param="tools")

    # Answered here rather than left to LiteLLM, which reports an unknown name as a
    # BadRequestError about "healthy deployments" — indistinguishable, from outside, from
    # an outage. A typo is the client's mistake and should read as one.
    spec = fleet.capabilities.get(model)
    if spec is None:
        raise CbkError("capability_not_found",
                       f"no capability {model!r} in the fleet registry; "
                       f"available: {sorted(fleet.capabilities)}", 404,
                       param="model", capability=model)
    if model not in router.get_model_names():
        # Registered, but build_router left it out: a provider account with no key.
        raise CbkError("capability_misconfigured",
                       f"capability {model!r} is registered but cannot be served "
                       f"(see the coordinator log)", 503, capability=model)

    kwargs = {k: v for k, v in body.items() if k in _PASSTHROUGH}
    try:
        resp = await router.acompletion(model=model, messages=messages, **kwargs)
    except Exception as e:  # an upstream / model-server failure
        code = classify_upstream(e)
        status = {"model_server_timeout": 504, "model_request_rejected": 400}.get(code, 502)
        # The upstream text is kept in the message for a person to read; the code is what
        # a client acts on, so the message can change without breaking anyone.
        raise CbkError(code, f"sync completion failed: {e}", status,
                       capability=model, model=spec.model,
                       use_async=await _async_could_serve(request, model, code)) from e
    return resp.model_dump()


def _looks_like_messages(messages: object) -> bool:
    """A shallow shape check, not full validation: LiteLLM/the model server are still the
    authority on what a valid message contains. This exists to stop a WRONGLY-shaped
    body — a string, a list of integers — from ever reaching `router.acompletion`, where
    LiteLLM's own internal `TypeError`/`AttributeError` gets wrapped as an
    `APIConnectionError` and misclassified by `classify_upstream` as the model server
    being unreachable: a client obeying `retryable` then retries a request that was
    always going to fail, forever, believing an outage is happening on the LAN when the
    request itself is malformed.
    """
    return (isinstance(messages, list) and len(messages) > 0
            and all(isinstance(m, dict) and "role" in m and "content" in m
                   for m in messages))


async def _async_could_serve(request: Request, capability: str, code: str) -> bool | None:
    """On a sync failure the server may be down, but is the capability?

    Only for "the server is not there" failures: a server that answered with an error or
    refused the request will answer the async plane the same way. It is worth saying when
    something is consuming the capability's queue — the case where a worker reaches its
    own model server and the coordinator cannot — or when a node could be woken to. A
    client then knows a `POST /jobs` is not the same dead end. None when unknowable.
    """
    if code not in ("model_server_unreachable", "model_server_timeout"):
        return None
    wake = getattr(request.app.state, "wake", None)
    if wake is None:
        return None
    if wake.wakeable_nodes(capability):
        return True
    try:
        return await wake.has_live_consumer(capability)
    except Exception:  # Redis down: the refusal must still go out
        _log.warning("could not read %s's consumers for use_async", capability,
                     exc_info=True)
        return None


@sync_routes.get("/v1/models")
async def list_models(request: Request) -> dict:
    fleet: Fleet | None = getattr(request.app.state, "fleet", None)
    caps = fleet.capabilities if fleet else {}
    data = [{"id": name, "object": "model", "owned_by": "clusterbuck"} for name in caps]
    return {"object": "list", "data": data}
