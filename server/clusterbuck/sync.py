"""The sync plane (protocols.md §1a): an OpenAI-compatible endpoint backed by LiteLLM.

clusterbuck adopts LiteLLM for route-now / health-check / load-balance / cloud-fallback
and does NOT reimplement OpenAI routing (ADR 5). A capability alias (e.g. `8b-extract`)
maps to a model-server deployment; LiteLLM calls that OpenAI-compatible endpoint DIRECTLY
— the async worker is not in the sync path.

Cloud fallback is opt-in, per tier: a capability's `cloud_fallback` names the provider
account LiteLLM falls back to when that tier's model server fails. None declared ⇒ the
sync plane is local-only. Note the asymmetry with the async plane: the OpenAI request
shape carries no per-request privacy field, so sync cloud control is coarse (config-level,
default-off) rather than the per-job invariant async jobs get.

What it is NOT outside of any more is the money. Every completion served here is metered
— metadata only, the same usage row an async job gets (§9) — and a cloud deployment is
only reachable while the budget allows it. The sync plane is the `urgent` rung of the
urgency ladder (a person is waiting), so it may draw the reserve; past the cap it is
served by a router that has no cloud deployments in it at all.

M1 is non-streaming; SSE streaming is a follow-up.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

import litellm
from fastapi import APIRouter, Request
from litellm import Router

from .budget import check_cloud_budget
from .cloud_executor import provider_of
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

# The deployment name CBK_CLOUD_FALLBACK_MODEL registers under (deprecated: per-tier
# `cloud_fallback` replaces it). Not a capability, so its usage rows carry none.
LEGACY_FALLBACK = "cloud-fallback"


def build_router(fleet: Fleet, cloud_fallback_model: str | None = None,
                 timeout_s: float | None = None, *,
                 local_only: bool = False) -> Router | None:
    """Build a LiteLLM Router from the fleet's capabilities. None if there's nothing to serve.

    ADR 23 stands: this stays config-level for the sync plane. A no-host provider account
    (ADR 30, `model_server is None`) is included here too, so the sync plane can use the
    same registered accounts, and so is the fallback each tier's `cloud_fallback` names.

    `local_only` builds the router the budget gate falls back to: no cloud deployment, no
    cloud fallback — nothing in it can spend. Two routers rather than one filtered per
    request, because LiteLLM decides a fallback inside `acompletion`, after anything this
    module could check; the only way to be sure a call cannot reach a provider is to hand
    it a router that does not know one.
    """
    model_list: list[dict] = []
    for name, spec in fleet.capabilities.items():
        if local_only and spec.cloud:
            continue
        if spec.model_server is None:
            # A registered provider account: no api_base, LiteLLM resolves the provider
            # from the "<provider>/<model>" string itself. Missing key ⇒ excluded rather
            # than a broken deployment entry (fail closed for routing, not for boot).
            key = resolve_api_key(spec)
            if not key:
                if not local_only:
                    _log.warning(
                        "cloud capability %r has no usable API key (%s unset) — excluded "
                        "from the sync plane", name,
                        spec.api_key_env or "no api_key_env configured",
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

    served = {d["model_name"] for d in model_list}
    # Per tier, to the account it names — a deployment with its own key, unlike the
    # legacy global fallback, which was registered with none and so could only ever work
    # if the provider's own env var happened to be set.
    fallbacks = [{name: [spec.cloud_fallback]}
                 for name, spec in fleet.capabilities.items()
                 if name in served and spec.cloud_fallback in served]
    if cloud_fallback_model and not local_only:
        model_list.append({
            "model_name": LEGACY_FALLBACK,
            # Provider + key come from the standard env (e.g. OPENAI_API_KEY);
            # LiteLLM owns that.
            "litellm_params": {"model": cloud_fallback_model},
        })
        has_own = {next(iter(f)) for f in fallbacks}
        fallbacks += [{name: [LEGACY_FALLBACK]} for name in served if name not in has_own]
    fallbacks = fallbacks or None

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

    from .config import settings

    budget = check_cloud_budget(
        request.app.state.store, monthly_cap=settings.cloud_budget_monthly,
        urgency="urgent", reserve_fraction=settings.cloud_budget_reserve_fraction)
    if not budget.allowed:
        if spec.cloud:
            raise CbkError("cloud_budget_exhausted",
                           f"capability {model!r} is cloud-backed and excluded: "
                           f"{budget.reason}", 422, reason="budget", capability=model)
        # Served locally or not at all: this router has no deployment that can spend.
        router = getattr(request.app.state, "sync_router_local", None) or router

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
    out = resp.model_dump()
    try:
        _meter(request.app.state.store, fleet, resp, out)
    except Exception:  # an answer the client is waiting for beats a usage row
        _log.warning("could not meter a sync completion", exc_info=True)
    return out


def _meter(store, fleet: Fleet, resp, body: dict) -> None:
    """Write the usage row for one sync completion — METADATA ONLY (design.md §9).

    Inline, not a LiteLLM success callback: `litellm.callbacks` is process-global, and the
    cloud executor calls `litellm.acompletion` in the same process, so a callback would
    meter every async cloud job a second time.

    The row is attributed to the deployment that actually ANSWERED, which after a
    fallback is not the one the client named — LiteLLM reports it as the model group.
    """
    from .usage import _job_cost, cached_input_tokens

    hidden = getattr(resp, "_hidden_params", None) or {}
    group = (hidden.get("additional_headers") or {}).get("x-litellm-model-group")
    spec = fleet.capabilities.get(group) if group else None
    usage = body.get("usage") or {}
    tin = int(usage.get("prompt_tokens") or 0)
    tout = int(usage.get("completion_tokens") or 0)
    if spec is not None:
        venue = "cloud" if spec.cloud else "local"
        # The whole body is priced (LiteLLM wants the response shape); only the numbers
        # that come back from it are written anywhere.
        cost, source = _job_cost(fleet, group, tin, tout, venue=venue, completion=body)
        node = f"cloud:{provider_of(spec.model)}" if spec.cloud else "sync"
    else:
        # The legacy global fallback: a provider LiteLLM priced, with no tier behind it.
        venue, node = "cloud", f"cloud:{provider_of(hidden.get('litellm_model_name') or '')}"
        cost = float(hidden.get("response_cost") or 0.0)
        source = "litellm" if hidden.get("response_cost") is not None else "none"
    now = datetime.now(UTC)
    store.record_usage(
        job_id=f"sync-{uuid.uuid4().hex}", ts=now.isoformat().replace("+00:00", "Z"),
        capability=group if spec is not None else None, model=body.get("model"),
        node=node, venue=venue, tokens_in=tin, tokens_out=tout,
        tokens_cached_in=cached_input_tokens(usage), outcome="done", cost=cost,
        cost_source=source, day=now.strftime("%Y-%m-%d"),
    )


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
