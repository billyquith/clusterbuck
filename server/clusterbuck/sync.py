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
from fastapi import APIRouter, HTTPException, Request
from litellm import Router

from .fleet import Fleet, resolve_api_key

_log = logging.getLogger("clusterbuck.sync")

# Local/custom models aren't in LiteLLM's cost map and drop_params keeps unknown params
# from erroring; quiet the resulting per-call warnings so server logs stay clean.
litellm.suppress_debug_info = True
litellm.drop_params = True
logging.getLogger("LiteLLM").setLevel(logging.ERROR)

# Params we forward from the client body to the model server.
_PASSTHROUGH = {
    "temperature", "max_tokens", "top_p", "stop", "presence_penalty",
    "frequency_penalty", "response_format", "n", "seed", "tools", "tool_choice", "user",
}

_NOOP_KEY = "sk-noop"  # local servers ignore the key; LiteLLM requires one present


def build_router(fleet: Fleet, cloud_fallback_model: str | None = None) -> Router | None:
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
    kwargs: dict = {}
    if fallbacks:
        kwargs["fallbacks"] = fallbacks
    return Router(model_list=model_list, **kwargs)


sync_routes = APIRouter()


@sync_routes.post("/v1/chat/completions")
async def chat_completions(request: Request) -> dict:
    router: Router | None = getattr(request.app.state, "sync_router", None)
    if router is None:
        raise HTTPException(status_code=503, detail="sync plane not configured (no fleet.yaml)")

    body = await request.json()
    model = body.get("model")
    messages = body.get("messages")
    if not model or messages is None:
        raise HTTPException(status_code=422, detail="`model` and `messages` are required")

    kwargs = {k: v for k, v in body.items() if k in _PASSTHROUGH}
    try:
        resp = await router.acompletion(model=model, messages=messages, **kwargs)
    except Exception as e:  # unknown capability, or upstream/model-server failure
        raise HTTPException(status_code=502, detail=f"sync completion failed: {e}") from e
    return resp.model_dump()


@sync_routes.get("/v1/models")
async def list_models(request: Request) -> dict:
    fleet: Fleet | None = getattr(request.app.state, "fleet", None)
    caps = fleet.capabilities if fleet else {}
    data = [{"id": name, "object": "model", "owned_by": "clusterbuck"} for name in caps]
    return {"object": "list", "data": data}
