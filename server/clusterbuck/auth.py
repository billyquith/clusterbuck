"""Shared-secret authentication for the coordinator's HTTP surface.

DESIGN.md's security section calls for "a shared key at minimum on the gateway/queue".
This is that key. It is deliberately simple — clusterbuck is LAN-only infrastructure, not a
multi-tenant service, so there are no user accounts, just one operator secret.

Configured via `CBK_API_KEY`. **Unset ⇒ auth is disabled** so local dev and the e2e scripts
work out of the box; that state is logged as a warning at startup because an unauthenticated
coordinator can be driven by any host that can reach it.

Accepted, in order: `X-CBK-Api-Key` header, `Authorization: Bearer <key>`, or a `cbk_key`
cookie (so the htmx dashboard works in a browser, which cannot set custom headers).

Two paths are exempt because they carry their own credential and are how a node bootstraps:

* `POST /nodes/enroll` — authenticated by a one-time join token, which is burned on use.
* `POST /nodes/{id}/heartbeat` — authenticated by that node's `node_key`.

`GET /healthz` is exempt so liveness probes work, and `/static/*` because it is vendored
CSS/JS. `GET /releases/*` is exempt because the update channel is untrusted BY DESIGN
(ADR 13/38) — see the route. Everything else — including the dashboard, job submission,
and every model-management endpoint — requires the key.
"""

from __future__ import annotations

import logging
import secrets

from fastapi import Request

from .errors import error_response

_log = logging.getLogger("clusterbuck.auth")

COOKIE_NAME = "cbk_key"
HEADER_NAME = "x-cbk-api-key"


def _is_exempt(path: str) -> bool:
    if path in ("/healthz", "/nodes/enroll"):
        return True
    # Worker bootstrap: a joining machine has no operator key, by design — that key never
    # leaves the coordinator. These two carry their own credential (CBK_JOIN_PASSWORD) and
    # are 404 unless it is configured, so exempting them adds no surface by default.
    #
    # Load-bearing: the operator-key middleware does NOT protect these, so the join
    # password is the only thing in front of the broker URL that /nodes/bootstrap returns.
    if path in ("/nodes/bootstrap", "/worker/artifact"):
        return True
    if path.startswith("/static/"):
        return True
    # Signed release artifacts. Unlike `/worker/artifact` — the BOOTSTRAP path, gated by
    # the join password the joining script already holds — this one is fetched by a worker
    # already in the field, which has no operator key and must never be given one. The
    # security boundary here is the ECDSA signature over (version, rid, sha256, channel,
    # url, protocol_version) plus the digest check, both applied before a byte is written.
    # An attacker who can serve this file cannot make a worker install it, so requiring a
    # credential would buy nothing and would break every worker already deployed (ADR 38).
    if path.startswith("/releases/"):
        return True
    # A node's own heartbeat is authenticated by its node_key, not the shared secret.
    if path.startswith("/nodes/") and path.endswith("/heartbeat"):
        return True
    return False


def presented_key(request: Request) -> tuple[str | None, str]:
    """Pull the candidate key. Returns (key, source) — source drives cookie-setting."""
    header = request.headers.get(HEADER_NAME)
    if header:
        return header, "header"
    authorization = request.headers.get("authorization")
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip(), "header"
    cookie = request.cookies.get(COOKIE_NAME)
    if cookie:
        return cookie, "cookie"
    # Browser convenience only: visiting /?key=… once exchanges the key for a cookie, so the
    # dashboard's htmx fragments authenticate thereafter. A key in a URL can leak via logs
    # and Referer headers, so this is a LAN-admin affordance, not the recommended path.
    return request.query_params.get("key"), "query"


def key_matches(presented: str | None, configured: str) -> bool:
    """Constant-time comparison — never leak the key through response timing."""
    if not presented:
        return False
    return secrets.compare_digest(presented, configured)


def install_auth(app, api_key: str | None) -> None:
    """Attach the shared-secret gate. A falsy key leaves the surface open (dev mode)."""
    if not api_key:
        _log.warning(
            "CBK_API_KEY is not set — the coordinator API is UNAUTHENTICATED. Any host that "
            "can reach this port can submit jobs, mint join tokens, and approve model "
            "installs/removals. Set CBK_API_KEY for anything beyond local development."
        )
        return

    @app.middleware("http")
    async def _auth(request: Request, call_next):
        if _is_exempt(request.url.path):
            return await call_next(request)
        presented, source = presented_key(request)
        if not key_matches(presented, api_key):
            # Built here, not raised: middleware runs outside the app's exception
            # handlers, so this is the one refusal that must assemble its own envelope.
            return error_response(
                "unauthorized", "missing or invalid API key (X-CBK-Api-Key)", 401)
        response = await call_next(request)
        if source == "query":
            # Exchange the URL key for a cookie so the dashboard keeps working without it.
            response.set_cookie(
                COOKIE_NAME, presented or "", httponly=True, samesite="strict"
            )
        return response

    _log.info("shared-secret auth enabled on the coordinator API")
