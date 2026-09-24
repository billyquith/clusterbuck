"""Stable error codes, and the one envelope every client-facing error is sent in.

A client could not tell an unknown alias from a dead model server from a worker failure:
all three arrived as prose, and branching on prose is how a UI ends up blaming the user
for an outage. So every refusal carries a `code` from a fixed table, and clients branch on
that — never on the HTTP status alone, never on the message (protocols.md §1c).

The envelope is OpenAI's error shape, `{"error": {"message", "type", "code", …}}`, because
the sync plane promises that a stock OpenAI SDK works unmodified: the SDK reads
`body["error"]` and exposes `.code`, and FastAPI's default `{"detail": …}` gave it nothing.
`detail` is still sent, holding the message, so a client written against the old shape
keeps working; it is deprecated.

`CODES` is checked against `contract/error-codes.json` by a conformance test rather than
read from it: the contract directory is not part of the installed package.
"""

from __future__ import annotations

from typing import Any

import httpx
import openai
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

# code → retryable. "Retryable" means the identical request may succeed if simply retried
# soon; it never means "could succeed once an operator changes the fleet".
CODES: dict[str, bool] = {
    "invalid_request": False,
    "unauthorized": False,
    "not_found": False,
    "fleet_not_configured": False,
    "capability_not_found": False,
    "capability_misconfigured": False,
    "ability_unsatisfied": False,
    "requirements_unsatisfied": False,
    "cloud_not_permitted": False,
    "cloud_budget_exhausted": False,
    "reservation_invalid": False,
    "conflict": False,
    "model_server_unreachable": True,
    "model_server_timeout": True,
    "model_server_error": True,
    "model_request_rejected": False,
    "artifact_not_installed": False,
    "model_substituted": False,
    "job_expired": False,
    "job_cancelled": False,
    "job_orphaned": False,
    "job_abandoned": True,
    "worker_failed": False,
    "internal_error": False,
}

# For an HTTPException raised without a code — operator and node routes mostly — so that
# even those answer in the envelope rather than falling back to bare prose.
_CODE_FOR_STATUS = {
    400: "invalid_request", 401: "unauthorized", 403: "unauthorized", 404: "not_found",
    409: "conflict", 422: "invalid_request", 502: "model_server_error",
    503: "fleet_not_configured",
}


class CbkError(HTTPException):
    """An HTTPException that knows its code. Subclassed so existing handlers still apply."""

    def __init__(self, code: str, message: str, status: int, **extra: Any) -> None:
        if code not in CODES:
            raise ValueError(f"unknown error code {code!r}")
        super().__init__(status_code=status, detail=message)
        self.code = code
        self.extra = {k: v for k, v in extra.items() if v is not None}


def error_body(code: str, message: Any, status: int, **extra: Any) -> dict:
    if status == 401:
        kind = "authentication_error"
    elif status < 500:
        kind = "invalid_request_error"
    else:
        kind = "api_error"
    text = message if isinstance(message, str) else "request validation failed"
    err = {"message": text, "type": kind, "code": code, "retryable": CODES[code]}
    err.update({k: v for k, v in extra.items() if v is not None})
    # `detail` keeps its old value, which for a validation error is pydantic's list.
    return {"error": err, "detail": message}


def error_response(code: str, message: Any, status: int, **extra: Any) -> JSONResponse:
    return JSONResponse(status_code=status, content=error_body(code, message, status, **extra))


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        if isinstance(exc, CbkError):
            code, extra = exc.code, exc.extra
        else:
            code = _CODE_FOR_STATUS.get(exc.status_code,
                                        "internal_error" if exc.status_code >= 500
                                        else "invalid_request")
            extra = {}
        resp = error_response(code, exc.detail, exc.status_code, **extra)
        for name, value in (exc.headers or {}).items():
            resp.headers[name] = value
        return resp

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        errors = exc.errors()
        # Name the first offending field, as OpenAI's `param` does. `loc` starts with
        # where it was found ("body", "header", …), which is not the field.
        loc = [str(p) for p in (errors[0].get("loc") or ())[1:]] if errors else []
        return error_response("invalid_request", _jsonable(errors), 422,
                              param=".".join(loc) or None)


def _jsonable(errors: list[dict]) -> list[dict]:
    # pydantic puts the raw exception in `ctx` for some errors; keep the rest.
    return [{k: v for k, v in e.items() if k in {"type", "loc", "msg", "input"}}
            for e in errors]


def classify_upstream(exc: BaseException) -> str:
    """Which code an upstream (model-server or provider) failure deserves.

    Decided by exception TYPE, never message text — but not by the outermost type alone.
    LiteLLM reports a refused connection as `litellm.InternalServerError`, wrapping the
    `openai.APIConnectionError` → `httpx.ConnectError` that actually happened, so reading
    only the outer class would call a dead server a broken one: the exact confusion the
    codes exist to remove. So the cause chain is searched for a transport failure first,
    and only then does the HTTP status of the outer error decide.
    """
    for cause in _chain(exc):
        # A timeout subclasses a connection error in both openai and httpx: test it first.
        if isinstance(cause, (openai.APITimeoutError, httpx.TimeoutException, TimeoutError)):
            return "model_server_timeout"
        if isinstance(cause, (openai.APIConnectionError, httpx.TransportError,
                              ConnectionError)):
            return "model_server_unreachable"
    status = getattr(exc, "status_code", None)
    if isinstance(status, int) and 400 <= status < 500:
        return "model_request_rejected"
    return "model_server_error"


def _chain(exc: BaseException):
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__
