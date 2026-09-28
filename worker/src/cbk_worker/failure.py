"""Why a job failed, as a stable code the client can branch on (protocols.md §1c).

A failed result always carried `error`, free text, and a client could not tell "the model
server is not there" from "it answered with a 500" from "this node lacks the model"
without reading it. So the result also carries `error_code`, from the job-plane codes in
`contract/error-codes.json`. A conformance test checks `CODES` against that file; it is
not read at runtime, because the zipapp does not ship the contract directory.

Classified by exception TYPE, never message text. A worker too old to set a code still
fails honestly: the coordinator reads the absence as `worker_failed`.
"""

from __future__ import annotations

import json

import httpx

# The job-plane codes this worker can emit. A subset of the contract's job plane: the
# rest (expired, orphaned, cancelled, abandoned, misconfigured) are coordinator decisions.
CODES = frozenset({
    "model_server_unreachable",
    "model_server_timeout",
    "model_server_error",
    "model_request_rejected",
    "artifact_not_installed",
    "model_substituted",
    "worker_failed",
})


class JobFailure(RuntimeError):
    """A failure this worker has already classified."""

    def __init__(self, code: str, message: str) -> None:
        if code not in CODES:
            raise ValueError(f"unknown error code {code!r}")
        super().__init__(message)
        self.code = code


def code_for(exc: BaseException) -> str:
    """The code a failed job's result carries for this exception."""
    if isinstance(exc, JobFailure):
        return exc.code
    # A timeout is a transport error in httpx, so it is tested first.
    if isinstance(exc, httpx.TimeoutException):
        return "model_server_timeout"
    if isinstance(exc, httpx.TransportError):
        return "model_server_unreachable"
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        # A 4xx is the model server refusing THIS request (a context window, an unknown
        # parameter); a 5xx is the server failing. Different messages to a user. But not
        # every 4xx says the job's own content is at fault — none of `_STATUS_OVERRIDE`
        # does, so retrying the SAME request may still succeed, unlike a genuine content
        # refusal. 408 is a timeout by name; 429 is the server's own capacity; 401/403/404
        # are the server refusing to answer at all — an operator problem (a bad node-local
        # key, a model it does not have loaded), the same bucket an unreachable or
        # malformed response already falls into.
        if status < 500:
            return _STATUS_OVERRIDE.get(status, "model_request_rejected")
        return "model_server_error"
    if isinstance(exc, json.JSONDecodeError):
        # It answered 2xx with a body that is not JSON: the server is misbehaving.
        return "model_server_error"
    return "worker_failed"


_STATUS_OVERRIDE = {
    401: "model_server_error", 403: "model_server_error", 404: "model_server_error",
    408: "model_server_timeout", 429: "model_server_error",
}
