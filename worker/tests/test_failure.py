"""Failure classification (failure.py) — the code a client branches on (protocols.md §1c)."""

from __future__ import annotations

import json

import httpx
import pytest
from cbk_worker.failure import CODES, JobFailure, code_for


def _http_status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "http://model/v1/chat/completions")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError("upstream", request=request, response=response)


@pytest.mark.parametrize(("status", "code"), [
    (400, "model_request_rejected"),
    (413, "model_request_rejected"),
    (422, "model_request_rejected"),
])
def test_a_genuine_content_refusal_stays_rejected(status, code):
    """A context window, a malformed parameter: retrying the SAME request just fails
    again, so this is the one bucket that must stay `retryable: false`."""
    assert code_for(_http_status_error(status)) == code


@pytest.mark.parametrize(("status", "code"), [
    (401, "model_server_error"),
    (403, "model_server_error"),
    (404, "model_server_error"),
    (429, "model_server_error"),
    (408, "model_server_timeout"),
])
def test_a_status_that_is_not_about_the_jobs_own_content_is_not_rejected(status, code):
    """None of these says the JOB is malformed, so blaming the client fixes nothing:
    408 is a timeout by name, 429 is the server's own capacity, and 401/403/404 are the
    server refusing to answer at all — a bad node-local key, a model it does not have
    loaded — an operator problem, not a bad request. Previously every one of these
    landed on `model_request_rejected`, `retryable: false`, which told a client to give
    up on work a fixed key or a retry could still serve."""
    assert code_for(_http_status_error(status)) == code
    assert code in CODES


def test_a_5xx_is_still_a_server_error():
    assert code_for(_http_status_error(500)) == "model_server_error"
    assert code_for(_http_status_error(503)) == "model_server_error"


def test_a_timeout_is_a_timeout_not_a_transport_error():
    assert code_for(httpx.ReadTimeout("slow")) == "model_server_timeout"


def test_a_connection_refusal_is_unreachable():
    assert code_for(httpx.ConnectError("refused")) == "model_server_unreachable"


def test_malformed_json_is_a_server_error():
    exc = json.JSONDecodeError("bad", "not json", 0)
    assert code_for(exc) == "model_server_error"


def test_a_job_failure_carries_its_own_code():
    assert code_for(JobFailure("artifact_not_installed", "nope")) == "artifact_not_installed"


def test_anything_unclassified_is_worker_failed():
    assert code_for(RuntimeError("something else entirely")) == "worker_failed"
