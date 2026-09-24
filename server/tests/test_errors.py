"""The error envelope and the code table (protocols.md §1c)."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import litellm
import openai
import pytest
from clusterbuck.errors import CODES, CbkError, classify_upstream, error_body
from jsonschema import Draft202012Validator

CONTRACT = Path(__file__).resolve().parents[2] / "contract"
ENVELOPE = Draft202012Validator(json.loads((CONTRACT / "error.schema.json").read_text()))


def test_codes_match_the_contract():
    """The table is duplicated in code on purpose (contract/ is not installed); this is the
    check that the copy has not drifted, in membership or in what `retryable` promises."""
    table = json.loads((CONTRACT / "error-codes.json").read_text())["codes"]
    assert CODES == {code: spec["retryable"] for code, spec in table.items()}


@pytest.mark.parametrize("code", sorted(CODES))
def test_every_code_builds_a_conforming_envelope(code):
    for status in (400, 401, 404, 422, 502, 503):
        body = error_body(code, "why", status, capability="x", use_async=None)
        ENVELOPE.validate(body)
        assert "use_async" not in body["error"]  # None is omitted, not sent as null
        assert body["detail"] == "why"


def test_an_unknown_code_is_a_bug_at_the_raise_site():
    with pytest.raises(ValueError):
        CbkError("server_down", "nope", 502)


def _req() -> httpx.Request:
    return httpx.Request("POST", "http://model/v1/chat/completions")


def _litellm(cls, cause: BaseException | None = None, **kw):
    exc = cls(message="upstream", model="m", llm_provider="openai", **kw)
    exc.__cause__ = cause
    return exc


def test_a_refused_connection_is_unreachable_whatever_litellm_calls_it():
    # The shape LiteLLM actually raises for a closed port (checked against a real Router):
    # InternalServerError, caused by openai.APIConnectionError, caused by httpx.ConnectError.
    transport = httpx.ConnectError("refused")
    conn = openai.APIConnectionError(request=_req())
    conn.__cause__ = transport
    exc = _litellm(litellm.InternalServerError, conn)
    assert classify_upstream(exc) == "model_server_unreachable"


def test_a_timeout_is_a_timeout_not_a_connection_error():
    # APITimeoutError subclasses APIConnectionError, so the order of the tests matters.
    assert classify_upstream(_litellm(litellm.Timeout)) == "model_server_timeout"
    assert classify_upstream(openai.APITimeoutError(request=_req())) == "model_server_timeout"


def test_an_upstream_4xx_is_the_request_and_a_5xx_is_the_server():
    assert classify_upstream(
        _litellm(litellm.ContextWindowExceededError)) == "model_request_rejected"
    assert classify_upstream(_litellm(litellm.BadRequestError)) == "model_request_rejected"
    assert classify_upstream(_litellm(litellm.ServiceUnavailableError)) == "model_server_error"
    assert classify_upstream(RuntimeError("anything else")) == "model_server_error"


def test_validation_errors_and_bare_http_errors_answer_in_the_envelope(client):
    resp = client.post("/jobs", json={"messages": "not a list"})
    assert resp.status_code == 422
    body = resp.json()
    ENVELOPE.validate(body)
    assert body["error"]["code"] == "invalid_request"
    assert isinstance(body["detail"], list)  # pydantic's list, as before the envelope

    resp = client.get("/jobs/job_nope")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


def test_a_bad_idempotency_key_names_the_header(client):
    resp = client.post("/jobs", headers={"Idempotency-Key": " "},
                       json={"capability": "8b-extract",
                             "messages": [{"role": "user", "content": "x"}]})
    assert resp.status_code == 400
    assert resp.json()["error"]["param"] == "Idempotency-Key"
