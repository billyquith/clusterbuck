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


# --- submit-time routing refusals (all 422; the code tells them apart) ---

_MSG = [{"role": "user", "content": "x"}]


@pytest.mark.parametrize("job, code", [
    ({"task_class": "extract", "min_ability": 10}, "ability_unsatisfied"),
    ({"capability": "8b-extract", "requires": {"vision": True}}, "requirements_unsatisfied"),
    ({"capability": "no-such-tier"}, "capability_not_found"),
    ({"capability": "claude-sonnet", "privacy": "local_only"}, "cloud_not_permitted"),
    ({"capability": "claude-sonnet", "privacy": "cloud_ok", "urgency": "waitable"},
     "cloud_not_permitted"),
])
def test_each_routing_refusal_has_its_own_code(client, job, code):
    resp = client.post("/jobs", json={"messages": _MSG, **job})
    assert resp.status_code == 422, resp.text
    body = resp.json()
    ENVELOPE.validate(body)
    assert body["error"]["code"] == code


def test_a_cloud_refusal_says_whether_policy_or_budget(client, monkeypatch):
    import clusterbuck.routing as routing
    from clusterbuck.budget import BudgetDecision

    resp = client.post("/jobs", json={"messages": _MSG, "capability": "claude-sonnet",
                                      "privacy": "local_only"})
    assert resp.json()["error"]["reason"] == "privacy"

    monkeypatch.setattr(routing, "check_cloud_budget",
                        lambda *a, **k: BudgetDecision(False, "monthly cap reached"))
    resp = client.post("/jobs", json={"messages": _MSG, "capability": "claude-sonnet",
                                      "privacy": "cloud_ok", "urgency": "necessary"})
    assert resp.json()["error"]["code"] == "cloud_budget_exhausted"
    assert resp.json()["error"]["reason"] == "budget"


# --- a job's error_code, as GET /jobs/{id} resolves it ---

def _job_with_result(client, redis_url, result: dict | None, status: str | None = None):
    import anyio
    from clusterbuck.queue import Queue

    job = client.post("/jobs", json={"capability": "8b-extract", "messages": _MSG}).json()
    if status:
        client.app.state.store.set_status(job["id"], status)
    if result is not None:
        async def write():
            q = Queue.from_url(redis_url)
            try:
                await q.write_result(job["result_key"], {
                    "job_id": job["id"], "worker": "node-a",
                    "completed_at": "2026-09-02T00:00:00Z", **result})
            finally:
                await q.aclose()
        anyio.run(write)
    return client.get(f"/jobs/{job['id']}").json()


def test_a_coded_failure_passes_its_code_through(client, redis_url):
    view = _job_with_result(client, redis_url, {
        "status": "failed", "error": "refused", "error_code": "model_server_unreachable"})
    assert view["error_code"] == "model_server_unreachable"
    assert view["error"] == "refused"  # still the string it always was


def test_an_uncoded_failure_is_a_worker_failure(client, redis_url):
    """An older worker writes no code; the job still failed, and says so."""
    view = _job_with_result(client, redis_url, {"status": "failed", "error": "boom"})
    assert view["error_code"] == "worker_failed"


def test_the_coordinators_own_terminal_states_are_derived(client, redis_url):
    # The deadline sweep and DELETE write no result blob at all.
    assert _job_with_result(client, redis_url, None, "expired")["error_code"] == "job_expired"
    assert _job_with_result(client, redis_url, None,
                            "cancelled")["error_code"] == "job_cancelled"


def test_success_and_waiting_carry_no_code(client, redis_url):
    assert _job_with_result(client, redis_url, None)["error_code"] is None
    done = _job_with_result(client, redis_url, {"status": "done", "completion": {}})
    assert done["error_code"] is None
