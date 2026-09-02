"""Python-side contract conformance (ADR 22).

Asserts the shared JSON Schema in contract/ accepts the valid fixtures and rejects the
invalid ones, and that the server's Pydantic models round-trip into schema-valid wire
shapes. The worker runs the mirror of this against the same files, so the two type
definitions cannot drift.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from clusterbuck.models import (
    EnrollRequest,
    HeartbeatRequest,
    HwProbe,
    JobRecord,
    Message,
    Privacy,
    Submitter,
    Urgency,
)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def _validator(contract_dir: Path, name: str) -> Draft202012Validator:
    schema = load_json(contract_dir / name)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def test_job_schema_accepts_valid(contract_dir):
    v = _validator(contract_dir, "job.schema.json")
    v.validate(load_json(contract_dir / "examples" / "job.valid.json"))


def test_job_schema_rejects_invalid(contract_dir):
    v = _validator(contract_dir, "job.schema.json")
    errors = list(v.iter_errors(load_json(contract_dir / "examples" / "job.invalid.json")))
    assert errors, "job.invalid.json should violate the schema"


def test_result_schema_accepts_valid(contract_dir):
    v = _validator(contract_dir, "result.schema.json")
    v.validate(load_json(contract_dir / "examples" / "result.valid.json"))


def test_result_schema_rejects_invalid(contract_dir):
    v = _validator(contract_dir, "result.schema.json")
    errors = list(
        v.iter_errors(load_json(contract_dir / "examples" / "result.invalid.json"))
    )
    assert errors, "result.invalid.json should violate the schema"


def test_pydantic_jobrecord_matches_schema(contract_dir):
    """A JobRecord the server would enqueue must satisfy the shared schema."""
    v = _validator(contract_dir, "job.schema.json")
    record = JobRecord(
        id="job_abc",
        created_at="2026-07-24T18:30:00Z",
        capability="8b-extract",
        messages=[Message(role="user", content="hi")],
        params={"temperature": 0.2, "max_tokens": 128},
        urgency=Urgency.waitable,
        escalate_after_min=10,
        privacy=Privacy.local_only,
        result_key="res_abc",
    )
    v.validate(record.to_wire())


def test_pydantic_jobrecord_with_submitter_matches_schema(contract_dir):
    """Caller provenance rides the wire record, so it must satisfy the shared schema
    too — the worker parses this payload (protocols.md §1b/§2)."""
    v = _validator(contract_dir, "job.schema.json")
    record = JobRecord(
        id="job_ghi",
        created_at="2026-07-24T18:30:00Z",
        capability="8b-extract",
        prompt="extract the dates",
        urgency=Urgency.waitable,
        privacy=Privacy.local_only,
        result_key="res_ghi",
        submitter=Submitter(
            app="nightly-importer",
            instance="workstation-2",
            request_id="req_4f9c1e70a2",
            submitted_at="2026-07-24T18:29:59Z",
        ),
    )
    wire = record.to_wire()
    v.validate(wire)
    assert wire["submitter"]["request_id"] == "req_4f9c1e70a2"


def test_jobrecord_without_submitter_omits_it(contract_dir):
    """`exclude_none` must drop it entirely rather than send `"submitter": null` — the
    schema types it as an object, and a null would be a shape a consumer must special-case."""
    v = _validator(contract_dir, "job.schema.json")
    record = JobRecord(
        id="job_jkl",
        created_at="2026-07-24T18:30:00Z",
        capability="8b-extract",
        prompt="no provenance here",
        urgency=Urgency.waitable,
        privacy=Privacy.local_only,
        result_key="res_jkl",
    )
    wire = record.to_wire()
    v.validate(wire)
    assert "submitter" not in wire


def test_submitter_is_identification_only_not_a_dedup_key(contract_dir):
    """Two DIFFERENT jobs may legitimately share a request_id — that is precisely how a
    retried call is expressed. The schema must not constrain it to be unique, or the
    coordinator would reject the very signal the field exists to carry."""
    v = _validator(contract_dir, "job.schema.json")
    shared = Submitter(app="importer", instance="host-1", request_id="req_same")
    for job_id in ("job_1", "job_2"):
        wire = JobRecord(
            id=job_id, created_at="2026-07-24T18:30:00Z", capability="8b-extract",
            prompt="same logical call, retried", urgency=Urgency.waitable,
            privacy=Privacy.local_only, result_key=f"res_{job_id}", submitter=shared,
        ).to_wire()
        v.validate(wire)


def test_pydantic_prompt_form_matches_schema(contract_dir):
    v = _validator(contract_dir, "job.schema.json")
    record = JobRecord(
        id="job_def",
        created_at="2026-07-24T18:30:00Z",
        capability="8b-extract",
        prompt="summarize this",
        urgency=Urgency.necessary,
        privacy=Privacy.cloud_ok,
        result_key="res_def",
    )
    v.validate(record.to_wire())


@pytest.mark.parametrize("schema,example", [
    ("enroll-request.schema.json", "enroll-request.valid.json"),
    ("enroll-response.schema.json", "enroll-response.valid.json"),
    ("heartbeat-request.schema.json", "heartbeat-request.valid.json"),
    ("heartbeat-response.schema.json", "heartbeat-response.valid.json"),
    ("update-manifest.schema.json", "update-manifest.valid.json"),
])
def test_registry_schemas_accept_valid(contract_dir, schema, example):
    _validator(contract_dir, schema).validate(load_json(contract_dir / "examples" / example))


def test_pydantic_enroll_matches_schema(contract_dir):
    v = _validator(contract_dir, "enroll-request.schema.json")
    req = EnrollRequest(
        join_token="jt_x", hostname="h", os="darwin", arch="arm64",
        hw=HwProbe(ram_gb=64, accelerator="metal", disk_free_gb=512), profile="shared",
    )
    v.validate(req.model_dump(mode="json", exclude_none=True))


def test_pydantic_heartbeat_matches_schema(contract_dir):
    v = _validator(contract_dir, "heartbeat-request.schema.json")
    hb = HeartbeatRequest(
        mode="away", installed=["m"], loaded=["m"], queues=["q:8b-extract"],
        stats={"jobs_done": 1, "tps": 10.0}, protocol_version=1,
    )
    v.validate(hb.model_dump(mode="json", exclude_none=True))


def test_coordinator_written_result_conforms_without_inference_timestamps(contract_dir):
    """Not every terminal result comes from an executor.

    The reaper's dead-letter, the expiry sweep and a cancellation are all written by the
    coordinator for a job that never reached a model server — so `started_at`/
    `finished_at` have no honest value and must stay optional. Pinned because making them
    required (an easy, plausible tightening) silently invalidates every one of those
    paths, and nothing else in the suite writes such a blob.
    """
    v = _validator(contract_dir, "result.schema.json")
    v.validate({
        "job_id": "job_1", "status": "failed", "worker": "cbk-reaper",
        "completed_at": "2026-09-02T00:00:00Z",
        "error": "abandoned by its worker and retried 3 times (max_attempts=3)",
    })


def test_an_executor_result_carries_both_inference_timestamps(contract_dir):
    """The other half: when an executor does measure, both ends must be there — one
    without the other cannot express a duration."""
    v = _validator(contract_dir, "result.schema.json")
    v.validate({
        "job_id": "job_1", "status": "done", "worker": "node-a",
        "started_at": "2026-09-02T00:00:00Z",
        "finished_at": "2026-09-02T00:00:07Z",
        "completed_at": "2026-09-02T00:00:00Z",
        "completion": {"choices": [
            {"index": 0, "message": {"role": "assistant", "content": "hi"}}]},
    })
