"""Python-side contract conformance (ADR 22).

Asserts the shared JSON Schema in contract/ accepts the valid fixtures and rejects the
invalid ones, and that the server's Pydantic models round-trip into schema-valid wire
shapes. The C# worker runs the mirror of this against the same files, so the two type
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
