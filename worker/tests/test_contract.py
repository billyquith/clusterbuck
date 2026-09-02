"""Contract conformance (ADR 22).

`contract/*.schema.json` is the source of truth. This suite asserts two directions:

  * every record this worker PRODUCES validates against the shared schema, and
  * every committed fixture PARSES into this worker's types without losing a field.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from cbk_worker.config import AGENT_FLAVOUR, AGENT_VERSION, PROTOCOL_VERSION
from cbk_worker.models import (
    ActionResult,
    EnrollRequest,
    HeartbeatRequest,
    HwProbe,
    Job,
    NodeState,
    Result,
    UpdateManifest,
)

CONTRACT = Path(__file__).resolve().parents[2] / "contract"
EXAMPLES = CONTRACT / "examples"


def _validator(name: str) -> Draft202012Validator:
    schema = json.loads((CONTRACT / f"{name}.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _assert_valid(name: str, instance: dict) -> None:
    errors = sorted(_validator(name).iter_errors(instance), key=lambda e: e.path)
    assert not errors, f"{name}: " + "; ".join(
        f"{list(e.path)}: {e.message}" for e in errors)


def test_contract_directory_is_where_we_think_it_is():
    """Guards the relative path: a silently-missing contract dir would make every
    conformance assertion below vacuously pass."""
    assert (CONTRACT / "job.schema.json").is_file(), f"no contract at {CONTRACT}"


# --- records we produce -----------------------------------------------------------------


def test_done_result_conforms():
    r = Result(job_id="job_1", status="done", worker="node-a",
               started_at="2026-07-28T10:00:00Z",
               finished_at="2026-07-28T10:00:07Z",
               completion={"id": "c1", "choices": [
                   {"index": 0, "message": {"role": "assistant", "content": "hi"}}]},
               usage={"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4})
    wire = r.to_wire()
    _assert_valid("result", wire)
    # Absent, not null: a "done" result carrying "error": null would be a lie the schema
    # happens to permit.
    assert "error" not in wire


def test_result_timestamps_bracket_the_model_call():
    """The bug this replaced: one timestamp, taken BEFORE inference and written as
    `completed_at`, so the only "completion" time in the system was a start time wrong by
    a full inference duration."""
    wire = Result(job_id="job_1", status="done", worker="node-a",
                  started_at="2026-07-28T10:00:00Z",
                  finished_at="2026-07-28T10:00:07Z",
                  completion={"id": "c1", "choices": [
                      {"index": 0, "message": {"role": "assistant", "content": "hi"}}]},
                  ).to_wire()
    assert wire["started_at"] < wire["finished_at"]
    # The deprecated alias must keep meaning exactly what it always did — a start time —
    # so a reader that has not migrated sees no change in its numbers.
    assert wire["completed_at"] == wire["started_at"]


def test_failed_result_conforms_and_carries_no_completion():
    wire = Result(job_id="job_1", status="failed", worker="node-a",
                  started_at="2026-07-28T10:00:00Z",
                  finished_at="2026-07-28T10:00:01Z",
                  error="model server refused connection").to_wire()
    _assert_valid("result", wire)
    assert "completion" not in wire and "usage" not in wire


def test_enroll_request_conforms():
    wire = EnrollRequest(
        join_token="jt_abc", hostname="node-x", os="linux", arch="arm64",
        hw=HwProbe(ram_gb=16.0, accelerator="cpu", disk_free_gb=120.5),
        profile="shared").to_wire()
    _assert_valid("enroll-request", wire)


def test_heartbeat_request_conforms_and_declares_this_flavour():
    wire = HeartbeatRequest(
        mode="away", installed=["llama3.2:3b"], loaded=["llama3.2:3b"],
        digests={"llama3.2:3b": "sha256:" + "a" * 64},
        queues=["q:8b-extract"], stats={"jobs_done": 2},
        protocol_version=PROTOCOL_VERSION, agent_version=AGENT_VERSION,
        agent_flavour=AGENT_FLAVOUR,
        action_result=ActionResult(proposal_id="prop_1", ok=True)).to_wire()
    _assert_valid("heartbeat-request", wire)
    # The field that stops the coordinator handing us an incompatible binary.
    assert wire["agent_flavour"] == "python"


@pytest.mark.parametrize("mode", ["active", "away", "paused"])
def test_every_presence_mode_is_a_valid_heartbeat(mode):
    _assert_valid("heartbeat-request", HeartbeatRequest(mode=mode).to_wire())


# --- records we consume -----------------------------------------------------------------


def test_committed_job_fixture_parses_without_loss():
    raw = json.loads((EXAMPLES / "job.valid.json").read_text())
    job = Job.from_wire(raw)
    assert job.id == raw["id"]
    assert job.result_key == raw["result_key"]
    assert job.capability == raw["capability"]
    # params must survive verbatim: the eval harness pins the artifact to run in there, and
    # a dropped key would silently score the wrong model.
    assert job.params == (raw.get("params") or {})


def test_committed_enroll_response_shapes_node_state():
    raw = json.loads((EXAMPLES / "enroll-response.valid.json").read_text())
    proposed = raw["proposed"]
    state = NodeState(node_id=raw["node_id"], node_key=raw["node_key"],
                      server="http://coordinator:8000",
                      capabilities=proposed["capabilities"],
                      ladder=proposed.get("ladder"))
    # Round-trips through the on-disk form the worker persists.
    assert NodeState.from_wire(state.to_wire()) == state


def test_committed_heartbeat_response_fixture_is_understood():
    raw = json.loads((EXAMPLES / "heartbeat-response.valid.json").read_text())
    _assert_valid("heartbeat-response", raw)
    if raw.get("update"):
        m = UpdateManifest.from_wire(raw["update"])
        assert m.version and m.sha256 and m.signature


def test_committed_manifest_fixture_parses():
    m = UpdateManifest.from_wire(
        json.loads((EXAMPLES / "update-manifest.valid.json").read_text()))
    assert m.version and len(m.sha256) == 64 and m.signature
