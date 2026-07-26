"""The eval harness as a fleet client (M7): dispatch as jobs, score results, record ability."""

from __future__ import annotations

import json

import pytest

from clusterbuck.eval_runner import (
    EVAL_CLIENT_KEY,
    artifacts_needing_eval,
    collect,
    dispatch,
    eval_tick,
)
from clusterbuck.evaluation import SCALE_VERSION, EvalItem, check_json_valid
from clusterbuck.models import EnrollRequest, HwProbe
from clusterbuck.queue import Queue, stream_key
from clusterbuck.store import Store

ARTIFACT = "newmodel:7b"
CAP = "8b-extract"

# A deterministic two-item suite: one JSON item, one keyword item, distinct task classes.
SUITE = [
    EvalItem("extract", 'Return JSON {"n": 1}.', check_json_valid),
    EvalItem("summarize", "Summarize the fox text.", lambda o: "fox" in (o or "").lower()),
]


@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(str(tmp_path / "eval.db"))


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


def _enroll_with(store: Store, artifacts: list[str], caps: list[str] = [CAP]) -> str:
    req = EnrollRequest(
        join_token="jt", hostname="h", os="linux", arch="arm64",
        hw=HwProbe(ram_gb=32, accelerator="cpu", disk_free_gb=100), profile="shared",
    )
    store.enroll_node(node_id="node-e", node_key="k", req=req,
                      capabilities=json.dumps(caps), enrolled_at="t")
    store.record_heartbeat(node_id="node-e", mode="active", installed=json.dumps(artifacts),
                           loaded="[]", queues="[]", jobs_done=None, tps=None,
                           last_heartbeat="t")
    return "node-e"


async def _finish(queue: Queue, store: Store, *, text_for) -> None:
    """Write worker-style results for every pending eval run."""
    for run in store.pending_eval_runs():
        await queue.client.set(run["result_key"], json.dumps({
            "job_id": run["job_id"], "status": "done", "worker": "node-e",
            "completed_at": "t",
            "completion": {"model": run["artifact"], "choices": [
                {"message": {"role": "assistant", "content": text_for(run)}}]},
        }))


# --- candidate selection ---

def test_only_installed_artifacts_are_candidates(store):
    assert artifacts_needing_eval(store, suite=SUITE) == []  # no nodes yet
    _enroll_with(store, [ARTIFACT])
    assert artifacts_needing_eval(store, suite=SUITE) == [(ARTIFACT, [CAP])]


def test_measured_artifacts_are_skipped(store):
    _enroll_with(store, [ARTIFACT])
    for tc in ("extract", "summarize"):
        store.set_ability(artifact=ARTIFACT, task_class=tc, score=6.0,
                          scale_version=SCALE_VERSION, updated_at="t")
    assert artifacts_needing_eval(store, suite=SUITE) == []


async def test_in_flight_artifacts_are_not_redispatched(store, queue):
    _enroll_with(store, [ARTIFACT])
    assert await dispatch(store, queue, now="t", suite=SUITE) == 2
    assert artifacts_needing_eval(store, suite=SUITE) == []   # already under eval
    assert await dispatch(store, queue, now="t", suite=SUITE) == 0  # no duplicate storm


# --- dispatch shape ---

async def test_dispatched_jobs_pin_the_artifact_and_stay_polite(store, queue):
    _enroll_with(store, [ARTIFACT])
    assert await dispatch(store, queue, now="t", suite=SUITE) == 2

    entries = await queue.client.xrange(stream_key(CAP))
    assert len(entries) == 2
    for _id, fields in entries:
        job = json.loads(fields["job"])
        # The artifact under test is pinned, else we'd measure the worker's default model.
        assert job["params"]["model"] == ARTIFACT
        assert job["urgency"] == "waitable"       # never wakes a machine for an eval
        assert job["privacy"] == "local_only"     # eval material stays on the LAN

    # Jobs are attributed to the harness client, and carry their task class.
    rows = store.pending_eval_runs()
    assert {r["task_class"] for r in rows} == {"extract", "summarize"}
    assert all(store.get(r["job_id"])["client_key"] == EVAL_CLIENT_KEY for r in rows)


# --- collect + scoring ---

async def test_all_pass_records_top_ability(store, queue):
    _enroll_with(store, [ARTIFACT])
    await dispatch(store, queue, now="t", suite=SUITE)
    await _finish(queue, store, text_for=lambda r: '{"n": 1}' if r["task_class"] == "extract"
                  else "a fox jumped")

    assert await collect(store, queue, now="t", suite=SUITE) == 2
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) == 10.0
    assert store.get_ability(ARTIFACT, "summarize", SCALE_VERSION) == 10.0


async def test_all_fail_records_floor_ability(store, queue):
    _enroll_with(store, [ARTIFACT])
    await dispatch(store, queue, now="t", suite=SUITE)
    await _finish(queue, store, text_for=lambda r: "unhelpful garbage")

    await collect(store, queue, now="t", suite=SUITE)
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) == 1.0
    assert store.get_ability(ARTIFACT, "summarize", SCALE_VERSION) == 1.0


async def test_unfinished_batch_records_nothing(store, queue):
    _enroll_with(store, [ARTIFACT])
    await dispatch(store, queue, now="t", suite=SUITE)
    # Only the extract item comes back.
    run = next(r for r in store.pending_eval_runs() if r["task_class"] == "extract")
    await queue.client.set(run["result_key"], json.dumps({
        "job_id": run["job_id"], "status": "done", "worker": "w", "completed_at": "t",
        "completion": {"choices": [{"message": {"role": "a", "content": '{"n":1}'}}]},
    }))

    assert await collect(store, queue, now="t", suite=SUITE) == 1
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) == 10.0   # its batch settled
    assert store.get_ability(ARTIFACT, "summarize", SCALE_VERSION) is None  # still waiting


async def test_failed_job_yields_no_score(store, queue):
    _enroll_with(store, [ARTIFACT])
    await dispatch(store, queue, now="t", suite=SUITE)
    for run in store.pending_eval_runs():
        await queue.client.set(run["result_key"], json.dumps({
            "job_id": run["job_id"], "status": "failed", "worker": "w",
            "completed_at": "t", "error": "model server down",
        }))

    assert await collect(store, queue, now="t", suite=SUITE) == 0
    # A broken run must not be scored as a failure of the model.
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) is None


# --- the closed loop ---

async def test_tick_measures_a_new_artifact_end_to_end(store, queue):
    """An artifact appears installed with no score; two ticks later it has one."""
    _enroll_with(store, [ARTIFACT])
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) is None

    collected, dispatched = await eval_tick(store, queue, now="t", suite=SUITE)
    assert (collected, dispatched) == (0, 2)

    await _finish(queue, store, text_for=lambda r: '{"n": 1}' if r["task_class"] == "extract"
                  else "the fox")
    collected, dispatched = await eval_tick(store, queue, now="t", suite=SUITE)
    assert collected == 2
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) == 10.0
    # Measured now, so nothing further is queued for it.
    assert dispatched == 0


# --- endpoints ---

def test_eval_endpoint_shape(client):
    data = client.get("/eval").json()
    assert data["scale_version"] == SCALE_VERSION
    assert data["needs_eval"] == []   # no enrolled nodes in this fixture
    assert data["batches"] == []


def test_eval_run_endpoint_triggers_a_pass(client):
    body = client.post("/eval/run").json()
    assert body == {"scored": 0, "dispatched": 0}   # nothing installed to measure
