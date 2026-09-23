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
from clusterbuck.evaluation import (
    SCALE_VERSION,
    TIER1_MAX_ABILITY,
    EvalItem,
    check_json_valid,
)
from clusterbuck.fleet import CapabilitySpec, Fleet
from clusterbuck.models import EnrollRequest, HwProbe, Privacy
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


def _enroll_with(store: Store, artifacts: list[str],
                 caps: list[str] | None = None) -> str:
    # `None` rather than `[CAP]` as the default: a mutable default is shared across every
    # call, so one test appending to it would silently change what the next test enrolls.
    caps = [CAP] if caps is None else caps
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
        await queue.client.set(run.result_key, json.dumps({
            "job_id": run.job_id, "status": "done", "worker": "node-e",
            "completed_at": "t",
            "completion": {"model": run.artifact, "choices": [
                {"message": {"role": "assistant", "content": text_for(run)}}]},
        }))


# --- candidate selection ---

def test_only_installed_artifacts_are_candidates(store):
    assert artifacts_needing_eval(store, suite=SUITE, min_items=1) == []  # no nodes yet
    _enroll_with(store, [ARTIFACT])
    assert artifacts_needing_eval(store, suite=SUITE, min_items=1) == [(ARTIFACT, [CAP])]


def test_measured_artifacts_are_skipped(store):
    _enroll_with(store, [ARTIFACT])
    for tc in ("extract", "summarize"):
        store.set_ability(artifact=ARTIFACT, task_class=tc, score=6.0,
                          scale_version=SCALE_VERSION, updated_at="t")
    assert artifacts_needing_eval(store, suite=SUITE, min_items=1) == []


async def test_in_flight_artifacts_are_not_redispatched(store, queue):
    _enroll_with(store, [ARTIFACT])
    assert await dispatch(store, queue, now="t", suite=SUITE, min_items=1) == 2
    assert artifacts_needing_eval(store, suite=SUITE, min_items=1) == []   # already under eval
    # No duplicate storm.
    assert await dispatch(store, queue, now="t", suite=SUITE, min_items=1) == 0


# --- dispatch shape ---

async def test_dispatched_jobs_pin_the_artifact_and_stay_polite(store, queue):
    _enroll_with(store, [ARTIFACT])
    assert await dispatch(store, queue, now="t", suite=SUITE, min_items=1) == 2

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
    assert {r.task_class for r in rows} == {"extract", "summarize"}
    assert all(store.get(r.job_id).client_key == EVAL_CLIENT_KEY for r in rows)


# --- collect + scoring ---

async def test_all_pass_records_top_ability(store, queue):
    _enroll_with(store, [ARTIFACT])
    await dispatch(store, queue, now="t", suite=SUITE, min_items=1)
    await _finish(queue, store, text_for=lambda r: '{"n": 1}' if r.task_class == "extract"
                  else "a fox jumped")

    assert await collect(store, queue, now="t", suite=SUITE, min_items=1) == 2
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) == TIER1_MAX_ABILITY
    assert store.get_ability(ARTIFACT, "summarize", SCALE_VERSION) == TIER1_MAX_ABILITY


async def test_all_fail_records_floor_ability(store, queue):
    _enroll_with(store, [ARTIFACT])
    await dispatch(store, queue, now="t", suite=SUITE, min_items=1)
    await _finish(queue, store, text_for=lambda r: "unhelpful garbage")

    await collect(store, queue, now="t", suite=SUITE, min_items=1)
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) == 1.0
    assert store.get_ability(ARTIFACT, "summarize", SCALE_VERSION) == 1.0


async def test_unfinished_batch_records_nothing(store, queue):
    _enroll_with(store, [ARTIFACT])
    await dispatch(store, queue, now="t", suite=SUITE, min_items=1)
    # Only the extract item comes back.
    run = next(r for r in store.pending_eval_runs() if r.task_class == "extract")
    await queue.client.set(run.result_key, json.dumps({
        "job_id": run.job_id, "status": "done", "worker": "w", "completed_at": "t",
        "completion": {"choices": [{"message": {"role": "a", "content": '{"n":1}'}}]},
    }))

    assert await collect(store, queue, now="t", suite=SUITE, min_items=1) == 1
    # Its batch settled.
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) == TIER1_MAX_ABILITY
    assert store.get_ability(ARTIFACT, "summarize", SCALE_VERSION) is None  # still waiting


async def test_failed_job_yields_no_score(store, queue):
    _enroll_with(store, [ARTIFACT])
    await dispatch(store, queue, now="t", suite=SUITE, min_items=1)
    for run in store.pending_eval_runs():
        await queue.client.set(run.result_key, json.dumps({
            "job_id": run.job_id, "status": "failed", "worker": "w",
            "completed_at": "t", "error": "model server down",
        }))

    assert await collect(store, queue, now="t", suite=SUITE, min_items=1) == 0
    # A broken run must not be scored as a failure of the model.
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) is None


# --- the closed loop ---

async def test_tick_measures_a_new_artifact_end_to_end(store, queue):
    """An artifact appears installed with no score; two ticks later it has one."""
    _enroll_with(store, [ARTIFACT])
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) is None

    collected, dispatched = await eval_tick(store, queue, now="t", suite=SUITE, min_items=1)
    assert (collected, dispatched) == (0, 2)

    await _finish(queue, store, text_for=lambda r: '{"n": 1}' if r.task_class == "extract"
                  else "the fox")
    collected, dispatched = await eval_tick(store, queue, now="t", suite=SUITE, min_items=1)
    assert collected == 2
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) == TIER1_MAX_ABILITY
    # Measured now, so nothing further is queued for it.
    assert dispatched == 0


# --- cloud provider artifacts (ADR 30) enter the same measurement path ---

CLOUD_ARTIFACT = "anthropic/claude-3-5-sonnet-20241022"
CLOUD_CAP = "claude-sonnet"


def _cloud_fleet(monkeypatch, *, keyed: bool = True) -> Fleet:
    if keyed:
        monkeypatch.setenv("CBK_TEST_PROVIDER_KEY", "sk-test-123")
    return Fleet(capabilities={
        CLOUD_CAP: CapabilitySpec(queue=f"q:{CLOUD_CAP}", model=CLOUD_ARTIFACT, cloud=True,
                                  api_key_env="CBK_TEST_PROVIDER_KEY"),
    })


def test_unkeyed_cloud_artifacts_are_not_candidates(store, monkeypatch):
    """No point spending eval budget on a provider whose key isn't configured — it can
    only ever fail, the same reason sync.build_router excludes it from the sync plane."""
    monkeypatch.delenv("CBK_TEST_PROVIDER_KEY", raising=False)
    fleet = _cloud_fleet(monkeypatch, keyed=False)
    assert artifacts_needing_eval(store, fleet=fleet, suite=SUITE, min_items=1) == []


def test_keyed_cloud_artifact_is_a_candidate(store, monkeypatch):
    fleet = _cloud_fleet(monkeypatch)
    assert artifacts_needing_eval(store, fleet=fleet, suite=SUITE, min_items=1) == [
        (CLOUD_ARTIFACT, [CLOUD_CAP])]


async def test_cloud_artifact_dispatch_uses_cloud_ok_privacy(store, queue, monkeypatch):
    """A cloud artifact can only be measured by actually calling the provider, so its eval
    jobs must carry cloud_ok — local_only would just have them refused (ADR 14)."""
    fleet = _cloud_fleet(monkeypatch)
    assert await dispatch(store, queue, now="t", fleet=fleet, suite=SUITE, min_items=1) == 2

    entries = await queue.client.xrange(stream_key(CLOUD_CAP))
    assert len(entries) == 2
    for _id, fields in entries:
        job = json.loads(fields["job"])
        assert job["params"]["model"] == CLOUD_ARTIFACT
        assert job["privacy"] == Privacy.cloud_ok.value
        assert job["urgency"] == "waitable"


async def test_cloud_artifact_scored_end_to_end(store, queue, monkeypatch):
    fleet = _cloud_fleet(monkeypatch)
    collected, dispatched = await eval_tick(
        store, queue, now="t", fleet=fleet, suite=SUITE, min_items=1)
    assert (collected, dispatched) == (0, 2)

    await _finish(queue, store, text_for=lambda r: '{"n": 1}' if r.task_class == "extract"
                  else "the fox")
    collected, dispatched = await eval_tick(
        store, queue, now="t", fleet=fleet, suite=SUITE, min_items=1)
    assert collected == 2
    assert store.get_ability(CLOUD_ARTIFACT, "extract", SCALE_VERSION) == TIER1_MAX_ABILITY


# --- endpoints ---

def test_eval_endpoint_shape(client):
    data = client.get("/eval").json()
    assert data["scale_version"] == SCALE_VERSION
    assert data["needs_eval"] == []   # no enrolled nodes in this fixture
    assert data["batches"] == []


def test_eval_run_endpoint_triggers_a_pass(client):
    body = client.post("/eval/run").json()
    assert body == {"scored": 0, "dispatched": 0}   # nothing installed to measure


# --- regressions: bugs an audit proved live, each previously passing three green tests ---

async def test_mixed_batch_does_not_score_from_a_shrunken_sample(store, queue):
    """A batch where some items yielded no signal must not record a score from the rest.

    Previously eval_progress counted only pending/scored, so one infrastructure failure made
    a 2-item batch look complete and recorded ability 10.0 from the single survivor.
    """
    _enroll_with(store, [ARTIFACT])
    single = [SUITE[0], SUITE[0]]  # two items, one task class
    await dispatch(store, queue, now="t", suite=single, min_items=1)
    runs = store.pending_eval_runs()
    assert len(runs) == 2

    # One passes, one fails outright (job error, not a wrong answer).
    await queue.client.set(runs[0].result_key, json.dumps({
        "job_id": runs[0].job_id, "status": "done", "worker": "w", "completed_at": "t",
        "completion": {"choices": [{"message": {"role": "a", "content": '{"n":1}'}}]}}))
    await queue.client.set(runs[1].result_key, json.dumps({
        "job_id": runs[1].job_id, "status": "failed", "worker": "w",
        "completed_at": "t", "error": "model server down"}))

    await collect(store, queue, now="t", suite=single, min_items=1)
    pending, scored, _passed, failed = store.eval_progress(ARTIFACT, "extract")
    assert (pending, scored, failed) == (0, 1, 1)
    # 1 of 2 items produced signal — below the majority threshold, so no score.
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) is None


async def test_all_failed_batch_stops_redispatching(store, queue):
    """An always-failing batch previously re-enqueued the whole suite every tick forever."""
    _enroll_with(store, [ARTIFACT])
    single = [SUITE[0]]

    for _ in range(10):
        await dispatch(store, queue, now="t", suite=single, min_items=1)
        for run in store.pending_eval_runs():
            await queue.client.set(run.result_key, json.dumps({
                "job_id": run.job_id, "status": "failed", "worker": "w",
                "completed_at": "t", "error": "always broken"}))
        await collect(store, queue, now="t", suite=single, min_items=1)

    # No score was invented from zero signal…
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) is None
    # …and the retry loop is bounded rather than infinite.
    assert store.failed_eval_runs(ARTIFACT, "extract") <= 8
    assert artifacts_needing_eval(store, suite=single) == []


async def test_seeded_artifacts_are_still_measured(store, queue):
    """seed_ability previously exempted the shipped fleet from evaluation forever."""
    from clusterbuck.evaluation import seed_ability

    seed_ability(store, now="t")
    _enroll_with(store, ["llama3.2:3b"])          # a seeded artifact, installed on a node

    # It has a score, but only a seeded one — so it must still be queued for measurement.
    assert store.get_ability("llama3.2:3b", "extract", SCALE_VERSION) is not None
    assert [a for a, _ in artifacts_needing_eval(store, suite=SUITE, min_items=1)] == [
        "llama3.2:3b"]
    assert await dispatch(store, queue, now="t", suite=SUITE, min_items=1) == 2

    # A real measurement replaces the seed and is labelled as earned.
    await _finish(queue, store, text_for=lambda r: '{"n": 1}' if r.task_class == "extract"
                  else "a fox")
    await collect(store, queue, now="t", suite=SUITE, min_items=1)
    assert store.ability_provenance("llama3.2:3b", "extract", SCALE_VERSION) == "measured"
    assert artifacts_needing_eval(store, suite=SUITE, min_items=1) == []


async def test_a_superseded_artifact_is_scored_only_on_its_own_measurements(store, queue):
    """ADR 15: a changed digest is a NEW artifact and inherits nothing — including the
    measurements the score is computed from.

    Dropping the ability row was not enough. Ability is recomputed from every eval_run
    recorded for the (artifact, task_class), so the re-eval averaged the new artifact's
    items together with the old artifact's: a model that now passed 2 of 2 was recorded on
    3 of 4 across both rounds rather than on its own 2 of 2, carrying the superseded
    measurement forward under a new digest.
    """
    _enroll_with(store, [ARTIFACT])
    single = [SUITE[0], SUITE[0]]  # two items, one task class

    async def run_batch(*answers: str) -> None:
        await dispatch(store, queue, now="t", suite=single, min_items=1)
        for run, answer in zip(store.pending_eval_runs(), answers, strict=True):
            await queue.client.set(run.result_key, json.dumps({
                "job_id": run.job_id, "status": "done", "worker": "w",
                "completed_at": "t",
                "completion": {"choices": [{"message": {"role": "a", "content": answer}}]}}))
        await collect(store, queue, now="t", suite=single, min_items=1)

    # Round 1: the old artifact gets one of two right.
    await run_batch('{"n":1}', "not json")
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) == 4.0

    # The artifact changes upstream.
    cleared, generation = store.supersede_artifact(ARTIFACT, SCALE_VERSION, now="t2")
    assert (cleared, generation) == (1, 2)
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) is None

    # Round 2: the new artifact gets both right, and is scored on those two items ALONE.
    await run_batch('{"n":1}', '{"n":2}')
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) == TIER1_MAX_ABILITY
    assert store.eval_progress(ARTIFACT, "extract", generation) == (0, 2, 2, 0)


async def test_superseding_frees_an_artifact_that_exhausted_its_failure_budget(store, queue):
    """The MAX_FAILED_RUNS cap counted failures across all time, so an artifact that once
    failed its way to the cap could never be measured again — not even as a new artifact
    with a new digest. The budget is per measurement round."""
    _enroll_with(store, [ARTIFACT])
    single = [SUITE[0]]

    for _ in range(10):
        await dispatch(store, queue, now="t", suite=single, min_items=1)
        for run in store.pending_eval_runs():
            await queue.client.set(run.result_key, json.dumps({
                "job_id": run.job_id, "status": "failed", "worker": "w",
                "completed_at": "t", "error": "always broken"}))
        await collect(store, queue, now="t", suite=single, min_items=1)
    assert artifacts_needing_eval(store, suite=single, min_items=1) == []  # budget spent

    store.supersede_artifact(ARTIFACT, SCALE_VERSION, now="t2")
    assert [a for a, _ in
            artifacts_needing_eval(store, suite=single, min_items=1)] == [ARTIFACT]


async def test_superseding_retires_runs_still_in_flight(store, queue):
    """An in-flight run measures the artifact that no longer exists here. Left `pending` it
    would both contaminate the next score and keep the artifact permanently 'under eval',
    which is a state dispatch refuses to touch."""
    _enroll_with(store, [ARTIFACT])
    await dispatch(store, queue, now="t", suite=SUITE, min_items=1)
    assert len(store.pending_eval_runs()) == 2
    assert ARTIFACT in store.artifacts_under_eval()

    store.supersede_artifact(ARTIFACT, SCALE_VERSION, now="t2")
    assert store.pending_eval_runs() == []
    assert ARTIFACT not in store.artifacts_under_eval()
    assert await dispatch(store, queue, now="t2", suite=SUITE, min_items=1) == 2


# --- a reply cut off at the token cap is not a wrong answer ----------------------------

async def test_a_truncated_reply_is_no_signal_not_a_failure(store, queue):
    """Found live: the harness capped output at 256 tokens, and a reasoning model spent all
    of it thinking and returned empty content. That scored a 30B at 3.5 for extract against
    a 9B's 7.0 — the 9B only winning because its template suppressed thinking. The number
    measured the cap, not the model."""
    _enroll_with(store, [ARTIFACT])
    single = [SUITE[0], SUITE[0]]
    await dispatch(store, queue, now="t", suite=single, min_items=1)

    for run in store.pending_eval_runs():
        await queue.client.set(run.result_key, json.dumps({
            "job_id": run.job_id, "status": "done", "worker": "w", "completed_at": "t",
            "completion": {"choices": [{"finish_reason": "length",
                                        "message": {"role": "a", "content": ""}}]}}))
    await collect(store, queue, now="t", suite=single, min_items=1)

    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) is None, \
        "scored a model on replies it never got to finish"


async def test_a_complete_reply_is_still_scored(store, queue):
    """The guard must key on finish_reason, not on emptiness — a model that legitimately
    answers badly still earns its low score."""
    _enroll_with(store, [ARTIFACT])
    single = [SUITE[0], SUITE[0]]
    await dispatch(store, queue, now="t", suite=single, min_items=1)
    for run in store.pending_eval_runs():
        await queue.client.set(run.result_key, json.dumps({
            "job_id": run.job_id, "status": "done", "worker": "w", "completed_at": "t",
            "completion": {"choices": [{"finish_reason": "stop",
                                        "message": {"role": "a", "content": "not json"}}]}}))
    await collect(store, queue, now="t", suite=single, min_items=1)
    assert store.get_ability(ARTIFACT, "extract", SCALE_VERSION) == 1.0


def test_the_eval_budget_clears_a_reasoning_preamble():
    """256 was not enough for a single qwen3-class answer, measured on real hardware."""
    from clusterbuck.eval_runner import EVAL_MAX_TOKENS
    assert EVAL_MAX_TOKENS >= 1024
