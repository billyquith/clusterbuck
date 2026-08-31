"""Running the tier-1 eval suite as ordinary fleet jobs (model-evaluation.md).

The design is explicit: "evals run as ordinary jobs — low-priority `waitable` jobs on the
fleet itself; the harness is just another client." So this module does not call model
servers directly. It **submits jobs** and later **scores their results**, which means evals
respect capacity, presence modes, urgency and the owner's machine exactly like real work,
and they reach remote nodes for free.

This closes the M6 loop: an install (or an upstream digest change) leaves an artifact with
no ability score, `dispatch` measures it, and `collect` records the score — after which the
need-shaped router (ADR 16) will actually select it. Nothing inherits a score it didn't earn.

Two things worth knowing:

* **The artifact is pinned per job** via `params.model`, which the worker forwards to its
  model server, overriding the worker's default model. Without that, an eval job would
  measure whatever the worker happens to be configured with rather than the artifact under
  test — the score would be attributed to the wrong artifact.
* **Item identity is positional** (`item_index` into the suite). Reordering the suite while
  runs are in flight would mis-score them; a suite version is the proper fix when the suite
  starts changing (model-evaluation.md versions suites alongside the scale).
"""

from __future__ import annotations

import logging

from .evaluation import SCALE_VERSION, SEED_SUITE, EvalItem, score_to_ability
from .ids import new_ids
from .models import JobRecord, Message, Privacy, Urgency
from .queue import Queue
from .store import Store

_log = logging.getLogger("clusterbuck.eval")

EVAL_CLIENT_KEY = "cbk:eval"
# Evals are cheap but not free; measure a couple of artifacts at a time rather than
# flooding the queues the moment several models appear.
MAX_ARTIFACTS_IN_FLIGHT = 2
# After this many runs for one (artifact, task_class) have produced no signal, stop
# re-dispatching. Without a cap, a batch that always fails (e.g. the pinned artifact isn't on
# the node that drained the job) is re-enqueued every tick forever.
MAX_FAILED_RUNS = 6
# A score is recorded only if a STRICT majority of the batch produced signal, so one
# infrastructure failure cannot promote a model on a shrunken sample. Exactly half is not
# enough: 1-of-2 surviving is precisely the case that recorded a perfect 10.0 from a single
# item, so the comparison is `>` this fraction, not `>=`.
MIN_SIGNAL_FRACTION = 0.5


def _suite_classes(suite: list[EvalItem]) -> set[str]:
    return {item.task_class for item in suite}


def artifacts_needing_eval(
    store: Store, *, suite: list[EvalItem] = SEED_SUITE, scale_version: str = SCALE_VERSION,
) -> list[tuple[str, list[str]]]:
    """(artifact, capabilities) for installed artifacts missing any tested task class.

    Only artifacts actually observed on a node are candidates — we can only measure what
    some worker can serve.
    """
    classes = _suite_classes(suite)
    busy = store.artifacts_under_eval()
    seen: dict[str, list[str]] = {}
    for _node_id, artifact, caps in store.installed_artifacts():
        if artifact in busy or artifact in seen or not caps:
            continue
        missing = [
            tc for tc in sorted(classes)
            # Unscored, or scored only by a seeded placeholder — a seed is not a measurement.
            if (store.get_ability(artifact, tc, scale_version) is None
                or store.ability_provenance(artifact, tc, scale_version) == "seed")
            # …but give up on a task class whose runs keep yielding nothing.
            and store.failed_eval_runs(artifact, tc) < MAX_FAILED_RUNS
        ]
        if missing:
            seen[artifact] = caps
    return list(seen.items())


async def dispatch(
    store: Store, queue: Queue, *, now: str, suite: list[EvalItem] = SEED_SUITE,
    scale_version: str = SCALE_VERSION,
) -> int:
    """Submit eval jobs for unmeasured installed artifacts. Returns jobs enqueued."""
    enqueued = 0
    candidates = artifacts_needing_eval(store, suite=suite, scale_version=scale_version)
    for artifact, caps in candidates[:MAX_ARTIFACTS_IN_FLIGHT]:
        capability = caps[0]  # any capability this node serves reaches its model server
        for index, item in enumerate(suite):
            measured = (
                store.get_ability(artifact, item.task_class, scale_version) is not None
                and store.ability_provenance(artifact, item.task_class, scale_version)
                != "seed"
            )
            if measured:
                continue  # genuinely measured already (a seed is not a measurement)
            job_id, result_key = new_ids()
            record = JobRecord(
                id=job_id, created_at=now, capability=capability,
                messages=[Message(role="user", content=item.prompt)],
                # Pin the artifact under test; the worker forwards this to its model server.
                params={"model": artifact, "temperature": 0.0, "max_tokens": 256},
                urgency=Urgency.waitable,          # never wakes a machine for an eval
                privacy=Privacy.local_only,        # eval material stays on the LAN
                result_key=result_key,
            )
            store.insert(
                id=job_id, result_key=result_key, capability=capability, created_at=now,
                urgency=Urgency.waitable.value, client_key=EVAL_CLIENT_KEY,
                task_class=item.task_class,
            )
            await queue.enqueue(record.to_wire())
            store.add_eval_run(job_id=job_id, artifact=artifact,
                               task_class=item.task_class, item_index=index,
                               result_key=result_key, created_at=now)
            enqueued += 1
        if enqueued:
            _log.info("eval dispatched for %s via %s", artifact, capability)
    return enqueued


def _completion_text(result: dict) -> str | None:
    completion = result.get("completion")
    if not isinstance(completion, dict):
        return None
    choices = completion.get("choices") or []
    if not choices:
        return None
    return (choices[0].get("message") or {}).get("content")


async def collect(
    store: Store, queue: Queue, *, now: str, suite: list[EvalItem] = SEED_SUITE,
    scale_version: str = SCALE_VERSION,
) -> int:
    """Score finished eval jobs and record ability for completed batches.

    Returns the number of runs scored this pass.
    """
    scored = 0
    touched: set[tuple[str, str]] = set()

    for run in store.pending_eval_runs():
        result = await queue.read_result(run.result_key)
        if result is None:
            continue  # still queued or running
        if result.get("status") != "done":
            store.fail_eval_run(run.job_id)  # no signal from a failed/expired job
            touched.add((run.artifact, run.task_class))
            continue
        text = _completion_text(result)
        index = run.item_index
        if text is None or index >= len(suite):
            store.fail_eval_run(run.job_id)
            touched.add((run.artifact, run.task_class))
            continue
        store.score_eval_run(run.job_id, bool(suite[index].check(text)))
        scored += 1
        touched.add((run.artifact, run.task_class))

    # A batch whose items have all settled yields an ability score — but only if enough of
    # them actually produced signal.
    for artifact, task_class in touched:
        pending, done, passed, failed = store.eval_progress(artifact, task_class)
        if pending:
            continue  # still in flight
        total = done + failed
        if not done or total == 0:
            # Every item failed: infrastructure, not the model. Record nothing; the
            # MAX_FAILED_RUNS cap stops this being retried forever.
            _log.warning("eval inconclusive: %s/%s — all %d item(s) failed, no score recorded",
                         artifact, task_class, failed)
            continue
        if done / total <= MIN_SIGNAL_FRACTION:
            _log.warning(
                "eval inconclusive: %s/%s — only %d of %d items produced signal, "
                "refusing to score on a shrunken sample", artifact, task_class, done, total)
            continue
        ability = score_to_ability(passed / done)
        store.set_ability(artifact=artifact, task_class=task_class, score=ability,
                          scale_version=scale_version, updated_at=now,
                          provenance="measured")
        _log.info("eval complete: %s/%s → ability %.1f (%d/%d passed, %d no-signal)",
                  artifact, task_class, ability, passed, done, failed)

    return scored


async def eval_tick(
    store: Store, queue: Queue, *, now: str, suite: list[EvalItem] = SEED_SUITE,
    scale_version: str = SCALE_VERSION,
) -> tuple[int, int]:
    """One coordinator pass: collect finished work first, then dispatch new work."""
    collected = await collect(store, queue, now=now, suite=suite, scale_version=scale_version)
    enqueued = await dispatch(store, queue, now=now, suite=suite, scale_version=scale_version)
    return collected, enqueued
