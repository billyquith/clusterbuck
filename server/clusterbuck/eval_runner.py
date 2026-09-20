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

from .evaluation import (
    MIN_ITEMS_FOR_SCORE,
    SCALE_VERSION,
    SEED_SUITE,
    SUITE_VERSION,
    EvalItem,
    score_to_ability,
    suite_is_measurable,
)
from .fleet import Fleet, resolve_api_key
from .ids import new_ids
from .models import JobRecord, Message, Privacy, Urgency
from .queue import Queue, stream_key
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


def _suite_classes(suite: list[EvalItem], min_items: int = MIN_ITEMS_FOR_SCORE) -> set[str]:
    """Task classes this suite can produce an HONEST score for.

    A class the suite barely covers is excluded outright rather than measured and then
    discarded at collection: dispatching items whose result can never be recorded spends
    fleet capacity to learn nothing. `embed` used to sit in the opposite trap — scored by a
    seed, absent from the suite, so never measurable and never replaced.
    """
    return {item.task_class for item in suite
            if suite_is_measurable(item.task_class, suite, min_items)}


def _cloud_artifacts(fleet: Fleet | None) -> list[tuple[str, list[str]]]:
    """(artifact, [capability]) for every registered provider-account artifact (ADR 30).

    A cloud artifact has no node to be "installed" on — it IS a fleet.yaml capability
    (`cloud: true`, no `model_server`) — so it needs its own source alongside
    `store.installed_artifacts()` rather than trying to make that method cover both.

    Skips a provider whose API key isn't configured: there is no point spending eval
    budget dispatching a batch that can only ever fail (the cloud executor would refuse
    every job for the same reason `sync.build_router` excludes it from the sync plane).
    """
    if fleet is None:
        return []
    return [(spec.model, [name]) for name, spec in fleet.capabilities.items()
            if spec.cloud and spec.model_server is None and resolve_api_key(spec)]


def artifacts_needing_eval(
    store: Store, *, fleet: Fleet | None = None,
    suite: list[EvalItem] = SEED_SUITE, scale_version: str = SCALE_VERSION,
    min_items: int = MIN_ITEMS_FOR_SCORE,
) -> list[tuple[str, list[str]]]:
    """(artifact, capabilities) for artifacts missing any tested task class.

    Two sources: artifacts actually observed on a node (we can only measure what some
    worker can serve), and registered cloud provider accounts (ADR 30) — measured the same
    way, just dispatched to the coordinator's own cloud executor instead of a worker.
    """
    classes = _suite_classes(suite, min_items)
    busy = store.artifacts_under_eval()
    seen: dict[str, list[str]] = {}

    def consider(artifact: str, caps: list[str]) -> None:
        if artifact in busy or artifact in seen or not caps:
            return
        generation = store.current_eval_generation(artifact)
        missing = [
            tc for tc in sorted(classes)
            # Unscored, or scored only by a seeded placeholder — a seed is not a measurement.
            if (store.get_ability(artifact, tc, scale_version) is None
                or store.ability_provenance(artifact, tc, scale_version) == "seed")
            # …but give up on a task class whose runs keep yielding nothing.
            # Counted within THIS generation: a changed artifact deserves a fresh budget,
            # and counting every generation meant one bad batch barred it for good.
            and store.failed_eval_runs(artifact, tc, generation) < MAX_FAILED_RUNS
        ]
        if missing:
            seen[artifact] = caps

    for _node_id, artifact, caps in store.installed_artifacts():
        consider(artifact, caps)
    for artifact, caps in _cloud_artifacts(fleet):
        consider(artifact, caps)

    return list(seen.items())


async def dispatch(
    store: Store, queue: Queue, *, now: str, fleet: Fleet | None = None,
    suite: list[EvalItem] = SEED_SUITE, scale_version: str = SCALE_VERSION,
    min_items: int = MIN_ITEMS_FOR_SCORE,
) -> int:
    """Submit eval jobs for unmeasured artifacts (local or registered cloud). Returns jobs
    enqueued."""
    enqueued = 0
    candidates = artifacts_needing_eval(store, fleet=fleet, suite=suite,
                                        scale_version=scale_version, min_items=min_items)
    for artifact, caps in candidates[:MAX_ARTIFACTS_IN_FLIGHT]:
        capability = caps[0]  # any capability this node serves reaches its model server
        spec = fleet.capabilities.get(capability) if fleet else None
        is_cloud = bool(spec and spec.cloud)
        # A cloud artifact can only be measured by actually calling the provider — the
        # eval harness spends real cloud budget to do it (model-evaluation.md: "judge calls
        # draw on the cloud budget; suites are small"). This bypasses budget.py entirely,
        # deliberately: eval jobs go straight onto the resolved capability's own stream
        # rather than through routing.resolve_capability, and the bound is small on its own
        # (MAX_ARTIFACTS_IN_FLIGHT capped batches of a handful of items each).
        privacy = Privacy.cloud_ok if is_cloud else Privacy.local_only
        generation = store.current_eval_generation(artifact)
        for index, item in enumerate(suite):
            # Skip a class the suite cannot score. Dispatching it would spend real fleet
            # capacity on items `collect` is then obliged to throw away.
            if not suite_is_measurable(item.task_class, suite, min_items):
                continue
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
                # Pin the artifact under test; the executor (worker or cloud, ADR 30)
                # forwards this to override its capability's own default model.
                params={"model": artifact, "temperature": 0.0, "max_tokens": 256},
                urgency=Urgency.waitable,   # never wakes a machine for an eval
                privacy=privacy,
                result_key=result_key,
            )
            store.insert(
                id=job_id, result_key=result_key, capability=capability, created_at=now,
                urgency=Urgency.waitable.value, client_key=EVAL_CLIENT_KEY,
                task_class=item.task_class,
            )
            entry_id = await queue.enqueue(record.to_wire())
            store.record_delivery(job_id, stream=stream_key(capability),
                                  entry_id=entry_id)
            store.add_eval_run(job_id=job_id, artifact=artifact,
                               task_class=item.task_class, item_index=index,
                               result_key=result_key, created_at=now,
                               generation=generation, suite_version=SUITE_VERSION)
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
    scale_version: str = SCALE_VERSION, min_items: int = MIN_ITEMS_FOR_SCORE,
) -> int:
    """Score finished eval jobs and record ability for completed batches.

    Returns the number of runs scored this pass.
    """
    scored = 0
    # A batch is one (artifact, task_class, generation): the generation is what stops a
    # re-measured artifact being scored on its predecessor's items (ADR 15).
    touched: set[tuple[str, str, int]] = set()

    for run in store.pending_eval_runs():
        # A run dispatched against a different suite points at an index that now holds a
        # different item. Scoring it would measure one thing and record another, so it is
        # retired rather than guessed at. (Rows written before suite versioning carry an
        # empty string, which is likewise not the current suite.)
        if run.suite_version != SUITE_VERSION:
            store.fail_eval_run(run.job_id)
            _log.info("eval run %s discarded: measured suite %r, current is %r",
                      run.job_id, run.suite_version, SUITE_VERSION)
            continue
        result = await queue.read_result(run.result_key)
        if result is None:
            continue  # still queued or running
        if result.get("status") != "done":
            store.fail_eval_run(run.job_id)  # no signal from a failed/expired job
            touched.add((run.artifact, run.task_class, run.generation))
            continue
        text = _completion_text(result)
        index = run.item_index
        if text is None or index >= len(suite):
            store.fail_eval_run(run.job_id)
            touched.add((run.artifact, run.task_class, run.generation))
            continue
        store.score_eval_run(run.job_id, bool(suite[index].check(text)))
        scored += 1
        touched.add((run.artifact, run.task_class, run.generation))

    # A batch whose items have all settled yields an ability score — but only if enough of
    # them actually produced signal.
    for artifact, task_class, generation in touched:
        pending, done, passed, failed = store.eval_progress(artifact, task_class, generation)
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
        if done < min_items:
            # The sample is intact but too SMALL to carry a 1–10 score. MIN_SIGNAL_FRACTION
            # guards a batch that shrank; this guards one that was never big enough. Without
            # it, a suite of two items scored a model on two items — and a perfect run on a
            # single item was recorded as the top of the scale.
            _log.warning(
                "eval inconclusive: %s/%s — %d item(s) is below the %d needed for a score",
                artifact, task_class, done, min_items)
            continue
        ability = score_to_ability(passed / done)
        store.set_ability(artifact=artifact, task_class=task_class, score=ability,
                          scale_version=scale_version, updated_at=now,
                          provenance="measured", n_items=done, n_passed=passed)
        _log.info("eval complete: %s/%s → ability %.1f (%d/%d passed, %d no-signal)",
                  artifact, task_class, ability, passed, done, failed)

    return scored


async def eval_tick(
    store: Store, queue: Queue, *, now: str, fleet: Fleet | None = None,
    suite: list[EvalItem] = SEED_SUITE, scale_version: str = SCALE_VERSION,
    min_items: int = MIN_ITEMS_FOR_SCORE,
) -> tuple[int, int]:
    """One coordinator pass: collect finished work first, then dispatch new work."""
    collected = await collect(store, queue, now=now, suite=suite,
                              scale_version=scale_version, min_items=min_items)
    enqueued = await dispatch(store, queue, now=now, fleet=fleet, suite=suite,
                              scale_version=scale_version, min_items=min_items)
    return collected, enqueued
