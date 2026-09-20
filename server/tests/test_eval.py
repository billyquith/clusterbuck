"""Tier-1 programmatic evaluator (M5): deterministic scoring, no judge."""

from __future__ import annotations

from clusterbuck.evaluation import (
    MIN_ITEMS_FOR_SCORE,
    SCALE_VERSION,
    SEED_SUITE,
    TASK_CLASSES,
    TIER1_MAX_ABILITY,
    EvalItem,
    check_json_valid,
    evaluate_and_record,
    items_for,
    run_tier1,
    score_to_ability,
    suite_is_measurable,
)
from clusterbuck.store import Store


def test_a_perfect_programmatic_run_is_strong_not_frontier():
    """The cap is the point. The old mapping was 1 + 9*rate, so a clean sweep of a handful
    of compliance items scored 10.0 — which model-evaluation.md's anchors define as a
    frontier cloud model. Programmatic checks establish that a model COMPLIES; they cannot
    tell a 7 from a 9, so they do not get to assert one."""
    assert score_to_ability(1.0) == TIER1_MAX_ABILITY
    assert TIER1_MAX_ABILITY < 10.0, "the scale still reaches 10; the instrument does not"


def test_score_to_ability_endpoints_and_half_points():
    assert score_to_ability(0.0) == 1.0
    assert score_to_ability(0.5) == 4.0    # 1 + 6*0.5
    assert score_to_ability(0.33) == 3.0   # 1 + 1.98 = 2.98 → nearest 0.5
    assert score_to_ability(1.5) == TIER1_MAX_ABILITY   # clamped, never extrapolated


def test_run_tier1_perfect_and_zero():
    items = [EvalItem("extract", "p1", check_json_valid),
             EvalItem("extract", "p2", check_json_valid)]
    assert run_tier1(items, lambda p: '{"x": 1}') == {"extract": (1.0, TIER1_MAX_ABILITY)}
    assert run_tier1(items, lambda p: "not json") == {"extract": (0.0, 1.0)}


def test_evaluate_and_record_writes_matrix(tmp_path):
    store = Store(str(tmp_path / "eval.db"))
    items = [EvalItem("extract", f"p{i}", check_json_valid) for i in range(MIN_ITEMS_FOR_SCORE)]

    results = evaluate_and_record(store, "test-model", items, lambda p: '{"n": 2}', now="t")
    assert results["extract"] == (1.0, TIER1_MAX_ABILITY)
    assert store.get_ability("test-model", "extract", SCALE_VERSION) == TIER1_MAX_ABILITY


def test_a_class_with_too_few_items_is_run_but_not_recorded(tmp_path):
    """A score from a handful of items is noise wearing a number's clothes. The old suite
    decided a model's whole `code` ability from ONE item — 'Reply with the word PASS' —
    which any model passes, and which then beat a correctly-configured cloud model in
    routing because the sort prefers local."""
    store = Store(str(tmp_path / "tiny.db"))
    items = [EvalItem("code", "p", check_json_valid)] * (MIN_ITEMS_FOR_SCORE - 1)

    results = evaluate_and_record(store, "m", items, lambda p: '{"ok": 1}', now="t")
    assert results["code"][0] == 1.0, "it still RUNS the items"
    assert store.get_ability("m", "code", SCALE_VERSION) is None, "but records nothing"


def test_the_shipped_suite_can_actually_measure_every_class_it_names():
    """A task class in TASK_CLASSES with no items is unroutable-but-scored limbo: seeded
    forever, never measured, never replaced. `embed` sat there until it was removed."""
    for task_class in TASK_CLASSES:
        n = len(items_for(task_class, SEED_SUITE))
        assert suite_is_measurable(task_class, SEED_SUITE), (
            f"{task_class} has {n} item(s), below the {MIN_ITEMS_FOR_SCORE} needed")


def test_evidence_counts_are_recorded_beside_the_score(tmp_path):
    """model-evaluation.md asks for scores "with an uncertainty note". A bare number made a
    10 from one item indistinguishable from a 7 from forty."""
    store = Store(str(tmp_path / "ev.db"))
    items = [EvalItem("extract", f"p{i}", check_json_valid) for i in range(10)]
    outputs = iter(['{"ok": 1}'] * 7 + ["nope"] * 3)
    evaluate_and_record(store, "m", items, lambda p: next(outputs), now="t")

    row = next(r for r in store.ability_matrix(SCALE_VERSION) if r.artifact == "m")
    assert (row.n_items, row.n_passed) == (10, 7)


def test_evaluate_records_partial_credit(tmp_path):
    store = Store(str(tmp_path / "eval2.db"))
    items = [EvalItem("extract", "a", check_json_valid),
             EvalItem("extract", "b", check_json_valid),
             EvalItem("extract", "c", check_json_valid),
             EvalItem("extract", "d", check_json_valid)]
    # Half the items produce valid JSON.
    outputs = iter(['{"ok": 1}', "nope", '{"ok": 1}', "nope"])
    evaluate_and_record(store, "m", items, lambda p: next(outputs), now="t", min_items=1)
    assert store.get_ability("m", "extract", SCALE_VERSION) == 4.0  # 0.5 pass-rate
