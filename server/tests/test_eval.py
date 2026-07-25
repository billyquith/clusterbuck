"""Tier-1 programmatic evaluator (M5): deterministic scoring, no judge."""

from __future__ import annotations

from clusterbuck.evaluation import (
    SCALE_VERSION,
    SEED_SUITE,
    EvalItem,
    check_json_valid,
    evaluate_and_record,
    run_tier1,
    score_to_ability,
)
from clusterbuck.store import Store


def test_score_to_ability_endpoints_and_half_points():
    assert score_to_ability(1.0) == 10.0
    assert score_to_ability(0.0) == 1.0
    assert score_to_ability(0.5) == 5.5   # 1 + 9*0.5
    assert score_to_ability(0.33) == 4.0  # 1 + 2.97 = 3.97 → nearest 0.5


def test_run_tier1_perfect_and_zero():
    items = [EvalItem("extract", "p1", check_json_valid),
             EvalItem("extract", "p2", check_json_valid)]
    assert run_tier1(items, lambda p: '{"x": 1}') == {"extract": (1.0, 10.0)}
    assert run_tier1(items, lambda p: "not json") == {"extract": (0.0, 1.0)}


def test_evaluate_and_record_writes_matrix(tmp_path):
    store = Store(str(tmp_path / "eval.db"))

    def complete(prompt: str) -> str:
        # A cooperative model: valid JSON for json prompts, else a phrase with the tokens.
        return '{"n": 2}' if "JSON" in prompt else "the quick fox says pass"

    results = evaluate_and_record(store, "test-model", SEED_SUITE, complete, now="t")
    assert results["extract"] == (1.0, 10.0)      # both JSON items pass
    assert store.get_ability("test-model", "extract", SCALE_VERSION) == 10.0
    assert store.get_ability("test-model", "summarize", SCALE_VERSION) == 10.0
    assert store.get_ability("test-model", "code", SCALE_VERSION) == 10.0


def test_evaluate_records_partial_credit(tmp_path):
    store = Store(str(tmp_path / "eval2.db"))
    items = [EvalItem("extract", "a", check_json_valid),
             EvalItem("extract", "b", check_json_valid),
             EvalItem("extract", "c", check_json_valid),
             EvalItem("extract", "d", check_json_valid)]
    # Half the items produce valid JSON.
    outputs = iter(['{"ok": 1}', "nope", '{"ok": 1}', "nope"])
    evaluate_and_record(store, "m", items, lambda p: next(outputs), now="t")
    assert store.get_ability("m", "extract", SCALE_VERSION) == 5.5  # 0.5 pass-rate
