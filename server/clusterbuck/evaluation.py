"""Model ability measurement (ADR 15 / model-evaluation.md).

Ability is `ability(artifact, task_class) → 1–10`, versioned. This module ships the
**provable core**: the ability matrix as data, seeded with anchored defaults, plus a
**tier-1 programmatic evaluator** — deterministic, no judge — that runs eval items and
turns pass-rates into scores.

Deferred, deliberately (they need a judge model, real usage data, and a real model mix —
model-evaluation.md says as much): tier-2 checklist judging, tier-3 pairwise
Bradley-Terry/Elo, anchor-based calibration, the model catalog + eval-gated downloads, and
cost-quality arbitrage. Those sit behind this seam; faking them would only pass fake tests.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable

from .store import Store

SCALE_VERSION = "2026.1"
TASK_CLASSES = ["extract", "summarize", "reason", "code", "embed"]

# Anchored seed defaults (model-evaluation.md: 5 ≈ good 8B-Q4, 7 ≈ strong 70B, 3 ≈ 3B).
# Placeholder starting data — a real tier-1/2/3 run overwrites these per installation.
SEED_ABILITY: dict[str, dict[str, float]] = {
    "llama3.2:3b":  {"extract": 4.0, "summarize": 4.0, "reason": 3.0, "code": 3.0, "embed": 4.0},
    "qwen2.5:32b":  {"extract": 7.0, "summarize": 7.0, "reason": 6.5, "code": 6.5, "embed": 6.0},
    "llama3.1:70b": {"extract": 8.0, "summarize": 8.0, "reason": 7.5, "code": 7.0, "embed": 7.0},
}


def seed_ability(store: Store, *, now: str, scale_version: str = SCALE_VERSION) -> int:
    """Populate the matrix with anchored defaults if empty. Returns rows written.

    These are PLACEHOLDERS so need-shaped routing works before anything has been measured.
    They are written with `provenance='seed'`, which the eval harness treats as unmeasured —
    otherwise the fleet's own shipped models would be exempted from evaluation forever and
    every routing decision would rest on a guess that merely looked authoritative.
    """
    if store.ability_count(scale_version) > 0:
        return 0
    n = 0
    for artifact, by_class in SEED_ABILITY.items():
        for task_class, score in by_class.items():
            store.set_ability(artifact=artifact, task_class=task_class, score=score,
                              scale_version=scale_version, updated_at=now,
                              provenance="seed")
            n += 1
    return n


# --- tier-1 programmatic checks (deterministic, unarguable) ---

# Chat models routinely wrap a JSON answer in a markdown fence (```json ... ```) even when
# told not to — reasoning models especially. Strip one before parsing so that formatting
# habit doesn't fail an otherwise-correct answer.
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*\n?(.*?)\n?```", re.DOTALL)


def check_json_valid(output: str) -> bool:
    text = (output or "").strip()
    fence = _JSON_FENCE_RE.search(text)
    if fence:
        text = fence.group(1).strip()
    try:
        json.loads(text)
        return True
    except (ValueError, TypeError):
        return False


def check_contains(substr: str) -> Callable[[str], bool]:
    return lambda out: substr.lower() in (out or "").lower()


def check_exact(expected: str) -> Callable[[str], bool]:
    return lambda out: (out or "").strip() == expected


def check_min_length(n: int) -> Callable[[str], bool]:
    return lambda out: len((out or "").strip()) >= n


@dataclass(frozen=True)
class EvalItem:
    task_class: str
    prompt: str
    check: Callable[[str], bool]


# A tiny deterministic seed suite (bespoke, not a public benchmark — contamination, ADR 15).
SEED_SUITE: list[EvalItem] = [
    EvalItem("extract", 'Return JSON {"n": 2} and nothing else.', check_json_valid),
    EvalItem("extract", 'Output the JSON object {"ok": true}.', check_json_valid),
    EvalItem("summarize", "Summarize: the quick brown fox jumps.", check_contains("fox")),
    EvalItem("code", "Reply with the word PASS.", check_contains("pass")),
    EvalItem(
        "reason",
        "A bus has 34 passengers. At the next stop, 9 get off and 5 get on. How many "
        "passengers are on the bus now? Reply with just the number.",
        check_contains("30"),
    ),
    EvalItem(
        "reason",
        "Dana is older than Ellen. Ellen is older than Frank. Who is the youngest? "
        "Reply with just the name.",
        check_contains("frank"),
    ),
]


def score_to_ability(pass_rate: float) -> float:
    """Map a tier-1 pass-rate to the 1–10 scale at half-point granularity.

    A deterministic tier-1 proxy; anchor-based calibration against pinned reference
    artifacts (model-evaluation.md) is deferred.
    """
    raw = 1.0 + 9.0 * pass_rate
    return max(1.0, min(10.0, round(raw * 2) / 2))


def run_tier1(items: list[EvalItem], complete: Callable[[str], str]) -> dict[str, tuple[float, float]]:
    """Run items through `complete` (prompt→text). Returns {task_class: (pass_rate, ability)}."""
    tally: dict[str, list[int]] = defaultdict(lambda: [0, 0])  # [passed, total]
    for item in items:
        ok = item.check(complete(item.prompt))
        tally[item.task_class][0] += 1 if ok else 0
        tally[item.task_class][1] += 1
    return {tc: (p / t, score_to_ability(p / t)) for tc, (p, t) in tally.items()}


def evaluate_and_record(store: Store, artifact: str, items: list[EvalItem],
                        complete: Callable[[str], str], *, now: str,
                        scale_version: str = SCALE_VERSION) -> dict[str, tuple[float, float]]:
    """Run tier-1 for an artifact and write the resulting abilities to the matrix."""
    results = run_tier1(items, complete)
    for task_class, (_rate, score) in results.items():
        store.set_ability(artifact=artifact, task_class=task_class, score=score,
                          scale_version=scale_version, updated_at=now)
    return results
