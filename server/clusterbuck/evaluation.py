"""Model ability measurement (ADR 15 / model-evaluation.md).

Ability is `ability(artifact, task_class) → 1–10`, versioned. This module ships the
**provable core**: the ability matrix as data, seeded with anchored defaults, plus a
**tier-1 programmatic evaluator** — deterministic, no judge — that runs eval items and
turns pass-rates into scores.

Two properties of this tier decide how much its output may be trusted, and both are
enforced here rather than left as documentation:

* **It cannot reach the top of the scale.** Programmatic checks verify *compliance* — did
  the model produce the requested shape, follow the format, get the arithmetic right — not
  *quality*. They genuinely cannot tell a 7 from a 9. So a perfect tier-1 pass rate maps to
  `TIER1_MAX_ABILITY`, the "strong local model" anchor, and the 8–10 band stays reserved
  for the judged tiers that can justify it. Letting `1 + 9·rate` reach 10 certified a 3B as
  frontier-equivalent on the strength of a single one-word answer.
* **It needs enough items to mean anything.** A score derived from one item is not a
  measurement, and half-point granularity over a handful of items is a fiction. Each task
  class carries at least `MIN_ITEMS_FOR_SCORE` items and the runner refuses to record a
  score below that count.

Deferred, deliberately (they need a judge model, real usage data, and a real model mix —
model-evaluation.md says as much): tier-2 checklist judging, tier-3 pairwise
Bradley-Terry/Elo, anchor-based calibration, and cost-quality arbitrage. Those sit behind
this seam; faking them would only pass fake tests.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable

from .store import Store

# Bumped from 2026.1 when the tier-1 instrument was recalibrated: the pass-rate mapping,
# the minimum sample and the suite itself all changed, so a score recorded under the old
# version does not mean the same thing as one recorded now. Re-stamping rather than
# silently mixing the two is the mechanism model-evaluation.md already specifies for a
# scale change ("stored scores re-stamped against the new version"). Routing reads only the
# current version, so every artifact is re-measured against the new instrument.
SCALE_VERSION = "2026.2"

# Item identity within a suite is POSITIONAL (`item_index`), so reordering or re-cutting
# the suite would mis-score any run still in flight. Stamping the version on each run lets
# the collector discard runs measuring a suite that no longer exists, rather than scoring
# them against whatever now sits at that index.
SUITE_VERSION = "2026.2"

TASK_CLASSES = ["extract", "summarize", "reason", "code"]

# The highest ability a purely programmatic pass rate may claim. model-evaluation.md's
# anchors put 7 at "strong 70B-class local" and 9–10 at "frontier cloud". Compliance checks
# cannot distinguish those upper bands, so they do not get to assert them. A judged tier —
# or an operator writing a score directly — can still record above this; the cap is on the
# INSTRUMENT, not on the scale.
TIER1_MAX_ABILITY = 7.0

# Fewest settled items a task class needs before a score is recorded at all. Below this,
# one lucky answer moves the score by whole points and the number describes noise.
MIN_ITEMS_FOR_SCORE = 8

# Anchored seed defaults (model-evaluation.md: 5 ≈ good 8B-Q4, 3 ≈ 3B). Placeholder
# starting data — a real tier-1 run overwrites these per installation. Capped at the same
# ceiling as a measurement, so nothing in a fresh install claims a band the system has no
# instrument to justify.
# The 70B row sits exactly on its anchor (7 = "strong 70B-class local") rather than below
# it: a seed that undershoots its own anchor makes `min_ability: 7` unsatisfiable across a
# whole fresh fleet, which reads as "your fleet is inadequate" when it is really "the
# placeholder was timid".
SEED_ABILITY: dict[str, dict[str, float]] = {
    "llama3.2:3b":  {"extract": 4.0, "summarize": 4.0, "reason": 3.0, "code": 3.0},
    "qwen2.5:32b":  {"extract": 6.5, "summarize": 6.5, "reason": 6.0, "code": 6.0},
    "llama3.1:70b": {"extract": 7.0, "summarize": 7.0, "reason": 7.0, "code": 6.5},
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
            store.set_ability(artifact=artifact, task_class=task_class,
                              score=min(score, TIER1_MAX_ABILITY),
                              scale_version=scale_version, updated_at=now,
                              provenance="seed")
            n += 1
    return n


# --- tier-1 programmatic checks (deterministic, unarguable) ---

# Chat models routinely wrap a JSON answer in a markdown fence (```json ... ```) even when
# told not to — reasoning models especially. Strip one before parsing so that formatting
# habit doesn't fail an otherwise-correct answer.
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*\n?(.*?)\n?```", re.DOTALL)

# Some models emit a <think>…</think> preamble. It is not the answer, and leaving it in
# makes a "does the reply contain X" check pass on the model's own deliberation.
_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def _clean(output: str | None) -> str:
    text = _THINK_RE.sub(" ", output or "").strip()
    fence = _JSON_FENCE_RE.search(text)
    return fence.group(1).strip() if fence else text


def _parse_json(output: str | None) -> Any:
    """Parsed JSON from a reply, or `None` if it isn't any.

    Falls back to the first {...} or [...] span, because a model that answers correctly but
    prefixes "Here is the JSON:" has done the task. What it must NOT do is omit the data.
    """
    text = _clean(output)
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        pass
    for opener, closer in (("{", "}"), ("[", "]")):
        start, end = text.find(opener), text.rfind(closer)
        if 0 <= start < end:
            try:
                return json.loads(text[start:end + 1])
            except (ValueError, TypeError):
                continue
    return None


def check_json_valid(output: str) -> bool:
    """Parses as JSON at all. Kept for callers that only care about the shape."""
    return _parse_json(output) is not None


def check_contains(substr: str) -> Callable[[str], bool]:
    return lambda out: substr.lower() in _clean(out).lower()


def check_exact(expected: str) -> Callable[[str], bool]:
    return lambda out: _clean(out) == expected


def check_min_length(n: int) -> Callable[[str], bool]:
    return lambda out: len(_clean(out)) >= n


def check_json_fields(**expected: Any) -> Callable[[str], bool]:
    """Valid JSON object whose named fields hold the expected values.

    This is the check `check_json_valid` should always have been for an extraction item.
    Asking for `{"n": 2}` and accepting `{}` measured whether the model can emit braces,
    not whether it can extract anything — and two such items were enough to certify a 3B at
    the top of the extract scale.

    Comparison is case-insensitive and whitespace-trimmed on strings; numbers compare by
    value, so `2` and `2.0` and `"2"` all satisfy an expected `2`.
    """
    def norm(v: Any) -> Any:
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            s = v.strip()
            try:
                return float(s)
            except ValueError:
                return s.casefold()
        return v

    def run(out: str) -> bool:
        data = _parse_json(out)
        if not isinstance(data, dict):
            return False
        lowered = {str(k).casefold(): v for k, v in data.items()}
        for key, want in expected.items():
            if key.casefold() not in lowered:
                return False
            if norm(lowered[key.casefold()]) != norm(want):
                return False
        return True
    return run


def check_json_array(*members: str, exact_length: int | None = None) -> Callable[[str], bool]:
    """A JSON array containing exactly the given members (order-insensitive)."""
    want = {m.casefold() for m in members}

    def run(out: str) -> bool:
        data = _parse_json(out)
        if not isinstance(data, list):
            return False
        got = {str(x).strip().casefold() for x in data}
        if exact_length is not None and len(data) != exact_length:
            return False
        return got == want
    return run


_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")


def check_number(expected: float, *, not_also: tuple[float, ...] = ()) -> Callable[[str], bool]:
    """The reply states this number, and does not also state a named wrong one.

    `check_contains("30")` passed on "130" and on a model that talked through several
    candidate answers without committing. Matching whole numeric tokens, and naming the
    distractors an item is actually probing for, makes the item discriminate instead of
    rewarding verbosity.
    """
    def run(out: str) -> bool:
        found = {float(m) for m in _NUMBER_RE.findall(_clean(out))}
        return expected in found and not (set(not_also) & found)
    return run


def check_word_limit(limit: int) -> Callable[[str], bool]:
    """At most `limit` words. Instruction compliance, mechanically checkable."""
    return lambda out: 0 < len(_clean(out).split()) <= limit


def check_absent(*forbidden: str) -> Callable[[str], bool]:
    """None of these appear. Used as a FAITHFULNESS probe on summarize items: the terms are
    plausible for the topic but absent from the source, so a model that imports them is
    inventing rather than compressing."""
    lowered = [f.lower() for f in forbidden]
    return lambda out: not any(f in _clean(out).lower() for f in lowered)


def check_all(*checks: Callable[[str], bool]) -> Callable[[str], bool]:
    return lambda out: all(c(out) for c in checks)


def check_one_of(*allowed: str) -> Callable[[str], bool]:
    """The whole reply is exactly one of these values — an enum item, where a model that
    editorialises has failed the instruction even if it picked the right label."""
    options = {a.casefold() for a in allowed}
    return lambda out: _clean(out).strip(" .\"'").casefold() in options


@dataclass(frozen=True)
class EvalItem:
    task_class: str
    prompt: str
    check: Callable[[str], bool]


_JSON_ONLY = " Reply with JSON only, no other text."
_ANSWER_ONLY = " Reply with the answer only, nothing else."


# The tier-1 suite: bespoke items, never a public benchmark verbatim (contamination,
# ADR 15). Every check is deterministic, and every item is written so that echoing the
# prompt, emitting an empty object, or talking around the answer FAILS it.
SEED_SUITE: list[EvalItem] = [
    # --- extract: pull stated facts into an exact shape ----------------------------
    EvalItem("extract",
             "Text: 'Invoice 4471 was paid on 2026-03-14 for 89.50 GBP.'"
             " Return the invoice number as `invoice`, the amount as `amount`, and the"
             " currency as `currency`." + _JSON_ONLY,
             check_json_fields(invoice=4471, amount=89.5, currency="GBP")),
    EvalItem("extract",
             "Text: 'The shipment left Rotterdam on Tuesday and arrives in Oslo on"
             " Friday.' Return `origin` and `destination`." + _JSON_ONLY,
             check_json_fields(origin="Rotterdam", destination="Oslo")),
    EvalItem("extract",
             "Extract every email address from this text as a JSON array of strings:"
             " 'Write to ana@example.org or, failing that, to dev-null@example.net.'"
             + _JSON_ONLY,
             check_json_array("ana@example.org", "dev-null@example.net",
                              exact_length=2)),
    EvalItem("extract",
             "Extract every email address from this text as a JSON array of strings:"
             " 'Our office is open Monday to Thursday; ring the front desk.'"
             + _JSON_ONLY,
             check_json_array(exact_length=0)),
    EvalItem("extract",
             "Log line: '2026-01-09T04:12:55Z WARN disk_pressure device=/dev/sdb"
             " free_pct=4'. Return `level`, `event` and `free_pct`." + _JSON_ONLY,
             check_json_fields(level="WARN", event="disk_pressure", free_pct=4)),
    EvalItem("extract",
             "Text: 'Priya Raman (she/her) joined the Lisbon office in 2019.'"
             " Return `name` and `year`." + _JSON_ONLY,
             check_json_fields(name="Priya Raman", year=2019)),
    EvalItem("extract",
             "Classify the sentiment of 'The delivery was three days late and nobody"
             " replied to my emails.' Answer with exactly one of: positive, negative,"
             " neutral." + _ANSWER_ONLY,
             check_one_of("negative")),
    EvalItem("extract",
             "Classify the sentiment of 'The package arrived on the day it was promised.'"
             " Answer with exactly one of: positive, negative, neutral." + _ANSWER_ONLY,
             check_one_of("positive")),
    EvalItem("extract",
             "Text: 'Order SJ-2291 contains 3 units of part XR7 and 12 units of part"
             " XR9.' Return `order`, and `total_units` as the sum." + _JSON_ONLY,
             check_json_fields(order="SJ-2291", total_units=15)),
    EvalItem("extract",
             "Text: 'Reboot scheduled 02:30–03:15 UTC on 2026-05-02.' Return `date`,"
             " `start` and `end` (times as HH:MM)." + _JSON_ONLY,
             check_json_fields(date="2026-05-02", start="02:30", end="03:15")),

    # --- summarize: compress without inventing, and obey the limit -----------------
    # Faithfulness is probed by forbidden terms: each is plausible for the topic and
    # absent from the source, so importing one is invention, not compression.
    EvalItem("summarize",
             "Summarise in at most 15 words: 'The library will close for eight weeks"
             " from June while the roof is replaced. Borrowing moves to the mobile van,"
             " which will park outside the town hall on Wednesdays.'",
             check_all(check_word_limit(15), check_contains("roof"),
                       check_absent("fire", "funding", "closed permanently"))),
    EvalItem("summarize",
             "Summarise in at most 12 words: 'Heavy rain delayed the match by two hours."
             " It finished 1-1 after both goals came in the final ten minutes.'",
             check_all(check_word_limit(12), check_contains("rain"),
                       check_absent("penalty", "penalties", "extra time", "cancelled"))),
    EvalItem("summarize",
             "Summarise in at most 15 words: 'The bakery on Mill Lane has changed hands."
             " The new owner keeps the sourdough recipe but has dropped the cafe"
             " seating.'",
             check_all(check_word_limit(15), check_contains("sourdough"),
                       check_absent("closed down", "franchise", "expanded"))),
    EvalItem("summarize",
             "In one sentence of at most 20 words, say what this changelog entry does:"
             " 'Fixed a race where two workers could claim the same task, which"
             " occasionally produced duplicate invoices.'",
             check_all(check_word_limit(20), check_contains("duplicate"),
                       check_absent("security", "vulnerability", "performance"))),
    EvalItem("summarize",
             "Summarise in at most 10 words: 'Membership fell for a third year, though"
             " the under-18 section grew slightly.'",
             check_all(check_word_limit(10),
                       check_absent("increased overall", "record high", "profit"))),
    EvalItem("summarize",
             "Summarise the OUTCOME only, in at most 8 words: 'After a long debate the"
             " council voted 7-4 to reject the parking proposal.'",
             check_all(check_word_limit(8), check_contains("reject"),
                       check_absent("approved", "passed", "accepted"))),
    EvalItem("summarize",
             "Summarise in at most 12 words: 'The ferry runs hourly in summer and twice"
             " daily in winter; it does not carry vehicles.'",
             check_all(check_word_limit(12),
                       check_absent("free", "cars welcome", "carries vehicles"))),
    EvalItem("summarize",
             "Summarise in at most 15 words: 'The trial found the drug reduced symptoms"
             " in 3 of 40 patients, which the authors call inconclusive.'",
             check_all(check_word_limit(15), check_contains("inconclusive"),
                       check_absent("cure", "breakthrough", "highly effective"))),
    EvalItem("summarize",
             "Summarise in at most 12 words: 'The museum will open a new wing in 2028,"
             " funded entirely by a private bequest.'",
             check_all(check_word_limit(12), check_contains("wing"),
                       check_absent("taxpayer", "government funding", "lottery"))),
    EvalItem("summarize",
             "Summarise in at most 10 words: 'Following complaints, the bus route will"
             " keep its original timetable for another six months.'",
             check_all(check_word_limit(10),
                       check_absent("cancelled", "new timetable", "rerouted"))),

    # --- reason: one right answer, arrived at in steps ------------------------------
    EvalItem("reason",
             "A bus has 34 passengers. At the next stop 9 get off and 5 get on. How many"
             " passengers are on the bus now?" + _ANSWER_ONLY,
             check_number(30, not_also=(38, 48, 25))),
    EvalItem("reason",
             "Dana is older than Ellen. Ellen is older than Frank. Who is the youngest?"
             + _ANSWER_ONLY,
             check_all(check_contains("frank"), check_absent("dana", "ellen"))),
    EvalItem("reason",
             "A shelf holds 4 boxes. Each box holds 6 jars, and 3 jars are empty in"
             " total. How many jars are NOT empty?" + _ANSWER_ONLY,
             check_number(21, not_also=(24, 12, 18))),
    EvalItem("reason",
             "Which is larger, 9.11 or 9.9?" + _ANSWER_ONLY,
             check_all(check_contains("9.9"), check_absent("9.11 is larger",
                                                           "9.11 is greater"))),
    EvalItem("reason",
             "A meeting starts at 14:45 and lasts 95 minutes. What time does it end?"
             " Answer as HH:MM." + _ANSWER_ONLY,
             check_all(check_contains("16:20"), check_absent("15:20", "16:40"))),
    EvalItem("reason",
             "Every cadet in the squad can swim. Rosa is in the squad. Can Rosa swim?"
             " Answer yes or no." + _ANSWER_ONLY,
             check_one_of("yes")),
    EvalItem("reason",
             "No cadet in the squad can fly. Rosa is in the squad. Can Rosa fly?"
             " Answer yes or no." + _ANSWER_ONLY,
             check_one_of("no")),
    EvalItem("reason",
             "A tank holds 50 litres. It is three-fifths full. How many litres are in it?"
             + _ANSWER_ONLY,
             check_number(30, not_also=(20, 50, 15))),
    EvalItem("reason",
             "Ravi is twice as old as Sam. Together they are 27. How old is Sam?"
             + _ANSWER_ONLY,
             check_number(9, not_also=(18, 13.5, 27))),
    EvalItem("reason",
             "If today is Thursday, what day will it be in 10 days?" + _ANSWER_ONLY,
             check_all(check_contains("sunday"),
                       check_absent("saturday", "monday", "tuesday"))),

    # --- code: predict behaviour exactly (nothing is executed here) -----------------
    EvalItem("code",
             "What does this print?\n\nxs = [1, 2, 3, 4]\nprint(sum(xs[1:3]))"
             + _ANSWER_ONLY,
             check_number(5, not_also=(10, 6, 9))),
    EvalItem("code",
             "What does this print?\n\nd = {'a': 1}\nd['b'] = d.get('a', 0) + 2"
             "\nprint(d['b'])" + _ANSWER_ONLY,
             check_number(3, not_also=(2, 1, 0))),
    EvalItem("code",
             "What does this print?\n\nprint(len('clusterbuck'))" + _ANSWER_ONLY,
             check_number(11, not_also=(10, 12))),
    EvalItem("code",
             "What does this print?\n\nfor i in range(3):\n    pass\nprint(i)"
             + _ANSWER_ONLY,
             check_number(2, not_also=(3, 0))),
    EvalItem("code",
             "What does this print?\n\nprint('ab' * 3)" + _ANSWER_ONLY,
             check_all(check_contains("ababab"), check_absent("ab ab ab"))),
    EvalItem("code",
             "Does this raise an exception?\n\nxs = [1, 2]\nprint(xs[2])\n\nAnswer yes"
             " or no." + _ANSWER_ONLY,
             check_one_of("yes")),
    EvalItem("code",
             "What does this print?\n\nprint(list(range(1, 10, 3)))" + _ANSWER_ONLY,
             check_all(check_contains("1"), check_contains("4"), check_contains("7"),
                       check_absent("10"))),
    EvalItem("code",
             "What does this print?\n\ns = 'a,b,,c'\nprint(len(s.split(',')))"
             + _ANSWER_ONLY,
             check_number(4, not_also=(3, 5))),
    EvalItem("code",
             "Return a JSON object with key `sql` whose value is a SQL statement counting"
             " rows in the table `orders`." + _JSON_ONLY,
             lambda out: (lambda v: isinstance(v, str)
                          and "count" in v.lower() and "orders" in v.lower()
                          and v.lower().strip().startswith("select"))(
                              (_parse_json(out) or {}).get("sql")
                              if isinstance(_parse_json(out), dict) else None)),
    EvalItem("code",
             "What does this print?\n\nprint(10 // 3, 10 % 3)" + _ANSWER_ONLY,
             check_all(check_contains("3"), check_contains("1"),
                       check_absent("3.33", "3.3"))),
]


def score_to_ability(pass_rate: float) -> float:
    """Map a tier-1 pass-rate onto the scale, at half-point granularity.

    Spans 1 → `TIER1_MAX_ABILITY`, not 1 → 10. Programmatic checks establish that a model
    complies; they cannot establish that it is frontier-grade, and the old `1 + 9·rate`
    let a perfect run on a handful of compliance items claim exactly that. Reaching the
    8–10 band requires an instrument that can tell those bands apart — the judged tiers.
    """
    span = TIER1_MAX_ABILITY - 1.0
    raw = 1.0 + span * max(0.0, min(1.0, pass_rate))
    return max(1.0, min(TIER1_MAX_ABILITY, round(raw * 2) / 2))


def items_for(task_class: str, suite: list[EvalItem] = SEED_SUITE) -> list[EvalItem]:
    return [i for i in suite if i.task_class == task_class]


def suite_is_measurable(task_class: str, suite: list[EvalItem] = SEED_SUITE,
                        min_items: int = MIN_ITEMS_FOR_SCORE) -> bool:
    """Whether the suite carries enough items for this class to yield an honest score.

    `min_items` is a parameter rather than a hard constant so a test can exercise the
    runner's batching on a deliberately tiny suite. Production callers take the default.
    """
    return len(items_for(task_class, suite)) >= min_items


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
                        scale_version: str = SCALE_VERSION,
                        min_items: int = MIN_ITEMS_FOR_SCORE,
                        ) -> dict[str, tuple[float, float]]:
    """Run tier-1 for an artifact and write the resulting abilities to the matrix.

    Task classes with too few items are run but NOT recorded: a score from a handful of
    items is noise wearing a number's clothes.
    """
    results = run_tier1(items, complete)
    counts: dict[str, int] = defaultdict(int)
    for item in items:
        counts[item.task_class] += 1
    for task_class, (rate, score) in results.items():
        n = counts[task_class]
        if n < min_items:
            continue
        store.set_ability(artifact=artifact, task_class=task_class, score=score,
                          scale_version=scale_version, updated_at=now,
                          n_items=n, n_passed=round(rate * n))
    return results
