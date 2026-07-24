# Model evaluation & ability

How clusterbuck measures model quality and turns it into the **ability score** clients
use to ask for "a good-enough model" without naming one. This powers the
[fleet-management](fleet-management.md) eval gate, the planner's cost-quality
suggestions, and the router's model selection.

## Ability is a matrix (but we ship a scalar too)

Model quality is *jagged*: the same model can be strong at structured extraction and
weak at multi-step reasoning, or vice versa. A single per-model number routes jobs
wrongly. So the primitive is:

```
ability(artifact, task_class) → 1–10
```

- **artifact** = model + quantisation (an 8B-Q4 and 8B-Q8 are different artifacts;
  quality belongs to the artifact, throughput belongs to artifact × node).
- **task_class** = the classes jobs already carry: `extract | summarize | reason |
  code | embed`.
- A **headline scalar** per artifact still exists for humans: the ability matrix
  weighted by the fleet's *actual task mix* — "6.2" means "for what this network does."
- Scores are coarse by design: **half-point granularity**, with an uncertainty note.
  Small suites cannot honestly distinguish 6.3 from 6.5; don't pretend otherwise.

## Measurement: three tiers, most reliable first

### Tier 1 — programmatic checks (deterministic, no judge)
Wherever an output is mechanically verifiable, verify it:

- JSON validity + schema conformance; enum-value validity.
- Exact-answer retrieval from a supplied document (long-context recall).
- Instruction compliance: length limits, format, forbidden content honoured.
- Code items that must pass unit tests; math with known answers.

Free, unarguable, and covers most of what matters for extraction-type work. Run these
the most.

### Tier 2 — checklist judging (for summaries and extraction quality)
Never ask a judge "rate this summary 1–10" — holistic grading is noisy and biased.
Each eval item ships with a **gold checklist of key points** for its source text; a
judge model answers verifiable micro-questions:

- **Coverage:** is each checklist point present in the output? → coverage %.
- **Faithfulness:** does the output assert anything not supported by the source?
  → hallucination rate.
- **Coherence:** a small (1–5) readability/structure score.

Item-by-item verification is far more reliable than open-ended scoring.

### Tier 3 — pairwise preference (for open-ended reasoning/writing)
Show the judge two models' outputs for the same prompt; it picks the better (ties
allowed). **Randomise A/B order** (position bias is real) and aggregate with
**Bradley-Terry / Elo** per task class. Judges are good at "which is better," bad at
"how good is this" — pairwise exploits the reliable question.

### Judge rules
- The judge must be **materially stronger** than the models being judged: the frontier
  cloud model when budget allows, else the largest local artifact.
- A model **never judges itself** (self-preference bias).
- Rubrics instruct **length-neutrality** (judges over-reward verbosity).

## Calibration: an anchored 1–10 scale

Raw Elo/coverage numbers mean nothing to users. The scale is pinned by **anchor
artifacts** with assigned scores; new artifacts are placed by pairwise comparison
against the nearest anchors and interpolated on the Bradley-Terry scale. Illustrative
anchors (not normative):

| Ability | Anchor class |
|---|---|
| 9–10 | Frontier cloud models |
| 7 | Strong 70B-class local |
| 5 | Good 8B-class at Q4 |
| 3 | 3B-class |
| 1 | Sub-1B / broken quantisation |

The scale carries a **version**: when the frontier moves, anchors are rebased and
stored scores re-stamped against the new version — otherwise "8" silently deflates
over the years. Ability values are always reported with their scale version.

## Eval suites

- **Per task class**, small and fixed: ~20–50 items each, versioned alongside the
  scale version.
- **Bespoke and rotating** — never reuse well-known public benchmarks verbatim; models
  have memorised them (contamination), which inflates scores meaninglessly.
- clusterbuck ships generic seed suites; an installation may add **private local
  items** (never shared) so scores reflect its real material.
- **Evals run as ordinary jobs**: low-priority `wait` jobs on the fleet itself — the
  harness is just another client. Judge calls draw on the cloud budget (suites are
  small; judging costs are trivial next to normal usage).

## When to (re)measure

- A new artifact is installed (the [upgrade eval gate](fleet-management.md)).
- The artifact changes: different quantisation, engine/runtime major update.
- Anchor rebase / scale-version bump.
- A periodic light regression pass (e.g. quarterly), plus optional **shadow testing** —
  run a candidate silently on a sample of real jobs and compare before promotion.

## How the router uses ability

The client-facing request contract becomes **need-shaped** rather than supply-shaped:

```
{ "task_class": "summarize", "min_ability": 6, "privacy": "local_only", "urgency": "waitable" }
```

Selection: filter artifacts by `ability(artifact, task_class) ≥ min_ability` and the
privacy class → prefer **local** → then **cheapest** → then fastest. Explicit
`capability` addressing remains for power users; capability tiers otherwise become an
internal supply-side detail. A `min_ability` no local artifact can meet routes to a
cloud model (if `cloud_ok`) or fails explicitly (if `local_only`).

## Provider accounts, budget, and the cost-quality loop

- **Provider accounts** (e.g. Anthropic, OpenAI) are registered on the coordinator with
  API keys (held by the gateway) and per-Mtoken pricing per model.
- Cloud models enter the **same catalog** as local ones — with *measured* ability, not
  assumed; they are just artifacts with a price and no host node.
- The monthly **budget** ([fleet-management](fleet-management.md)) gets **pacing** (a
  soft daily allowance so week one can't burn the month) and a **reserve** for
  interactive `now` jobs.
- Metering + the ability matrix close the loop for the planner's best trick,
  **cost-quality arbitrage**: "this month's cloud `summarize` spend was $X at ability 9;
  a local artifact scoring 5.8 clears your `min_ability` for 80% of those jobs —
  install it on node N."

## Pitfalls (design around these)

- **Scalar worship** — the headline number is for humans; the router must use the
  matrix.
- **Tiny-difference chasing** — half-point granularity, report uncertainty, ties are
  fine.
- **Benchmark contamination** — bespoke, rotating items only.
- **Judge bias** — stronger judge, never self-judging, order randomisation,
  length-neutral rubrics.
