---
name: fleet-curation
description: Curate the clusterbuck model catalog and refine capability tiers — adding or updating candidate models, deciding when a tier earns its existence, and diagnosing why a node enrolls healthily but never receives work. Use this whenever the user mentions the model catalog, adding or upgrading a model, model upgrade proposals, fleet.yaml, capability tiers (like 8b-extract or 32b-reason), ability scores, the eval harness, or asks why jobs aren't routing to a node — even if they frame it as a simple config edit. clusterbuck keeps three separate registries that are easy to confuse, and editing the wrong one produces a change that looks applied but does nothing.
---

# Curating the fleet

clusterbuck routes jobs to whichever node can serve them. Two questions decide that:
*what may this node install* (the catalog) and *what does this tier mean* (the registry).
They live in different places, and the most common failure here is editing one while
meaning the other — a change that appears to succeed and silently changes nothing.

## Three registries, not one

Before touching anything, be clear which of these you mean. They do not reconcile with
each other, and nothing warns you when they disagree.

| Registry | Holds | Maintained by | Where |
|---|---|---|---|
| **Node registry** | what each node *is* — RAM, accelerator, installed models, presence | automatic: enrollment + heartbeat | SQLite, via `/nodes` |
| **Capability registry** | what a tier *means* — queue, `model_server`, `model`, prices | hand-edited, needs a restart | `server/fleet.yaml` |
| **Model catalog** | what a node *may install* — candidate artifacts + gate metadata | `POST /catalog`, seeded once | SQLite, via `/catalog` |

The node registry needs no maintenance. The other two do, and for different reasons.

### The disconnect worth knowing

A worker serves **one model** — whatever `CBK_MODEL` says — across *every* queue it
consumes, while `fleet.yaml`'s `model:` is what decides whether a job clears its ability
bar. These are still two separate settings, but they are **no longer unreconciled**:

- The coordinator **pins** the artifact it selected onto the job, and a worker that does
  not have it returns a `failed` result naming it — rather than answering with whatever it
  happens to be running. So the mismatch is now loud instead of silent.
- `GET /nodes` → `capability_warnings` names every tier a node advertises whose model it
  has not got, **before** any job hits it.

So a node advertising `8b-extract` and `32b-reason` while running a 7B will now *fail* the
`32b-reason` jobs rather than answering them badly. The fix is unchanged: a model that fits
the tier, or narrowing the node with `CBK_CAPABILITIES`.

## Which thing do I touch?

| Goal | Touch | Then |
|---|---|---|
| Let the fleet propose a newer model | catalog: `POST /catalog` | run the planner |
| Define a new tier, or change what one serves | `server/fleet.yaml` | restart the coordinator |
| Stop a node serving a tier it can't honour | `CBK_CAPABILITIES` in its `worker.env` | restart the worker |
| Find which nodes serve a tier they can't honour | nothing — `GET /nodes` reports it | fix the model or the tier list |
| Let a job demand tools / a big context / vision | catalog: `POST /catalog` capability fields | clients pass `requires` |
| Re-measure a model whose behaviour changed | `POST /ability/clear?artifact=…` | the harness picks it up |
| Give a node's disk back | let `reclaim` propose it | approve it |

Every call below needs the operator key. It never leaves the coordinator (ADR 26), so run
these **on the coordinator**, not from a worker:

```bash
curl -s -H "X-CBK-Api-Key: $CBK_API_KEY" http://COORDINATOR:8018/catalog
```

## Curating the catalog

The catalog is the set of artifacts the planner may propose. It seeds **only into an empty
table**, so left alone it freezes at whatever shipped — and the planner can only ever
propose what it holds. A fleet with a stale catalog keeps proposing the same vintage
forever, which is why this needs periodic attention rather than one-time setup.

```bash
curl -s -X POST -H "X-CBK-Api-Key: $CBK_API_KEY" \
     -H 'Content-Type: application/json' \
     http://COORDINATOR:8018/catalog -d '{
  "artifact": "<name>:<size>",
  "registry_ref": "<what the model-manager adapter pulls>",
  "size_gb": 9.0,
  "min_ram_gb": 24.0,
  "family": "<family>",
  "params_b": 14.0,
  "quant": "Q4_K_M",
  "expected_ability": 6.5,
  "context_tokens": 131072,
  "supports_tools": true,
  "supports_json_schema": true,
  "supports_vision": false
}'
```

Upserts by `artifact`, so correcting a field is a re-POST, not a delete-and-recreate.

**The two numbers that are load-bearing.** `min_ram_gb` and `size_gb` are gate 1 — they
decide whether a candidate fits the node's RAM and the owner's disk quota. Guess them high
and the model is silently never proposed anywhere; guess them low and you propose a
multi-GB pull onto a machine that can't run it. Get them from the model's actual quantised
size, not from the parameter count.

**The capability fields ARE load-bearing at routing time** (ADR 37), unlike
`expected_ability` below. `context_tokens`, `supports_tools`, `supports_json_schema` and
`supports_vision` say what a model CAN DO rather than how well — a window has a hard edge
and tool calling is a boolean, and no 1–10 score can express either. A job carrying
`requires` is filtered on them *before* ability is compared. Leaving one null is not
"false", it is "not curated": the artifact is excluded from any job needing that feature,
and the 422 names it, so the fix is a re-POST. Get `context_tokens` from the model's
published window, not from the runtime's current setting.

**`expected_ability` is a ranking hint, not a score.** It orders candidates for the
planner. It never routes anything. Routing uses *measured* ability (ADR 15), which a
not-yet-installed artifact doesn't have — so an approved install triggers measurement
rather than inheriting the hint. A wrong hint costs an eval, not a bad route. It is bounded
to the anchored 1–10 scale because an out-of-range value is a typo that would distort every
ranking it takes part in.

### The invariant that makes this safe

**Adding a model never makes it routable.** It becomes a candidate; a human approves an
install; the worker pulls it; the eval harness measures it; only then can it be selected.
A fresh install or a changed digest **never inherits** an ability score — a changed digest
raises a `reeval` proposal precisely so a stale score can't keep driving routing.

This is why curating the catalog is low-risk and worth doing often. You are widening what
the fleet *may* consider, not what it *will* use.

### Driving the proposal loop

```bash
curl -s -X POST -H "X-CBK-Api-Key: $CBK_API_KEY" http://COORDINATOR:8018/proposals/scan
curl -s -H "X-CBK-Api-Key: $CBK_API_KEY" 'http://COORDINATOR:8018/proposals?status=pending'
curl -s -X POST -H "X-CBK-Api-Key: $CBK_API_KEY" http://COORDINATOR:8018/proposals/<id>/approve
```

Three kinds: `upgrade` (a fitting artifact that would raise ability), `reeval` (digest moved
upstream — same name, new artifact), `reclaim` (unused for a month; give the disk back).
Reclaim ships alongside install deliberately — a tool that only ever grows its footprint
stops being a good guest on someone else's machine.

Approval is single-shot; deciding twice is a conflict, not a silent overwrite. Per-node
`auto_approve` is opt-in, and reclaim is never auto-approved.

## Refining capability tiers

Tiers live in `server/fleet.yaml` under `capabilities:`. Each is a queue, an OpenAI-compatible
endpoint, a model, and cloud-equivalent prices for the avoided-spend headline. The name is a
human convention — nothing parses `32b-reason`.

Adding a tier means adding it there and restarting. Until then a node can advertise it, heartbeat
happily, and never receive a single job, because routing resolves against the registry.

### When a tier actually earns its existence

Resist adding tiers because a node *could* serve one. A tier is justified when there is
demand it serves better than an existing tier — which means evidence, not intuition:

```bash
curl -s -H "X-CBK-Api-Key: $CBK_API_KEY" http://COORDINATOR:8018/ability   # measured, per task class
curl -s -H "X-CBK-Api-Key: $CBK_API_KEY" http://COORDINATOR:8018/usage     # what is actually being asked for
curl -s -H "X-CBK-Api-Key: $CBK_API_KEY" http://COORDINATOR:8018/queues    # where work is piling up
```

Ask: is some task class under-served at its current tier? Is a queue backing up while
another idles? Does a node's measured ability actually differ enough from the incumbent's
to be worth a separate address? If ability is unmeasured and usage is empty, there is
nothing to refine *from* — say so plainly rather than producing plausible-looking tiers.
A single-node fleet with no eval history does not need tier work; it needs measurements.

Node capability *proposals* (`coordinator.py` → `propose_capabilities`) budget against
**VRAM where the node reports it**, falling back to system RAM where it does not, and stop
one tier short on a CPU-only node. So a 64 GB Mac reporting 48 GB of wired-down unified
memory is proposed two tiers, not three — the correction, not a regression: the third tier
was a capability it could only serve by swapping. A pre-0.9.0 worker sends no `vram_gb` and
still gets the old RAM-based proposal. It remains a starting default the owner edits, not a
judgement; measured throughput and the catalog are still not weighed.

## When a node enrols but never gets work

This is the signature failure, and it looks healthy from every angle — enrolment succeeds,
heartbeats are green, `fitness: ok`. Work through it in this order:

1. **Is the tier in the registry?** `GET /fleet`. A tier the node serves but the coordinator
   doesn't define can never be selected. The join warns about this; it's easy to miss.
2. **Can the coordinator reach the node's model server?** The sync plane calls `model_server`
   *directly* — the worker isn't in that path. A model server bound to loopback serves
   `POST /jobs` perfectly while being invisible to `/v1/chat/completions`.
3. **Has the artifact been measured?** `GET /ability`. Need-shaped routing
   (`task_class` + `min_ability`) can't select an unscored artifact. `GET /eval` shows what's
   still pending; `POST /eval/run` forces a pass.
4. **Does the node serve a tier whose model it hasn't got?** `GET /nodes` →
   `capability_warnings`. The registry's `model:` is what clears a job's ability bar, and the
   coordinator now pins it on the job — so a node advertising a tier it cannot serve FAILS
   those jobs with a reason rather than answering with whatever it is running. Fix it with a
   model that fits the tier, or a narrower `CBK_CAPABILITIES`.
5. **Are the artifact's capabilities curated?** `GET /catalog`. A job carrying `requires`
   (context window, tools, JSON schema, vision) skips any artifact that does not *declare*
   the feature, whether or not it has it.
6. **Is the node paused, or below its presence ladder?** `GET /nodes`. Note the enrolment
   proposal only serves the FIRST tier while the owner is `active` — a multi-tier node
   leaves its other tiers unconsumed until it goes `away`.

## Keep it domain-agnostic

clusterbuck is Apache-2.0 infrastructure, shared publicly, and knows nothing about the
applications using it. When editing anything here — including examples in docs or configs —
never introduce real hostnames, client application names, domain concepts, or employer
names. Use generic placeholders (`COORDINATOR`, "an always-on ~16 GB node"). A leak of that
kind is a bug, not a cosmetic issue.
