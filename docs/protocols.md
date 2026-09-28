# Protocols

Every boundary in clusterbuck is a documented protocol, which is what keeps the system
polyglot and open (see [design.md](design.md) → *Decisions worth not undoing*). This document specifies each seam. The shapes below describe the **implemented** system;
where a machine-readable schema exists in [`contract/`](../contract/) that schema is
authoritative and this document is its prose companion.

## 1. Client ↔ clusterbuck

Two entry points, both HTTP; a client picks one per request, by whether a person is
waiting — not by what the work is. The two are addressed differently on purpose:

- **Sync targets a ready capability.** `model` is a capability alias, and it binds the
  deployment behind that alias as it stands now. `model: "30b-reason"` means "that tier",
  not "give me reasoning". Nothing is routed by ability, filtered by `requires`, queued or
  woken; a client choosing an alias reads `/fleet`'s `features` and `health` (§9).
- **Async declares an intended outcome.** `task_class` + `min_ability` + `requires` say
  what the work needs, and the coordinator chooses the tier, waits for it, and wakes it.

A request is four separate things, and keeping them apart is what lets routing stay
domain-agnostic: **intent** (`task_class`, `min_ability`, `urgency`, `privacy`,
`deadline` — what service the work needs), **requirements** (`requires` — hard technical
facts a model must have), **params** (generation settings), and **messages** (the task and
its data). Why the client is asking, and how the answer should be framed, belong in the
messages; clusterbuck routes on the first two and never reads the last.

A reference client that does all of this — including the durable-job lifecycle below —
is [`examples/client/cbk_client.py`](../examples/client/cbk_client.py). It is run by the
end-to-end suite, so it cannot drift from what the coordinator does.

### 1a. Sync (interactive) — OpenAI-compatible
Standard OpenAI Chat Completions, served by the LiteLLM gateway:

```
POST /v1/chat/completions
{ "model": "<capability-alias>", "messages": [...], "temperature": ..., "max_tokens": ... }
```

The `model` alias maps to a capability tier in the gateway config. Response is the
standard OpenAI shape. Any OpenAI-compatible SDK works unmodified — including its errors,
which arrive in OpenAI's envelope with a stable `code` (§1c).

Every completion is metered (metadata only) under an id of its own, attributed to the
deployment that answered. A tier's `cloud_fallback` (§5) is the provider LiteLLM falls
back to when that tier's model server fails. While the cloud budget is spent, no call can
reach a provider: a request naming a cloud tier is refused with `cloud_budget_exhausted`
(422, as on the job API), and a local tier is served with its cloud fallback removed.

A failed call is **not retried behind the client's back**. LiteLLM's default of two
silent retries meant a dead model server cost an interactive caller three connection
attempts before it heard anything; the refusal now says whether it is `retryable`, and the
client — which knows whether a person is waiting — decides.

### 1b. Async (patient) — job API
Submit a job:

```
POST /jobs
{
 // Addressing — one of the two forms:
 "task_class": "summarize", // preferred: describe the need…
 "min_ability": 6, // …and the coordinator resolves an artifact
 // (see design.md for the 1-10 scale)
 "capability": "32b-reason", // OR name a supply-side tier explicitly (advanced)
 "requires": {                      // optional hard requirements (ADR 37), applied as a
   "context_tokens": 60000,         //   FILTER BEFORE ability is compared — see below
   "tools": true,
   "json_schema": true,
   "vision": false
 },
 "messages": [ {role, content}, … ],// OpenAI-style; or "prompt"
 "params": { "temperature": 0.2, "max_tokens": 1500, "response_format": "json_object" },
 "urgency": "waitable", // urgent | necessary | waitable — a trajectory:
 "escalate_after_min": 10, // waitable = eager but non-demanding; N is a
 // patience bound (not a delay) — unserved
 // after N min it becomes necessary (see
 // design.md → urgency & escalation)
 "privacy": "local_only", // local_only | cloud_ok (default local_only —
 // local_only NEVER routes to cloud). A
 // cloud_ok job with wake rights goes to the
 // cloud if its local tier cannot be served:
 // at submit when nothing can serve it, or
 // near its `deadline` if nothing came
 // (design.md §8)
 "deadline": "2026-01-01T00:00:00Z",// optional expiry; see expires_at below.
 // Enforced by a coordinator sweep, which
 // withdraws an unclaimed entry and marks the
 // job `expired`. The worker does not check
 // it: a job already claimed runs on, and its
 // late result is discarded — `expired` is
 // final.
 "submitter": { // optional caller provenance (all fields optional)
 "app": "nightly-importer", // which client
 "instance": "workstation-2", // on which machine
 "request_id": "req_4f9c1e70a2", // one LOGICAL call — see below
 "submitted_at": "2026-01-01T00:00:00Z" // client's own clock; ADVISORY only
 }
}
→ 202 Accepted
{ "id": "job_…", "result_key": "res_…", "status": "queued" }
```

Both addressing forms **fail explicitly** (`422`) at submit time rather than queuing a job
nothing will ever serve: `task_class`/`min_ability` when no artifact clears the ability bar
(design.md), and `capability` when the name is not in the fleet registry — a
typo'd capability would otherwise sit on a stream no worker consumes, with no error and no
expiry short of `deadline`.

Every refusal here is a `422`, told apart by its `error.code` (§1c): `ability_unsatisfied`,
`requirements_unsatisfied`, `capability_not_found`, `cloud_not_permitted` (the job's
privacy or urgency forbids the named cloud tier; `reason` says which) and
`cloud_budget_exhausted`. The status is deliberately the same for all of them — the code
is what names the fix.

Speed is never a reason to refuse. It **orders** the tiers that clear the bar — local
first, then the soonest answer, then the cheapest (design.md §4) — but a job is never
rejected, and a node never turns one down, for being slow. A tier whose nodes have measured
*nothing* is ordered on price alone: unknown is not slow, and a fresh fleet has finished no
jobs.

**What a model CAN DO is a filter, not a score (`requires`).** Ability is a graded 1–10
judgement of how *well* a model does a task class. These are not that shape: a context
window is a number with a hard edge, tool calling is a boolean, and a 4k-context model and
a 128k one can both honestly be "a 6 at summarize" — so the matrix cannot tell them apart,
and a 60k-token document routed to the first is silently truncated. So `requires` is
applied as a **hard filter before ability is compared at all**; otherwise a
capable-but-unsuitable artifact wins on score and the requirement is decided by an
unrelated number.

It applies to **both** addressing forms. Naming a `capability` explicitly is the advanced
form, not a bypass — the same reason privacy and budget are enforced there.

**Undeclared reads as "no"**, and that is deliberately the opposite of how speed is
treated. An unmeasured *speed* costs at most a slower answer, and corrects itself as the
node finishes jobs and reports what it measured. An undeclared *feature* has no such backstop:
serving a tool-calling job on a model nobody has checked either fails at the model server,
where it reads as a model bug rather than a routing one, or succeeds while quietly
ignoring the tools. A local artifact declares these in the **model catalog**; a provider
account, which has no host node and never enters that catalog, declares them on its
`fleet.yaml` capability.

The refusal is a `422` **naming the artifact and the missing feature**, and is reported
separately from an ability miss because the two call for different fixes: a missing
declaration is usually one `POST /catalog` away, since the model can very often do the
thing and simply has not been recorded as able to. A missing *ability* needs a better
model.

`context_tokens` is **declared by the client, not inferred from the prompt.** The
coordinator does not estimate how many tokens a submission will occupy: a character-based
guess would start refusing valid work on a heuristic, and a real tokenizer is per-model
and not a dependency this project carries. A client that knows its document is large says
so. Auto-deriving a floor is a reasonable future addition; guessing one silently is not.

**`requires.tools` selects a model; it does not run tools.** It filters routing to models
declared able to emit tool calls, and `params.tools` / `tool_choice` are forwarded to the
model server. That is all. A job is one completion: any `tool_calls` the model emits come
back in `result` exactly as returned, and nothing resumes the job with tool results. A
tool loop is the client's — run the tool, then submit a *new* job whose `messages` carry
the call and its result. Routing a job to a tool-capable model grants that model no
authority to do anything; the client decides which calls to honour.

`requires: {"vision": false}` is not a requirement — it means "I do not need vision", and
must not exclude a model that happens to have it. Only truthy values filter.

**`response_format` is forwarded only when the job requires `json_schema`.** The field was
dropped unconditionally, for a sound reason: local servers do not reliably honour it, and
a model that ignores the flag hands prose to a caller who asked for JSON — a silent wrong
answer, worse than an explicit refusal. But that left the extraction tiers unable to ask
for the one thing they exist to produce, and the two planes disagreeing, since the sync
plane forwarded it. `requires.json_schema` settles it: it is a hard requirement routing
filters on, so by the time the job reaches a worker the pinned artifact is declared
capable of honouring the flag — which is exactly the check whose absence justified
dropping it. Without the requirement it remains a dropped hint. Set both: the requirement
to gate routing, and `params.response_format` to say what shape you want (default
`{"type": "json_object"}`).

**The resolved artifact is pinned on the job** (`params.model`). A capability is only a queue
name: the worker draining it answers with its own `CBK_MODEL`, for every capability it
serves, so without the pin the model whose ability cleared the bar and the model that ran the
job were unrelated. A client-supplied `params.model` is overwritten — otherwise any caller
could name a stronger model on a cheaper tier and `min_ability` would enforce nothing. A node
that lacks the pinned artifact returns a **failed** result naming it, rather than
answering with something else.

**Caller provenance (`submitter`).** A queue holding N byte-identical payloads cannot say
whether it is one logical call retried N times (a client retry loop) or N genuinely repeated
calls — and the difference decides whether the fix is on the client or the fleet.
`request_id` is that discriminator: **two jobs sharing a `request_id` are the same call
retried; two jobs with identical content but different `request_id`s are real repeated
work.** `app` and `instance` name the caller.

It is **identification only**. The coordinator never dedupes, collapses, reorders or rejects
on these fields, and `request_id` is deliberately not unique-constrained — a client reusing
one is *describing a retry*, which is the signal, not a constraint to enforce. Every field is
optional, so clients predating it keep working unchanged.

Two properties worth stating because they are easy to get wrong:

- `submitted_at` is **advisory** — useful only as a clock-skew / queue-delay signal.
 `created_at`, stamped server-side at receipt, remains authoritative for every ordering,
 escalation and metering decision.
- `observed_ip` is stamped **server-side** from the connection and is rejected (`422`) if sent
 in the body: a client can misreport its own name but not the address it dialled from. It is
 therefore the only provenance that attributes a flood from a client sending none at all —
 the case that motivates the whole block. Being coordinator-side only, it is **not** in
 `contract/job.schema.json`; a worker has no use for it.

Poll for the result:

```
GET /jobs/{id}
→ { "id","status":"queued|running|done|failed|expired|cancelled",
 "urgency","capability",
 "created_at": "…", // server receipt time — authoritative
 "started_at": "…" | null, // a claim was OBSERVED at this time (see below)
 "finished_at": "…" | null, // a terminal status was first recorded at this time
 "deadline": "…" | null, // your `deadline`, echoed back
 "escalates_at":"…" | null, // when waitable gains the right to demand capacity
 "expires_at": "…" | null, // effective give-up time; NULL = nothing ever gives up
 "queue_position": 0 | null, // unclaimed jobs ahead of you; null unless queued
 "result": { … OpenAI-style completion … } | null,
 "error": null, "error_code": null, // why it did not complete — see below
 "retryable": null | true | false, // CODES[error_code]; null iff error_code is null
 "attempts": 0, "worker": "<opaque-node-id>" | null,
 "submitter": { "app","instance","request_id","submitted_at",
 "observed_ip" } | null } // provenance as recorded; null if none
```

The client never learns which machine ran the job beyond an opaque id (diagnostics only).

`capability` is the tier the job is on **now**, which is not always the one it was sent
to. A `cloud_ok` job is moved to a cloud tier when its local tier cannot be served,
either at submit or by the rescue sweep near its `deadline` (design.md §8). A job that
ran in the cloud reports `worker: "cloud:<provider>"`.

**Why a job did not complete: `error_code`.** `error` is free text for a person;
`error_code` is the stable code a client branches on (§1c). It is set on every terminal
status but `done`:

| `error_code` | Status | Meaning | What a client does |
|---|---|---|---|
| `job_expired` | expired | Its `deadline`, or the maximum queue age, passed unserved. | Report it; resubmit (fresh key) only if still wanted. |
| `job_cancelled` | cancelled | Withdrawn by `DELETE`. | Nothing. |
| `job_orphaned` | failed | Its queue entry is gone and cannot be rebuilt. | Resubmit under a **fresh** key. |
| `job_abandoned` | failed | Workers kept dying on it until the reaper gave up. | Resubmit (fresh key), perhaps later. |
| `capability_misconfigured` | failed | A provider account with no usable key. | Operator fix. |
| `model_server_*`, `model_request_rejected` | failed | The executor's model server failed, as on the sync plane. | Per `retryable`. |
| `artifact_not_installed` | failed | The node that claimed it lacks the artifact routing pinned. | Operator fix; resubmit (fresh key) once fixed. |
| `model_substituted` | failed | The model server answered with a different model than the one pinned. | Operator fix (usually an LM Studio load). |
| `worker_failed` | failed | Failed without a classified cause — a worker that predates error codes, or an unexpected error. | Treat as not retryable. |

**`retryable` rides beside `error_code`** (`CODES[error_code]` from
[`contract/error-codes.json`](../contract/error-codes.json)), the identical table §1c
already uses for HTTP errors — so a client branches on the job the same way it branches
on a refused submit, rather than keeping a second, driftable copy of "which codes are
worth trying again" for the job plane. It means exactly what it means there: the
identical payload may succeed if simply resubmitted soon, nothing about whether a
resubmit is the right ACTION — `job_orphaned` is `false` (the failure will not clear
itself) yet the table still says resubmit under a fresh key, because that is the only way
to recover a payload nothing kept a copy of. `null` iff `error_code` is `null`.

A resubmit always takes a **fresh** idempotency key: a key has no TTL, so the old one
returns the old, terminal job forever. The same key is only for a submit whose response
was lost. Like `result`, a `failed` job's `error_code` is `null` once its result has
passed `CBK_RESULT_TTL_S` — persist both on first sight. A `null` on `expired` or
`cancelled` never happens: those are the coordinator's own decisions and are derived from
the status.

**Reading the wait state.** Four fields are easy to misread, so each is stated exactly:

- **`attempts` counts reaper requeues, not deliveries.** It is incremented only when the
 coordinator concludes a worker abandoned a job and puts it back (`reaper.py`), so a
 healthy job that runs once reads `0` for its whole life. It is **not** a "has anything
 picked this up" signal, and never was.
- **`started_at` and `worker` are coordinator observations, on the tick interval.** They
 come from the stream's pending-entries list, which knows the claiming consumer before
 any result exists — so a *running* job now reports both. But pending entries vanish on
 `XACK`, so a job claimed and finished between two ticks is never seen there and ends up
 with `finished_at` set and `started_at`/`worker` null. **`started_at == null` means "no
 claim was observed", never "not started".** A wait state should key on `status` and
 `queue_position`, which are correct in every case.
- **`status` reaches `running`** when such a claim is observed. `started_at` set *with*
 `status: queued` is meaningful rather than contradictory: a worker had the job and died,
 and the reaper has put it back.
- **`queue_position` counts unclaimed work ahead of you.** Jobs already claimed are in
 flight, not ahead in the queue, so a position of `0` is compatible with one job still
 generating. It is capped, so past the cap it means "at least N", and it is `null` once
 the job is no longer queued.

**When clusterbuck gives up: `expires_at`, and the honest `null`.** A `waitable` job
submitted with neither `deadline` nor `escalate_after_min` has **no terminating mechanism
of its own** — it is excluded from escalation (no `escalate_at`), from the deadline sweep
(no `deadline`), and it is invisible to the reaper, because `XAUTOCLAIM` walks the
pending-entries list and a never-delivered entry never enters it. `expires_at: null`
reports exactly that, and is the reason to set `deadline` and/or `escalate_after_min` on
anything a human is waiting for: such a job waits for a capable worker indefinitely, which
on a fleet that sleeps for days is the correct behaviour rather than a fault.

What it no longer does is **vanish**. Its entry can still be trimmed away by `MAXLEN ~`
once enough later traffic arrives on the same capability, but that is now answered rather
than silent: the orphan sweep looks for the entry a job's row names and, finding it gone
with the job still queued, writes a terminal `failed` saying so. Previously such a job
kept polling `queued` for a result that no longer had any way to arrive — it retained its
`entry_id`, so the sweep's `entry_id IS NULL` filter skipped it, and it had never entered
a pending list, so the reaper could not see it either.

A `deadline` that is not an RFC 3339 timestamp is rejected with `422`. It used to be
dropped silently to "no expiry", which handed back the opposite of what was asked for with
nothing to notice.

**Retrying a submit safely: `Idempotency-Key`.** `POST /jobs` mints a new job per call,
so a client that loses the response (a killed request, a dropped connection) cannot tell
"submitted" from "not submitted" and has no safe retry. Send an opt-in header:

```
POST /jobs
Idempotency-Key: <opaque, unique per logical call>
→ 202 { "id","result_key","status":"queued" } // first time
→ 200 { "id","result_key","status":"<current>" } // a repeat
 Idempotency-Replayed: true
```

A repeat returns the **existing** job and enqueues nothing. `200` rather than `202`
(nothing was accepted for processing this time) and rather than `409` (the client asked
for at-most-once and got exactly that — this is success); the body carries the same three
keys either way, so a client that checks neither the status code nor the header still
parses the reply and polls the right job. `status` is the job's *current* status, so a
client retrying long after the original may be told `done`.

Details worth knowing:

- **A rejected submit does not burn the key.** A `400`/`422` means no job exists, so the
 same key remains usable — otherwise a client that submitted before any artifact cleared
 its ability bar could never retry.
- **No request fingerprint.** A key reused with a *different* payload returns the first
 job; the key is the client's promise that two requests are the same call. A
 canonicalised-body hash is a deliberate deferral, not an oversight.
- **No TTL.** The key lives on the job row, so its retention is the job row's. Keys must
 be unique per logical call — a UUID or a content hash, never a recycled counter.
- Keys are at most 255 printable ASCII characters, trimmed of surrounding whitespace.
- This is **not** `submitter.request_id`, which is identification only and never enforced
 (a reused one there *describes* a retry). Enforcement is opt-in and separate. Like
 `observed_ip`, the key is coordinator-side and is not in `contract/job.schema.json` — a
 worker has no use for it.

**Withdrawing a job: `DELETE /jobs/{id}`.** Best-effort, and explicit about which:

```
DELETE /jobs/{id}
→ 200 { …the same body GET /jobs/{id} returns, with status: … }
```

- **`cancelled`** — the work **provably** never ran and never will: the queued entry was
 removed and no consumer held it.
- **`cancelling`** — a worker already has it. A model call in flight cannot be
 interrupted, so that run finishes and its result is discarded. The job is still
 terminalised, so the client stops waiting either way.
- Already-finished jobs are returned unchanged; the result is checked before the stream,
 because `XACK` leaves an entry in place and a job that already ran is otherwise
 indistinguishable from one never delivered.

`cancelled` is a **coordinator** status: no worker can produce one, so it is deliberately
absent from `contract/result.schema.json`'s enum and appears only in the assembled client
view above. Any holder of the operator secret can cancel any job — `client_key` is an
opaque label, not an authenticated identity, and per-client keys were declined for
LAN-only single-operator infrastructure. Accepted limitation, stated rather than hidden.

**Terminating backstops.** Two coordinator sweeps, because only one of them can be policy:

- **Orphan sweep** (`CBK_ORPHAN_GRACE_S`, default 900s, always on) — a job whose row was
 committed but whose queue write never happened, because the coordinator died in between.
 Nothing will ever deliver it. It is answered `failed`, not resubmitted: the payload is
 not stored in columns and cannot be rebuilt, and inventing one would run something the
 client never asked for. A client retrying under an idempotency key should use a fresh key.
- **Maximum queue age** (`CBK_MAX_QUEUE_AGE_S`, **unset ⇒ disabled**) — a properly-queued
 job nobody ever claimed, answered `expired`. Off by default deliberately: on a fleet
 whose machines sleep for days, a patient `waitable` job outliving a fixed cutoff is
 *correct*, and clusterbuck will not impose a timeout on someone else's backlog.

So with the default configuration **the only bounds on a job are the ones its client
sets** — which is exactly what `expires_at: null` reports.

**The client's half of the lifecycle**, in order — `examples/client/cbk_client.py` is this,
runnable:

1. Mint one `Idempotency-Key` per *logical* call, and persist it with the request before
   sending.
2. On `202`/`200`, persist `{id, result_key}` against that key before doing anything else.
3. A transport failure OR a 5xx on submit is ambiguous either way: retry with the
   **same** key.
4. Poll `GET /jobs/{id}` until a terminal status; persist `result`, `error_code` and
   `retryable` the first time you see them.
5. Branch on `error_code`/`retryable`. To run the work again, submit under a **fresh** key.
6. Stop waiting with `DELETE /jobs/{id}`.

**Collecting a result later.** A completed result lives in the result store for
`CBK_RESULT_TTL_S` (default 24h). After that, `GET /jobs/{id}` still answers with the
job's terminal status and timing, but `result` is `null` — `finished_at` is what
distinguishes "it ran and the result expired" from "it never ran". Prompts and completions
are deliberately not kept in the coordinator's durable store (metering is metadata-only),
so a client that needs a result beyond the TTL must persist it on first successful poll.

### 1c. Errors — the envelope, and codes to branch on

Every client-facing error is sent in one shape, OpenAI's, so the stock SDK surfaces the
code as `.code` on the exception it raises:

```
{ "error": { "message": "…",               // for a person; may change freely
             "type": "invalid_request_error", // | authentication_error | api_error
             "code": "capability_not_found",  // what a client branches on
             "retryable": false,              // the identical request may succeed soon
             "param": "model",                // the field at fault, when there is one
             "capability": "…", "model": "…"  // the alias sent / the artifact resolved
           },
  "detail": "…" }                             // DEPRECATED: the message again
```

**Branch on `code`.** The HTTP status is coarse — a `502` is both "the model server is not
there" and "it answered with an error", which call for different messages to a user — and
the message is prose that will be reworded. `code` is the contract.
[`contract/error.schema.json`](../contract/error.schema.json) pins the envelope and
[`contract/error-codes.json`](../contract/error-codes.json) the full vocabulary, each code
with its meaning and whether it is retryable.

**`retryable` means the identical request may succeed if simply retried soon.** It is
about transient conditions — a server restarting, a connection dropped. It never means
"could succeed once an operator changes the fleet": an ability miss is `retryable: false`
even though adding a model would fix it.

`detail` carries what it always did (the message, or pydantic's list for a validation
error), so a client written before the envelope keeps working. New clients should not read
it.

What the coordinator raises on HTTP (a job's terminal codes are in §1b, *Why a job did
not complete*):

| Code | Status | Retryable | When |
|---|---|---|---|
| `invalid_request` | 400 / 422 | no | A malformed body, header or field; `param` names it. |
| `unauthorized` | 401 | no | Missing or wrong API key. |
| `not_found` | 404 | no | An unknown job or reservation id. |
| `fleet_not_configured` | 503 | no | No fleet registry, so nothing can be routed. |
| `capability_not_found` | 404 (sync) | no | The alias is not in the registry — a typo, not an outage. |
| `capability_misconfigured` | 503 | no | The alias is registered but cannot be served, e.g. a provider account whose key is unset. An operator fix. |
| `reservation_invalid` | 400 | no | The named reservation is unknown or unconfirmed. |
| `capability_not_found` | 422 (submit) | no | `POST /jobs` named a capability that is not registered. |
| `ability_unsatisfied` | 422 | no | Nothing clears `min_ability` for the task class. A better model, or a lower bar. |
| `requirements_unsatisfied` | 422 | no | Nothing declares what `requires` asks for — usually one `POST /catalog` away. |
| `cloud_not_permitted` | 422 | no | The named cloud tier is forbidden by the job's own privacy or urgency (`reason`). |
| `cloud_budget_exhausted` | 422 | no | The named cloud tier is over budget. |
| `model_server_unreachable` | 502 | yes | The model server could not be connected to. |
| `model_server_timeout` | 504 | yes | It did not answer in time. |
| `model_server_error` | 502 | yes | It answered with a server error or a malformed response. |
| `model_request_rejected` | 400 | no | It refused the request itself, e.g. the prompt exceeds its context window. |

On `model_server_unreachable` and `model_server_timeout`, `use_async: true` says the async
plane could still serve the capability: a worker is consuming its queue — a worker can
reach its own model server when the coordinator cannot — or a registered node serving it
can be woken. Absent or false, a `POST /jobs` would be waiting on nothing in particular.

`model_server_*` is decided from the failure's **type**, never its text — and not from the
outermost type alone, because LiteLLM reports a refused connection as an
`InternalServerError` wrapping the `ConnectError` that actually happened. Read naïvely,
every dead server would be a broken one.

## 2. clusterbuck server ↔ worker (Redis queue contract)

The server and the reference worker communicate **only** through Redis. Any worker in
any language that honours this contract can join the fleet — this section is the spec you
would write one from, so it states the concrete names and commands rather than intent.

Implemented on **Redis Streams + consumer groups** , *not* plain lists: the
pending-entries list and `XAUTOCLAIM` provide the visibility-timeout / at-least-once
semantics natively.

- **Two streams per capability :** `q:<capability>:urgent` and
 `q:<capability>`. The job JSON is the single field **`job`** of each entry; the server
 appends with `XADD <stream> MAXLEN ~ <cap> * job <json>`. Tier is a pure function of
 urgency — `urgent` and `necessary` take the urgent stream, `waitable` the base one — and
 a consumer **reads the urgent stream first, the base stream only if it was empty**. That
 read order is the whole mechanism: it is what makes urgency order work rather than only
 decide whether a machine gets woken.
 A worker must still take at most one job per capability per pass, so a busy urgent
 stream cannot starve another capability the same node serves.
 The base stream keeps its historical name, so a coordinator that does not tier and a
 worker that does interoperate in both directions. The coordinator only writes to the
 urgent tier once every node that is **currently reporting queues** for that capability
 names an urgent stream among them (`CBK_URGENT_STREAMS=auto|on|off`) — a worker built
 before tiering reads only the base stream, and an urgent-tier write would strand the
 job. A node reporting *no* queues abstains rather than vetoing: `queues` is what a node
 says it is claiming from right now, so it is empty whenever the node is paused or cut
 off from the broker, and neither says anything about what it could read on resuming. If
 no node is reporting any, the gate stays shut.
- **Consumer group:** one shared group named **`cbk-workers`** (`CBK_CONSUMER_GROUP`) per
 stream, created with `XGROUP CREATE … $ MKSTREAM` (tolerate `BUSYGROUP`). Every worker
 joins the *same* group, so each job is delivered to exactly one of them.
- **Claiming:** `XREADGROUP GROUP cbk-workers <worker-id> COUNT n STREAMS <stream> >`,
 urgent tier first (above).
 Note the read is **non-blocking** — no `BLOCK` argument — and the worker sleeps between
 polls instead: it has a pause flag to notice and a job to be able to abandon, so it must
 come back to its own loop. A worker MUST NOT block here. Its broker socket carries a
 read timeout precisely because no legitimate operation on this path outlasts one (see
 the worker's `broker.py`), and a blocking read would trip it every time.

 ```
 {
 id, created_at,
 capability,
 messages | prompt,
 params, # forwarded to the model server; see the `model` pin below
 urgency, escalate_after_min, privacy, deadline,
 result_key,
 attempts, max_attempts,
 submitter? # optional caller provenance (§1b); parse-and-ignore is fine
 }
 ```

- **`params.model` is a REQUIREMENT, not a hint.** If present it names the exact artifact the
 job must run on, overriding the worker's configured default model. The eval harness relies
 on it to attribute a measurement to the artifact under test; a worker that ignores it will
 silently mis-attribute scores.
- **Completion:** write the result (below), then `XACK q:<capability> cbk-workers <entry-id>`.
 Acknowledge on failure too, having written a `failed` result — an unacked entry is
 indistinguishable from an abandoned one. Acknowledge even when the result write was
 refused (see *Idempotency*): a refused write means the job already has an answer, and
 leaving the entry pending only gives the reaper something to reclaim.
- **Release:** a worker may abandon a claimed entry without answering it — the owner
 pausing the node cancels the in-flight call (protocols.md is silent on *why* a worker
 stops; it need not say). Write no result and do not ack: an interrupted job has not
 failed, and leaving the entry pending is what returns it through the recovery path
 below. Answering it with `failed` would make a retryable job terminal.
- **Recovery:** entries left pending longer than the coordinator's idle threshold
 (`CBK_REAPER_MIN_IDLE_MS`, default 10 min) are reclaimed with `XAUTOCLAIM`, re-appended to
 the stream with `attempts` incremented, and the stale delivery acked away. Past
 `max_attempts` the coordinator writes a terminal `failed` result instead. This is what makes
 a laptop closing its lid mid-job safe. The threshold necessarily exceeds the longest
 plausible inference, because a worker mid-generation is not reading from Redis.
 **The reaper only ever sees entries that were delivered**, because `XAUTOCLAIM` walks
 the pending-entries list — a job no worker ever claimed is not in it, and is therefore
 invisible to this recovery path at any threshold. That gap is what `expires_at` reports
 to a client (§1b); closing it is a coordinator-side sweep over queued rows, not
 something the reaper can be tuned into doing.
 A requeue is a **new stream entry**, so the coordinator re-records the job's delivery
 (and returns its status to `queued`) — anything holding the old entry id, such as queue
 position, would otherwise point at an entry about to be acked away.
- **Result:** written to a plain Redis key named by `result_key`, with a TTL
 (`CBK_RESULT_TTL_S`). Authoritative schema:
 [`contract/result.schema.json`](../contract/result.schema.json) — it *requires*
 `{job_id, status, worker, completed_at}` and nests the OpenAI-shaped body under
 **`completion`**, with optional `usage`. `status` is `done | failed | expired`. A
 non-`done` result carries `error` (text) and, from any writer that knows it, `error_code`
 — a job-plane code from [`contract/error-codes.json`](../contract/error-codes.json),
 which the schema enforces.
- **Idempotency: FIRST WRITER WINS.** Delivery is at-least-once, so a job may legitimately
 run twice — and the two copies can overlap by hours, because a node that sleeps
 mid-inference is not dead: the reaper hands the work on, and the sleeper still finishes
 when it wakes. An executor therefore writes its result **only if the key is absent**
 (`SET result_key … NX EX <ttl>`) and treats a refusal as "already answered elsewhere",
 discarding its copy. Terminal must mean terminal: an unconditional write let a client be
 told `failed` and later find `done`, with no way to know which it had acted on.
 The coordinator's own terminalising writes — the reaper's dead-letter past `max_attempts`,
 and the queued-row sweeps — are unconditional by design, because they exist to write an
 answer where nothing else will; the reaper checks for an existing result first, and skips
 re-running a reclaimed entry that has one.

## 3. Worker ↔ model server (OpenAI HTTP)

The worker calls the model server running on its own machine over the OpenAI-compatible
API (`POST /v1/chat/completions`). This is why the model server can be Ollama, llama.cpp,
vLLM, or LM Studio interchangeably — the worker only speaks the wire protocol, never a
vendor SDK. Local models don't reliably honour structured-output flags, so JSON-shape
requests are made in the prompt and validated by the client, not assumed here.

If that model server sits behind an authenticated gateway, `CBK_MODEL_SERVER_API_KEY` (node
env) sends `Authorization: Bearer <key>` on every call. This is node-local config, the same
trust boundary as `CBK_MODEL_SERVER_URL` — it is **not** how a registered cloud provider
account is reached (§5): those keys live only on the coordinator, which calls the
provider itself rather than ever handing a worker that key. A job's own `params` can never
supply or override `api_key`/`api_base` (§2's envelope rule, same as
`stream`/`messages`).

A failed call becomes a `failed` result whose `error_code` is classified by exception
type (`worker/src/cbk_worker/failure.py`): no connection → `model_server_unreachable`, no
answer in time → `model_server_timeout`, a 5xx or a body that is not JSON →
`model_server_error`, a 4xx → `model_request_rejected`. A 4xx is the server refusing
*this* request — a context window, a parameter it does not support — which a user should
be told differently from the server being broken.

## 4. Coordinator ↔ node (Wake-on-LAN)

Waking a sleeping machine is a pure network action, needing no software on the target:

- The coordinator sends a **Wake-on-LAN magic packet** to a node's MAC address.
- Trigger conditions: a `q:<capability>` has depth and a capable machine is asleep on the
 LAN, or a scheduled-wake window opens.
- The target must have "wake for network access" enabled and be reachable on the LAN;
 off-LAN machines cannot be woken (see [design.md](design.md)).

## 5. Model / capability registry

Which node hosts which models, its MAC (for WoL), and its capacity. Starts as static
config; with a live fleet (see [design.md](design.md)) the same shape is the seed for a coordinator-held **dynamic registry** maintained by
enrollment + heartbeats, which additionally tracks *installed vs currently loaded* models,
profile, and presence mode per node.

```yaml
# fleet.yaml
nodes:
 - id: node-a
 mac: "aa:bb:cc:dd:ee:ff" # for Wake-on-LAN
 wake: on-demand # opportunistic | scheduled | on-demand
 capabilities: [8b-extract]
 - id: node-b
 mac: "11:22:33:44:55:66"
 wake: opportunistic
 capabilities: [8b-extract, 32b-reason, 70b-reason]
capabilities:
 # No `queue:` — the stream is DERIVED from the capability name (`q:<capability>`, above).
 # A worker builds its stream names from its own capability list and never reads this
 # file, so a name declared here could only disagree with the one actually in use. A
 # declared value is still accepted for older files, but it must match or the fleet
 # refuses to load.
 #
 # `description` (optional) is display-only: what the dashboard shows beside the tier name,
 # since the name is a terse routing key. Nothing routes on it.
 8b-extract: { model_server: "http://localhost:11434/v1", model: "…",
               description: "Quick structured work: fields into JSON, tagging" } # → q:8b-extract
 # `cloud_fallback` (optional) names a registered provider account (below): where a
 # `cloud_ok` job addressed to this tier by name goes when no machine can serve it, and
 # the tier's sync-plane fallback. It must name a `cloud: true` capability with no
 # `model_server` in the same file, or the fleet refuses to load.
 32b-reason: { model_server: "http://localhost:11434/v1", model: "…",
               cloud_fallback: claude-sonnet } # → q:32b-reason
 70b-reason: { model_server: "http://localhost:11434/v1", model: "…" } # → q:70b-reason

 # A registered provider account : no `model_server` — it has no host node, so
 # the coordinator calls it directly instead of dispatching to a worker. `model` is a
 # LiteLLM "<provider>/<model>" id; `api_key_env` NAMES the env var holding the key (never
 # the key itself). Enters the ability matrix unscored, like any new artifact .
 # Prices are optional: LiteLLM prices a model it knows for the model that answered,
 # cache included; a price here is the fallback for one it does not.
 claude-sonnet:
 model: "anthropic/claude-sonnet-5"
 api_key_env: CBK_ANTHROPIC_API_KEY
 cloud: true
```

A capability's `model_server` distinguishes two different cloud shapes, both `cloud: true`:
a **hosted OpenAI-compatible endpoint** (`model_server` set — some other reachable HTTP
server, called by a real worker exactly like a local one) versus a **registered provider
account** (`model_server` absent — no host node at all, drained only by the coordinator's
own cloud executor). `fleet.py` rejects a node listing the latter at load time: no
worker can ever serve a capability with no host.

## 6. Node enrollment & heartbeat (fleet-management phase)

Spoken between a **worker agent** and the coordinator; see
[design.md](design.md) for the lifecycle these serve.

Enroll (once, with a one-time join token minted by the admin):

```
POST /nodes/enroll
{
 "join_token": "…", // one-time; burned on use
 "hostname": "…", "os": "…", "arch": "arm64",
 "hw": { "ram_gb": 64, "accelerator": "metal|cuda|cpu",
 "vram_gb": 48.0, // what a model must fit into to run at device
 // speed. null = no accelerator or not
 // measurable, which is NOT zero. Capability
 // proposals budget against THIS, not ram_gb.
 "disk_free_gb": 512 }, // on the MODEL STORE's volume, not the fs root
 "profile": "shared" // dedicated | shared | background
}
→ 201 { "node_id": "…", "node_key": "…", // per-node auth key from here on
 "proposed": { "capabilities": [...], "ladder": {...} } } // owner confirms/edits
```

**Bootstrap** (optional, opt-in) — one command joins a machine without the operator key:

```
POST /nodes/bootstrap
X-CBK-Join-Password: …
→ 201 { "join_token": "…", // single-use; feeds POST /nodes/enroll below
 "redis_url": "…", // the broker, credential included
 "consumer_group": "…",
 "capabilities": [ … ] } // what the registry knows, for the caller to check

GET /worker/artifact // same header; the blessed cbk.pyz
GET /releases/{filename} // NO credential — the signed update artifact .
 // Only files the current release manifest names, and
 // `filename` is matched against that allowlist rather
 // than joined as a path. A worker already in the field
 // holds no operator key and no join password; the
 // signature over (version, rid, sha256, channel, url,
 // protocol_version) plus the digest are the boundary,
 // both checked before a byte is written.
```

The point is that **the operator key never leaves the coordinator**. A worker has no
business holding it — it mints join tokens, approves model installs and deletes models
 — so a joining machine presents a join password once and receives a single-use
token plus the broker URL, then enrolls normally and thereafter authenticates with its own
per-node key. `install/worker/join.py` drives this.

Notes that matter:

- **The advertised broker is not always the coordinator's own.** Redis normally runs on the
 coordinator box, so its `CBK_REDIS_URL` is loopback — handing that to a joining worker
 points it at its own localhost, and the failure is invisible at join time (install,
 enrolment and service start all succeed). So `redis_url` comes from
 `CBK_BROKER_ADVERTISE_URL`, falling back to `CBK_REDIS_URL`, and bootstrap returns **503
 naming that setting** rather than advertising a loopback address.
- **`CBK_JOIN_PASSWORD` unset ⇒ both routes 404**, so no surface is added by default. 404
 rather than 401 so a coordinator that has not opted in does not advertise the feature.
- These two are **exempt from the operator-key middleware** — they must be, since the
 caller has no key. So the join password is the *only* thing in front of the broker
 credential. It is therefore required to be at least 16 characters; a shorter value leaves
 the routes disabled (and logs why) rather than weakly guarding Redis — failing closed on
 the feature, not on the service.
- The password is validated **before** a token is minted, so failed guesses cannot grow the
 token table.
- `/worker/artifact` is gated too, even though the artifact is Apache-2.0 code carrying no
 secret of its own. Not a claim of secrecy: the caller holds the password anyway, so gating
 costs nothing and keeps one invariant — the coordinator serves files to no unauthenticated
 caller.
- `capabilities` is returned so the caller can warn when a node is about to serve a tier
 the registry does not contain. That is the failure mode where enrolment succeeds,
 heartbeats report `fitness: ok`, and no job ever routes.

Heartbeat (periodic; also the poll point for updates):

```
POST /nodes/{id}/heartbeat
{
 "mode": "active|away|paused",
 "installed": ["model-a", "model-b"],
 "loaded": ["model-a"], // warm right now. Empty means the model server
 // could not say, NOT that nothing is loaded —
 // which is why a cold-start measurement needs
 // positive evidence before it samples.
 "queues": ["q:8b-extract"], // current subscriptions (follow the ladder)
 "stats": { "jobs_done": 12, // MEASURED from real work, never benchmarked:
 "tps": 38.5, // median output tokens/sec — the artifact x node
 // pairing no ability score can express 
 "load_s": 24.5 } // median seconds to bring a model up from cold,
 // which is what the reservation pre-warm lead is
 // computed from. Both are OMITTED until
 // measured; a node that has served nothing has
 // no speed, and 0 would claim it is instant.
}
→ 200 { "update": null | { …manifest, see §7… }, "planner_notes": [ … ] }
```

**Attached endpoints** (machines running no agent) have no enrollment/heartbeat of their
own: they are registered by the admin, and a coordinator-side **proxy worker** consumes
their queues and drives them over the model-server API (§3); HTTP health checks stand in
for heartbeats.

## 7. Self-update channel

The coordinator hosts a release manifest per platform; agents learn of it via the
heartbeat response (or `GET /updates/manifest?rid=…`):

```
{ "version": "1.4.0", "rid": "osx-arm64",
 "url": "…", "sha256": "…", "signature": "…", // signed; agents verify against a
 "channel": "canary|stable" } // pinned public key — or refuse
```

Rules (rationale in [design.md](design.md)): signature verification
is mandatory (an update channel is RCE by design); canary ring updates first, fleet
follows after a healthy grace period; crash-loop → automatic rollback to the retained
previous binary; queue-contract `protocol_version` gates skew (a too-old worker pauses
pulling until updated); per-node `auto_update: false` opt-out.

## 8. Workload reservations (client ↔ clusterbuck)

Advance capacity booking — see [design.md](design.md) → *Workload
reservations* for semantics (soft commitment, owner-eviction wins, re-planning).

```
POST /reservations
{
 "task_class": "summarize",
 "min_ability": 4,
 "load": "light", // light | medium | heavy (throughput class)
 "duration_min": 30, // expected active window
 "est_jobs": 200, // optional volume hint
 "priority": "medium", // low | medium | high
 "privacy": "local_only",
 "window": { "start": "02:00" | "asap", "recur": "daily" | null }
}
→ 201 {
 "id": "rsv_…",
 "status": "confirmed | counter | declined",
 "plan": { "starts": "…", "warm_by": "…", "artifact": "…", "node": "<opaque>" },
 "counter": null | { …alternative window / ability / cloud offer… }
}
```

- Jobs opt in with `"reservation": "rsv_…"` on `POST /jobs`; they may be submitted
 before the window and queue against it.
- `GET /reservations/{id}` → lifecycle state
 (`scheduled | warming | open | draining | closed`, or `cancelled` on DELETE).
- `DELETE /reservations/{id}` cancels; recurring reservations carry the recurrence on
 the parent and spawn per-occurrence instances.

## 9. Coordinator/operator endpoints

The sections above specify the load-bearing seams. These are the remaining HTTP endpoints the
coordinator serves — mostly read-only views over state described elsewhere, listed here so
"the concrete spec of every boundary" is true rather than aspirational. All of them sit behind
the operator shared secret except where noted.

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness. **Unauthenticated** (probe). |
| `GET /fleet` | The registry as loaded from `fleet.yaml` (§5), including each tier's `cloud_fallback` (the provider account it falls back to, `null` for none), plus two things per capability that a sync client needs to choose an alias: `features` — what its model is declared able to do, from the same lookup `requires` is filtered against (§1b), `null` = undeclared — and `health` — `{state: ready \| degraded \| unreachable \| unknown, checked_at, last_ok_at, last_error}`, the coordinator's own probe of `GET {model_server}/models` every `CBK_MODEL_PROBE_S`. `health` is advisory: the sync plane never refuses on it. A provider account is not probed and reads `unknown`. |
| `GET /queues` | Per-capability **backlog** (queued, never delivered — the real backlog), `pending` (claimed-unacked, i.e. in flight), `depth` (`XLEN`: retained history incl. acked, bounded by `CBK_STREAM_MAXLEN`), live worker `consumers`, and `executors` (live coordinator-side cloud executors). A client does not need this endpoint to know its own place in line — `queue_position` is on the job body (§1b) — which matters because this one sits behind the operator secret. |
| `GET /nodes` | Enrolled nodes: profile, mode, probed hardware, installed/loaded models, last heartbeat. Never exposes `node_key`. |
| `POST /nodes/tokens` | Mint a one-time join token for §6 enrollment. |
| `POST /nodes/{id}/policy` | The owner's contract for a node — `{disk_quota_gb, auto_approve}` as a **JSON body**. `auto_approve` opts that node out of human approval for installs. |
| `GET /usage` | Metering rollups + the avoided-cloud-spend headline + budget burn (design.md → Usage accounting). The budget is **enforced**, on both planes (design.md §8). |
| `GET /ability` | The ability matrix `ability(artifact, task_class)` with its scale version, each row marked `seed` or `measured`, plus a per-artifact headline scalar . |
| `POST /ability/clear?artifact=<name>` | Drop an artifact's scores so the eval harness re-measures it. The heartbeat handler already does this automatically when a model's digest changes ; this is the same reset for an operator to trigger by hand when an artifact's behaviour changed without its digest moving (e.g. a model-server config or template edit). Opens a new measurement generation as well as dropping the scores, so the next batch is not averaged with the measurements being discarded; returns `{artifact, cleared, generation}`. |
| `GET /eval` | Eval-harness state: artifacts still needing measurement, and per-batch progress. |
| `POST /eval/run` | Run one harness pass now instead of waiting for the coordinator cadence. |
| `GET /catalog` | Known-good artifacts with the metadata the "fits" gate needs . |
| `POST /catalog` | Add or update a catalog candidate; upsert by `artifact`. The seed list only writes into an empty table, so without this the catalog froze at whatever shipped and no newer model could ever be proposed. `expected_ability` is a ranking **hint** on the 1-10 scale, never a score: an approved install is still measured before anything routes to it . |
| `GET /proposals` | Planner proposals (`upgrade` / `reeval` / `reclaim`), filterable by status. |
| `POST /proposals/scan` | Re-run the planner immediately. |
| `POST /proposals/{id}/approve` \| `/deny` | The human gate. Single-shot: deciding twice is a conflict, not a silent overwrite. |
| `GET /updates/manifest?rid=…` | A signed release manifest (§7). 404 when no update channel is configured. |
| `GET /` and `GET /ui/*` | The htmx dashboard and its fragments . Browser clients exchange `?key=…` for a cookie once. |

`GET /jobs/{id}` additionally returns `urgency`, which is not in §1b: it reflects the
escalation trajectory , so a client can see that its `waitable` work was promoted.
