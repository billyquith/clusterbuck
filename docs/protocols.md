# Protocols

Every boundary in clusterbuck is a documented protocol, which is what keeps the system
polyglot and open (see [`../DESIGN.md`](../DESIGN.md) → *Implementation language &
boundaries*). This document specifies each seam. The shapes below describe the **implemented** system;
where a machine-readable schema exists in [`contract/`](../contract/) that schema is
authoritative and this document is its prose companion.

## 1. Client ↔ clusterbuck

Two entry points, both HTTP; a client picks one per request.

### 1a. Sync (interactive) — OpenAI-compatible
Standard OpenAI Chat Completions, served by the LiteLLM gateway:

```
POST /v1/chat/completions
{ "model": "<capability-alias>", "messages": [...], "temperature": ..., "max_tokens": ... }
```

The `model` alias maps to a capability tier in the gateway config. Response is the
standard OpenAI shape. Any OpenAI-compatible SDK works unmodified.

### 1b. Async (patient) — job API
Submit a job:

```
POST /jobs
{
  // Addressing — one of the two forms:
  "task_class":  "summarize",           // preferred: describe the need…
  "min_ability": 6,                     // …and the coordinator resolves an artifact
                                        //   (see model-evaluation.md for the 1-10 scale)
  "capability":  "32b-reason",          // OR name a supply-side tier explicitly (advanced)
  "messages":    [ {role, content}, … ],// OpenAI-style; or "prompt"
  "params":      { "temperature": 0.2, "max_tokens": 1500, "response_format": "json_object" },
  "urgency":     "waitable",            // urgent | necessary | waitable — a trajectory:
  "escalate_after_min": 10,             //   waitable = eager but non-demanding; N is a
                                        //   patience bound (not a delay) — unserved
                                        //   after N min it becomes necessary (see
                                        //   fleet-management.md → urgency & escalation)
  "privacy":     "local_only",          // local_only | cloud_ok (default local_only —
                                        //   local_only NEVER routes to cloud)
  "deadline":    "2026-01-01T00:00:00Z",// optional expiry; see expires_at below.
                                        //   Enforced by a coordinator sweep, NOT by
                                        //   removing the queued entry, and the worker
                                        //   does not check it — a job already claimed
                                        //   runs to completion regardless.
  "callback_url":"https://…",           // optional; else poll
  "submitter": {                        // optional caller provenance (all fields optional)
    "app":          "nightly-importer", // which client
    "instance":     "workstation-2",    //   on which machine
    "request_id":   "req_4f9c1e70a2",   // one LOGICAL call — see below
    "submitted_at": "2026-01-01T00:00:00Z" // client's own clock; ADVISORY only
  }
}
→ 202 Accepted
{ "id": "job_…", "result_key": "res_…", "status": "queued" }
```

Both addressing forms **fail explicitly** (`422`) at submit time rather than queuing a job
nothing will ever serve: `task_class`/`min_ability` when no artifact clears the ability bar
(model-evaluation.md), and `capability` when the name isn't in the fleet registry — a typo'd
capability would otherwise sit on a stream no worker consumes, with no error and no expiry
short of `deadline`.

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
→ { "id","status":"queued|running|done|failed|expired",
    "urgency","capability",
    "created_at":  "…",          // server receipt time — authoritative
    "started_at":  "…" | null,   // a claim was OBSERVED at this time (see below)
    "finished_at": "…" | null,   // a terminal status was first recorded at this time
    "deadline":    "…" | null,   // your `deadline`, echoed back
    "escalates_at":"…" | null,   // when waitable gains the right to demand capacity
    "expires_at":  "…" | null,   // effective give-up time; NULL = nothing ever gives up
    "queue_position": 0 | null,  // unclaimed jobs ahead of you; null unless queued
    "result": { … OpenAI-style completion … } | null,
    "error": null, "attempts": 0, "worker": "<opaque-node-id>" | null,
    "submitter": { "app","instance","request_id","submitted_at",
                   "observed_ip" } | null }   // provenance as recorded; null if none
```

The client never learns which machine ran the job beyond an opaque id (diagnostics only).

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
at all** — it is excluded from escalation (no `escalate_at`), from the deadline sweep (no
`deadline`), and it is invisible to the reaper, because `XAUTOCLAIM` walks the
pending-entries list and a never-delivered entry never enters it. Its only exit is being
trimmed away by `MAXLEN ~` once enough later traffic arrives on the same capability, with
no result and no status change. `expires_at: null` reports exactly that, and is the reason
to set `deadline` and/or `escalate_after_min` on anything a human is waiting for.

A `deadline` that is not an RFC 3339 timestamp is rejected with `422`. It used to be
dropped silently to "no expiry", which handed back the opposite of what was asked for with
nothing to notice.

**Collecting a result later.** A completed result lives in the result store for
`CBK_RESULT_TTL_S` (default 24h). After that, `GET /jobs/{id}` still answers with the
job's terminal status and timing, but `result` is `null` — `finished_at` is what
distinguishes "it ran and the result expired" from "it never ran". Prompts and completions
are deliberately not kept in the coordinator's durable store (metering is metadata-only),
so a client that needs a result beyond the TTL must persist it on first successful poll.

## 2. clusterbuck server ↔ worker (Redis queue contract)

The server and the reference worker communicate **only** through Redis. Any worker in
any language that honours this contract can join the fleet — this section is the spec you
would write one from, so it states the concrete names and commands rather than intent.

Implemented on **Redis Streams + consumer groups** (ADR 20), *not* plain lists: the
pending-entries list and `XAUTOCLAIM` provide the visibility-timeout / at-least-once
semantics natively.

- **Stream per capability:** `q:<capability>` (e.g. `q:8b-extract`). The job JSON is the
  single field **`job`** of each entry. The server appends with
  `XADD q:<capability> MAXLEN ~ <cap> * job <json>`.
- **Consumer group:** one shared group named **`cbk-workers`** (`CBK_CONSUMER_GROUP`) per
  stream, created with `XGROUP CREATE … $ MKSTREAM` (tolerate `BUSYGROUP`). Every worker
  joins the *same* group, so each job is delivered to exactly one of them.
- **Claiming:** `XREADGROUP GROUP cbk-workers <worker-id> COUNT n STREAMS q:<capability> >`.
  Note the reference worker polls rather than blocking, because StackExchange.Redis exposes
  no blocking read; a blocking `BLOCK` argument is equally valid for the contract.
- **Job payload** (authoritative schema: [`contract/job.schema.json`](../contract/job.schema.json)) —

  ```
  {
    id, created_at,
    capability,
    messages | prompt,
    params,                 # forwarded to the model server; see the `model` pin below
    urgency, escalate_after_min, privacy, deadline,
    result_key,
    attempts, max_attempts,
    submitter?              # optional caller provenance (§1b); parse-and-ignore is fine
  }
  ```

- **`params.model` is a REQUIREMENT, not a hint.** If present it names the exact artifact the
  job must run on, overriding the worker's configured default model. The eval harness relies
  on it to attribute a measurement to the artifact under test; a worker that ignores it will
  silently mis-attribute scores.
- **Completion:** write the result (below), then `XACK q:<capability> cbk-workers <entry-id>`.
  Acknowledge on failure too, having written a `failed` result — an unacked entry is
  indistinguishable from an abandoned one.
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
  **`completion`**, with optional `usage`. `status` is `done | failed | expired`.
- **Idempotency:** a re-run overwrites the same `result_key` rather than duplicating. Delivery
  is at-least-once, so a job may legitimately run twice; the coordinator skips re-running a
  reclaimed entry whose result already exists.

## 3. Worker ↔ model server (OpenAI HTTP)

The worker calls the model server running on its own machine over the OpenAI-compatible
API (`POST /v1/chat/completions`). This is why the model server can be Ollama, llama.cpp,
vLLM, or LM Studio interchangeably — the worker only speaks the wire protocol, never a
vendor SDK. Local models don't reliably honour structured-output flags, so JSON-shape
requests are made in the prompt and validated by the client, not assumed here.

If that model server sits behind an authenticated gateway, `CBK_MODEL_SERVER_API_KEY` (node
env) sends `Authorization: Bearer <key>` on every call. This is node-local config, the same
trust boundary as `CBK_MODEL_SERVER_URL` — it is **not** how a registered cloud provider
account is reached (§5, ADR 30): those keys live only on the coordinator, which calls the
provider itself rather than ever handing a worker that key. A job's own `params` can never
supply or override `api_key`/`api_base` (contract/protocols.md §2's envelope rule, same as
`stream`/`messages`).

## 4. Coordinator ↔ node (Wake-on-LAN)

Waking a sleeping machine is a pure network action, needing no software on the target:

- The coordinator sends a **Wake-on-LAN magic packet** to a node's MAC address.
- Trigger conditions: a `q:<capability>` has depth and a capable machine is asleep on the
  LAN, or a scheduled-wake window opens.
- The target must have "wake for network access" enabled and be reachable on the LAN;
  off-LAN machines cannot be woken (see [deployment.md](deployment.md)).

## 5. Model / capability registry

Which node hosts which models, its MAC (for WoL), and its capacity. Starts as static
config; in the fleet-management phase (see [fleet-management.md](fleet-management.md))
the same shape becomes the seed for a coordinator-held **dynamic registry** maintained by
enrollment + heartbeats, which additionally tracks *installed vs currently loaded* models,
profile, and presence mode per node.

```yaml
# fleet.yaml
nodes:
  - id: node-a
    mac: "aa:bb:cc:dd:ee:ff"      # for Wake-on-LAN
    wake: on-demand               # opportunistic | scheduled | on-demand
    capabilities: [8b-extract]
  - id: node-b
    mac: "11:22:33:44:55:66"
    wake: opportunistic
    capabilities: [8b-extract, 32b-reason, 70b-reason]
capabilities:
  8b-extract:  { queue: "q:8b",  model_server: "http://localhost:11434/v1", model: "…" }
  32b-reason:  { queue: "q:32b", model_server: "http://localhost:11434/v1", model: "…" }
  70b-reason:  { queue: "q:70b", model_server: "http://localhost:11434/v1", model: "…" }

  # A registered provider account (ADR 30): no `model_server` — it has no host node, so
  # the coordinator calls it directly instead of dispatching to a worker. `model` is a
  # LiteLLM "<provider>/<model>" id; `api_key_env` NAMES the env var holding the key (never
  # the key itself). Enters the ability matrix unscored, like any new artifact (ADR 15).
  claude-sonnet:
    queue: "q:claude-sonnet"
    model: "anthropic/claude-3-5-sonnet-20241022"
    api_key_env: CBK_ANTHROPIC_API_KEY
    cloud: true
    price_in_per_1k: 0.003
    price_out_per_1k: 0.015
```

A capability's `model_server` distinguishes two different cloud shapes, both `cloud: true`:
a **hosted OpenAI-compatible endpoint** (`model_server` set — some other reachable HTTP
server, called by a real worker exactly like a local one) versus a **registered provider
account** (`model_server` absent — no host node at all, drained only by the coordinator's
own cloud executor, ADR 30). `fleet.py` rejects a node listing the latter at load time: no
worker can ever serve a capability with no host.

## 6. Node enrollment & heartbeat (fleet-management phase)

Spoken between a **worker agent** and the coordinator; see
[fleet-management.md](fleet-management.md) for the lifecycle these serve.

Enroll (once, with a one-time join token minted by the admin):

```
POST /nodes/enroll
{
  "join_token": "…",                    // one-time; burned on use
  "hostname": "…", "os": "…", "arch": "arm64",
  "hw": { "ram_gb": 64, "accelerator": "metal|cuda|cpu",
          "vram_gb": null, "disk_free_gb": 512,
          "bench_tps_small": 42.0 },    // optional micro-benchmark
  "profile": "shared"                   // dedicated | shared | background
}
→ 201 { "node_id": "…", "node_key": "…",   // per-node auth key from here on
        "proposed": { "capabilities": [...], "ladder": {...} } }  // owner confirms/edits
```

Heartbeat (periodic; also the poll point for updates):

```
POST /nodes/{id}/heartbeat
{
  "mode": "active|away|paused",
  "installed": ["model-a", "model-b"],
  "loaded":    ["model-a"],             // warm right now — enables model-affinity routing
  "queues":    ["q:8b"],                // current subscriptions (follow the ladder)
  "stats":     { "jobs_done": 12, "tps": 38.5 }
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
  "url": "…", "sha256": "…", "signature": "…",   // signed; agents verify against a
  "channel": "canary|stable" }                    //   pinned public key — or refuse
```

Rules (rationale in [fleet-management.md](fleet-management.md)): signature verification
is mandatory (an update channel is RCE by design); canary ring updates first, fleet
follows after a healthy grace period; crash-loop → automatic rollback to the retained
previous binary; queue-contract `protocol_version` gates skew (a too-old worker pauses
pulling until updated); per-node `auto_update: false` opt-out.

## 8. Workload reservations (client ↔ clusterbuck)

Advance capacity booking — see [fleet-management.md](fleet-management.md) → *Workload
reservations* for semantics (soft commitment, owner-eviction wins, re-planning).

```
POST /reservations
{
  "task_class":  "summarize",
  "min_ability": 4,
  "load":        "light",               // light | medium | heavy (throughput class)
  "duration_min": 30,                   // expected active window
  "est_jobs":    200,                   // optional volume hint
  "priority":    "medium",              // low | medium | high
  "privacy":     "local_only",
  "window":      { "start": "02:00" | "asap", "recur": "daily" | null }
}
→ 201 {
  "id": "rsv_…",
  "status": "confirmed | counter | declined",
  "plan":    { "starts": "…", "warm_by": "…", "artifact": "…", "node": "<opaque>" },
  "counter": null | { …alternative window / ability / cloud offer… }
}
```

- Jobs opt in with `"reservation": "rsv_…"` on `POST /jobs`; they may be submitted
  before the window and queue against it.
- `GET /reservations/{id}` → lifecycle state
  (`scheduled | warming | open | draining | closed | replanned`) + updated plan.
- `DELETE /reservations/{id}` cancels; recurring reservations carry the recurrence on
  the parent and spawn per-occurrence instances.

## 9. Client attention (escalation signal)

A client tells the coordinator its user became active, so pending lazy work heats up —
semantics in [fleet-management.md](fleet-management.md) → *Urgency, escalation & client
attention*. Attention is a **lease**: the client refreshes it while the user stays
active; expiry demotes unstarted work gracefully.

```
POST /attention
{
  "client_key": "…",
  "state":      "active",               // active | idle (idle ends the lease early)
  "scope":      ["summarize"] | null,   // optional task-class filter
  "ttl_s":      600                     // lease duration; refresh to extend
}
→ 200 { "promoted": 37, "prewarm": ["<artifact>"], "lease_expires": "…" }
```

Effect: the client's `waitable` backlog (within scope) promotes to `necessary`,
coalesced into a warm window rather than per-job wakes; artifacts that backlog needs may
pre-warm. On lease expiry, unstarted promoted jobs return to `waitable`.

## 10. Coordinator/operator endpoints

The sections above specify the load-bearing seams. These are the remaining HTTP endpoints the
coordinator serves — mostly read-only views over state described elsewhere, listed here so
"the concrete spec of every boundary" is true rather than aspirational. All of them sit behind
the operator shared secret (ADR 26) except where noted.

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness. **Unauthenticated** (probe). |
| `GET /fleet` | The static registry as loaded from `fleet.yaml` (§5). |
| `GET /queues` | Per-capability **backlog** (queued, never delivered — the real backlog), `pending` (claimed-unacked, i.e. in flight), `depth` (`XLEN`: retained history incl. acked, bounded by `CBK_STREAM_MAXLEN`), live worker `consumers`, and `executors` (live coordinator-side cloud executors). A client does not need this endpoint to know its own place in line — `queue_position` is on the job body (§1b) — which matters because this one sits behind the operator secret. |
| `GET /nodes` | Enrolled nodes: profile, mode, probed hardware, installed/loaded models, last heartbeat. Never exposes `node_key`. |
| `POST /nodes/tokens` | Mint a one-time join token for §6 enrollment. |
| `POST /nodes/{id}/policy` | The owner's contract for a node — `{disk_quota_gb, auto_approve}` as a **JSON body**. `auto_approve` opts that node out of human approval for installs. |
| `GET /usage` | Metering rollups + the avoided-cloud-spend headline + budget burn (fleet-management.md → Usage accounting). Budget is **displayed, not enforced**. |
| `GET /ability` | The ability matrix `ability(artifact, task_class)` with its scale version, each row marked `seed` or `measured`, plus a per-artifact headline scalar (ADR 15/16). |
| `POST /ability/clear?artifact=<name>` | Drop an artifact's scores so the eval harness re-measures it. The heartbeat handler already does this automatically when a model's digest changes (ADR 15); this is the same reset for an operator to trigger by hand when an artifact's behaviour changed without its digest moving (e.g. a model-server config or template edit). Opens a new measurement generation as well as dropping the scores, so the next batch is not averaged with the measurements being discarded; returns `{artifact, cleared, generation}`. |
| `GET /eval` | Eval-harness state: artifacts still needing measurement, and per-batch progress. |
| `POST /eval/run` | Run one harness pass now instead of waiting for the coordinator cadence. |
| `GET /catalog` | Known-good artifacts with the metadata the "fits" gate needs (ADR 25). |
| `GET /proposals` | Planner proposals (`upgrade` / `reeval` / `reclaim`), filterable by status. |
| `POST /proposals/scan` | Re-run the planner immediately. |
| `POST /proposals/{id}/approve` \| `/deny` | The human gate. Single-shot: deciding twice is a conflict, not a silent overwrite. |
| `GET /updates/manifest?rid=…` | A signed release manifest (§7). 404 when no update channel is configured. |
| `GET /` and `GET /ui/*` | The htmx dashboard and its fragments (ADR 21). Browser clients exchange `?key=…` for a cookie once. |

`GET /jobs/{id}` additionally returns `urgency`, which is not in §1b: it reflects the
escalation trajectory (ADR 18), so a client can see that its `waitable` work was promoted.
