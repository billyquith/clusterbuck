# Protocols

Every boundary in clusterbuck is a documented protocol, which is what keeps the system
polyglot and open (see [`../DESIGN.md`](../DESIGN.md) → *Implementation language &
boundaries*). This document specifies each seam. Shapes below are **design intent**, not
a frozen spec — field names may change before the first implementation.

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
  "escalate_after_min": 10,             //   waitable ages into necessary (see
                                        //   fleet-management.md → urgency & escalation)
  "privacy":     "local_only",          // local_only | cloud_ok (default local_only —
                                        //   local_only NEVER routes to cloud)
  "deadline":    "2026-01-01T00:00:00Z",// optional hard expiry
  "callback_url":"https://…"            // optional; else poll
}
→ 202 Accepted
{ "id": "job_…", "result_key": "res_…", "status": "queued" }
```

Poll for the result:

```
GET /jobs/{id}
→ { "id","status":"queued|running|done|failed|expired",
    "result": { … OpenAI-style completion … } | null,
    "error": null, "attempts": 1, "worker": "<opaque-node-id>" }
```

The client never learns which machine ran the job beyond an opaque id (diagnostics only).

## 2. clusterbuck server ↔ worker (Redis queue contract)

The server and the reference worker communicate **only** through Redis. Any worker in
any language that honours this contract can join the fleet.

- **Queues:** one list per capability, `q:<capability>` (e.g. `q:8b`, `q:32b`). A worker
  consumes from the queues it can satisfy (blocking pop, e.g. `BLPOP`).
- **Job payload on the queue:** the job record —

  ```
  {
    id, created_at,
    capability,
    messages | prompt,
    params,
    urgency, escalate_after_min, privacy, deadline,
    result_key,
    attempts, max_attempts
  }
  ```

- **In-flight tracking:** a claimed job moves to a per-worker processing list with a
  **visibility timeout**; if the worker doesn't acknowledge before it expires, a reaper
  returns the job to `q:<capability>` (bounded by `max_attempts`). This is what makes a
  laptop closing its lid mid-job safe.
- **Result:** written once to the result store under `result_key` (a Redis key with a
  TTL), shape mirroring an OpenAI completion plus `{status, error, worker}`.
- **Idempotency:** results are write-once under `result_key`; a re-run of the same job
  overwrites deterministically rather than duplicating.

## 3. Worker ↔ model server (OpenAI HTTP)

The worker calls the model server running on its own machine over the OpenAI-compatible
API (`POST /v1/chat/completions`). This is why the model server can be Ollama, llama.cpp,
vLLM, or LM Studio interchangeably — the worker only speaks the wire protocol, never a
vendor SDK. Local models don't reliably honour structured-output flags, so JSON-shape
requests are made in the prompt and validated by the client, not assumed here.

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
```

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
