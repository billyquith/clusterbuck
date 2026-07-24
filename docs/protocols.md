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
  "capability":  "32b-reason",          // required; the tier, not a machine
  "messages":    [ {role, content}, … ],// OpenAI-style; or "prompt"
  "params":      { "temperature": 0.2, "max_tokens": 1500, "response_format": "json_object" },
  "policy":      "wait",                // wait | wait_then_cloud | now
  "privacy":     "local_only",          // local_only | cloud_ok (default local_only —
                                        //   local_only NEVER routes to cloud)
  "deadline":    "2026-01-01T00:00:00Z",// optional; required for wait_then_cloud
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
    policy, privacy, deadline,
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
