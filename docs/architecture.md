# Architecture

Deeper treatment of the structure sketched in [`../DESIGN.md`](../DESIGN.md). Read that
first for the overview; this document covers the internal shape, the responsibility
split with off-the-shelf parts, and request lifecycles.

## One front door, two planes

A client sends every request to a single, stable endpoint. Behind it, a thin front
decides **route now vs. queue**, then delegates:

```
                     ┌──────────────── clusterbuck server (always-on node) ───────────────┐
   client ── job ──► │                                                                     │
                     │   interactive / a live worker available?                            │
                     │        ├─ YES → sync plane (LiteLLM) ──► live worker ──► reply       │
                     │        │                                                            │
                     │        └─ NO / "patient" → async plane (job broker, Redis)          │
                     │                • enqueue by capability (q:8b / q:32b / …)            │
                     │                • coordinator: Wake-on-LAN / scheduled wake           │
                     │                • a worker wakes, pulls, runs its model server        │
                     │                • result written to the result store                 │
   client ◄─ result ◄│                • client polls / callback                            │
                     └─────────────────────────────────────────────────────────────────────┘
```

The two planes share the **same** worker machines and model servers. A machine that is
awake can serve sync traffic *and* drain the async queue.

## What we build vs. what we adopt

The single most important architectural fact: **the sync plane already exists off the
shelf (LiteLLM); clusterbuck builds the async plane and the coordination around it.**

| Concern | Owner | Notes |
|---|---|---|
| OpenAI-compatible endpoint, route to a live worker, health check, load-balance, cloud fallback, cooldown | **LiteLLM** (adopted) | Request/response only — no concept of "hold this job until a machine wakes" |
| Durable job queue, submit/poll API, urgency + escalation | **clusterbuck server** (built) | The async plane |
| Wake-on-LAN, scheduled-wake policy, queue-depth watching | **clusterbuck coordinator** (built) | Turns "asleep" into "available" |
| Pull a job, call the local model server, write the result | **clusterbuck worker** (built) | Reference implementation; see [protocols](protocols.md) |
| The actual inference | **model server** (adopted) | Ollama / llama.cpp / vLLM / LM Studio |
| Queue + result storage | **Redis** (adopted) | Language-agnostic broker |

So "the host server wraps LiteLLM" is half the picture: LiteLLM handles *route-now*;
clusterbuck adds *queue-and-wait* plus the wake coordinator, sharing one worker pool.

## Efficiency is footprint, concurrency, distribution — not FLOPs

clusterbuck does almost no heavy computation. The compute lives in the model servers,
which it only talks to over HTTP. clusterbuck's own work is **I/O-bound orchestration**:
Redis queue operations, HTTP health checks, Wake-on-LAN packets, shuttling results. So
the implementation is optimised for **small idle footprint** (it may share the RAM-tight
always-on node), **high I/O concurrency** (`async`/await over many waits), and **trivial
distribution** (a self-contained binary per node). This is why the language choice
(see [decisions](decisions.md)) weighs distribution and familiarity over raw throughput.

## Addressing by capability, not by machine

Jobs never name a machine. They request a **capability tier** — an abstract label like
`8b-extract`, `32b-reason`, `70b-reason`. Workers subscribe to the capability queues
they can satisfy. This is what makes a heterogeneous, intermittent fleet work: a job for
`70b-reason` simply waits on that queue until *some* 70B-capable worker is up and pulls
it, with no dispatcher tracking who is alive. Adding or removing a machine changes only
which queues have consumers, never any client or job.

## Pull, not push

Workers **pull** from the queue; the server never pushes to a specific worker. For an
intermittent fleet this is decisive:

- No central liveness table to maintain, and no job ever dispatched to a node that just
  slept or left the LAN.
- A worker being subscribed and consuming *is* the liveness signal.
- Backpressure is natural: a slow/absent capability just means its queue grows until a
  worker drains it (or the coordinator wakes one).

## Request lifecycles

### Sync (interactive)
1. Client calls the OpenAI-compatible endpoint (`/v1/chat/completions`) with a model
   alias that maps to a capability.
2. LiteLLM routes to a healthy worker hosting that capability; on a miss it fails over
   to another worker or to a configured cloud model.
3. Response streams straight back. Nothing is queued.

### Async (patient)
1. Client `POST`s a job (capability, prompt, params, urgency class, optional deadline)
   and receives a job id + `result_key`.
2. The server enqueues it on `q:<capability>`.
3. If no capable worker is consuming, the coordinator may send Wake-on-LAN (on-demand
   policy) or wait for a scheduled/opportunistic wake.
4. A capable worker pulls the job, calls its local model server, writes the result to
   the result store under `result_key`, and acknowledges.
5. Client retrieves the result by polling (or a callback, if enabled). On worker death
   mid-job, the visibility timeout returns the job to the queue for another attempt.

See [protocols.md](protocols.md) for the concrete message shapes at each hop.
