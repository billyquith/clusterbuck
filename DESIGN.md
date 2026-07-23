# clusterbuck — design

> A **LAN LLM worker network**: submit inference jobs from any tool on the home
> network and have them run on whichever machine is capable and available — now, or
> when one next wakes up. (Name: a nod to *"pass the buck"* — the broker hands each
> job off to whichever worker is up.)

## Purpose

Turn a handful of intermittently-available LAN machines into a shared pool of LLM
capacity. Clients submit work; the fabric decides *where* it runs and *when* —
routing to a live worker for interactive requests, or queuing patient work until a
capable machine is contactable (waking one if worth it).

This is **infrastructure**, deliberately domain-agnostic. clusterbuck knows nothing
about any client's application: it only ever sees jobs, capabilities, and results.
Clients talk to a stable endpoint and never learn which physical machine served them.

## Why not an existing serving cluster

Frameworks like vLLM clusters, Ray Serve, or llama.cpp RPC assume **always-on,
homogeneous** nodes. Our defining constraint is the opposite: a **heterogeneous,
intermittent** fleet (a 16 GB always-on mini; a 64 GB laptop that roams and sleeps;
maybe more later). That is a *job-queue* problem, not a *serving-cluster* problem.
So the design is a distributed task queue with LLM-aware routing — not a cluster.

**These are complementary, not competing.** A serving engine like **vLLM sits one layer
below clusterbuck** — it is a *model server*, at the same level as Ollama / llama.cpp /
LM Studio. On a node with a capable (typically NVIDIA) GPU you could run vLLM as that
node's model server and have the worker call its OpenAI-compatible endpoint; clusterbuck
neither knows nor cares. What vLLM optimises — continuous batching, PagedAttention KV
cache, tensor/pipeline parallelism across a tight GPU cluster — is exactly the always-on,
homogeneous, fast-interconnect territory clusterbuck's non-goals exclude. clusterbuck
does the coarse-grained cross-machine routing *above* the engine; vLLM does the
high-throughput single-node (or datacenter-cluster) serving *within* one.

## Core idea: one queue, many policies

The ways a machine can contribute — "just work when contactable", "wake on a
schedule and drain a batch", "get woken on demand" — are not three systems. They
are **policies over one shared queue**:

| Intent | Policy | Mechanism |
|---|---|---|
| Work whenever up | opportunistic | worker **pulls** jobs while it's alive |
| Batch on a schedule | scheduled | `pmset` wake window → drain → sleep |
| Wake for pressure/priority | on-demand | coordinator sends **Wake-on-LAN** when queue depth / priority warrants |

Same jobs, same queue; each machine (and optionally each job) just carries a
different wake/consume policy.

## Two planes over one pool of machines

Both planes share the same worker machines and the same model servers on them.

1. **Sync plane — "answer now."** A **LiteLLM gateway** (OpenAI-compatible) in front
   of the workers. Health-checked routing + load-balancing across whichever workers
   are live; a miss falls back to another worker or to cloud. Any client that speaks
   the OpenAI API can use it, unmodified. This is the interactive path.

2. **Async plane — "patient work."** A **job queue**. The client submits
   `{capability, prompt, params, deadline, result_key}`, receives a job id, and
   polls (or gets a callback). The job waits in the queue until a capable worker
   drains it — after a Wake-on-LAN or scheduled wake if necessary. This is the batch
   / wake-them-up path.

A machine can serve sync requests while awake **and** drain the async queue.

## Pull-based workers + capability queues

For an intermittent fleet, **pull beats push**: no central liveness tracking, and a
job is never dispatched to a node that just went away. Each machine runs a worker
subscribed to the queues matching the models it can host:

- 16 GB mini → `q:8b`
- 64 GB laptop → `q:8b`, `q:32b`, `q:70b`

Jobs request a **capability tier** (e.g. `70b-reasoning`), not a machine, and land
wherever fits. A `q:70b` job simply waits until a 70B-capable worker is up and
pulls it. Graceful intermittency, for free.

## Components

| Component | Role | Choice |
|---|---|---|
| **Server** | job API, sync front, coordinator | **C#/.NET** (this repo) |
| **Worker** | pulls jobs for its capabilities, calls its local model server, writes result | **C#/.NET** (this repo; reference implementation) |
| **Broker + result store** | holds queued jobs and results | Redis |
| **Model server** | the actual inference on each machine | Ollama / LM Studio / vLLM (OpenAI-compatible) — off the shelf |
| **Sync gateway** | OpenAI-compatible routing for interactive requests | LiteLLM (Python) — off the shelf |
| **Model registry** | which machine hosts which models + capacity | static YAML first; heartbeat later |

The always-on node is the natural home for the server (broker + gateway + coordinator);
every other node runs a worker that contributes whenever it's up.

## Implementation language & boundaries

Only clusterbuck's **own** moving parts are written here, in **C#/.NET**: the **server**
(job API + sync front + coordinator) and the **reference worker**. C# is the choice for
author fluency, cross-platform reach (macOS/Linux/Windows × arm64/amd64), and
**self-contained single-file / Native AOT** binaries that drop onto any node with no
runtime install. Server and worker share one codebase and the same job/result types.

Everything else is either off the shelf (Redis, LiteLLM, the model servers) or external
(clients). Each is reached across a **documented protocol**, so the system is polyglot by
design and open at every seam:

| Boundary | Protocol | Consequence |
|---|---|---|
| Client ↔ clusterbuck | HTTP: submit-job / poll-result API + OpenAI-compatible sync endpoint | Clients are any language / any OS (e.g. a Python client) |
| clusterbuck ↔ worker | Redis queue contract (job + result schema) | C# worker is the *reference*; a worker in another language can slot in |
| worker ↔ model server | OpenAI-compatible HTTP | Any model server (Ollama / llama.cpp / vLLM / LM Studio) |
| coordinator ↔ node | Wake-on-LAN magic packets | Pure network protocol; no agent code needed to be woken |

Keeping server + worker in one C# repo is a convenience (shared types, one artefact to
distribute), **not** a constraint the fabric imposes on anyone integrating with it.

## Job model (async)

```
job = {
  id, created_at,
  capability,          # e.g. "8b-extract", "32b-reason", "70b-reason"
  messages / prompt,   # OpenAI-style
  params,              # temperature, max_tokens, response_format hint, …
  policy,              # wait | wait_then_cloud | now   (see below)
  deadline,            # optional; for wait_then_cloud / expiry
  result_key,          # where the worker writes the result
  attempts, max_attempts,
}
```

- **Idempotent + retryable.** A worker can die mid-inference (laptop lid closes).
  Jobs use a visibility timeout and are safe to re-run; results are written once
  under `result_key`.
- **Patience policy** (the knob a client sets):
  - `wait` — queue for a capable worker indefinitely; never cloud. Default for
    scheduled/background work.
  - `wait_then_cloud` — queue until `deadline`, then fall back to cloud.
  - `now` — go straight to the sync plane (live worker or cloud). For a user
    watching a spinner.

## Availability & wake

- **Detecting "contactable"** — the worker being subscribed and pulling *is* the
  liveness signal; no separate poll needed for the opportunistic case.
- **Scheduled wake** — `pmset repeat wake` windows for predictable overnight batch
  draining, then sleep.
- **On-demand wake** — the coordinator sends **Wake-on-LAN** magic packets when a
  capability queue has depth (or a priority/`now`-ish job) and a capable machine is
  asleep *on the LAN*.
- **Off-LAN machines** (e.g. the work laptop at the office) are **opportunistic
  only** — they cannot be woken remotely and simply don't drain until home.

## Security / trust

- LAN-only by default: bind model servers, gateway, and broker to the home network;
  a shared key at minimum on the gateway/queue.
- **Corporate machines** (e.g. an MDM-managed work laptop) participate as
  **dumb model-server endpoints** only — no fabric code, no client credentials,
  no DB access on them. Respect device policy; they may be firewalled/VPN'd.

## MVP scope

Smallest thing that proves the core loop, using parts already on hand:

1. Redis queues keyed by capability (`q:8b`, `q:32b`, …) + a result store.
2. One worker daemon (pull-based) on one machine, wrapping a local OpenAI-compatible
   model server; writes results back to Redis.
3. A submit/poll client library (submit job → poll `result_key`).
4. LiteLLM in front for the sync plane, health-checking that worker.
5. A static `fleet.yaml` model/capability registry.

Deferred: coordinator with WoL + scheduled wake; heartbeat registry; multiple
workers; priority scheduling; metrics/throughput accounting; callbacks/webhooks.

## Non-goals (for now)

- Not a WAN/public service — LAN only.
- Not a training/fine-tuning system — inference serving only.
- Not a cluster serving framework — intermittency is the point.
- No per-token cost accounting (local compute is "free").

## Example fleet

The design targets a **heterogeneous, intermittent** set of machines — not a uniform
cluster. A representative fleet:

| Node | RAM | Availability | Hosts | Role |
|---|---|---|---|---|
| always-on server | ~16 GB | 24/7 | small model (e.g. 8B, 4-bit ~5 GB) | broker + gateway + coordinator + light worker |
| workstation / laptop | 64 GB+ | sleeps / roams | large models (e.g. 32B, MoE, 70B) | heavy-reasoning worker (opportunistic + Wake-on-LAN when reachable) |
| additional nodes | — | — | capability-matched | extra pull workers |

**macOS / Metal note:** a Metal-backed model is capped by `iogpu.wired_limit_mb` (~67%
of RAM by default); raise it only after freeing resident RAM. Workers should keep large
models warm with sensible unload timeouts to avoid thrashing on shared machines.

## Clients

A client is any tool that needs inference; clusterbuck is agnostic to what it does. A
client targets one endpoint:

- **Interactive** work → the OpenAI-compatible **sync** endpoint (routes to a live
  worker, falls back to cloud on a miss). Drop-in for anything that speaks the OpenAI API.
- **Patient** work → **submit an async job** (capability + prompt + patience policy),
  then poll or receive a callback. The job queues until a capable worker drains it.

Clients never learn which machine served them, and clusterbuck never learns anything
about the client's domain.

## Open questions

- Result delivery for async: poll-only vs callbacks/webhooks.
- How workers advertise *currently loaded* vs *installable* models (affects cold-load
  latency and routing).
- Priority/fairness across multiple tenants once there's more than one.

## Detailed docs

- [docs/architecture.md](docs/architecture.md) — internal shape, what we build vs. adopt
  (LiteLLM handles sync; we build the async plane), request lifecycles.
- [docs/protocols.md](docs/protocols.md) — the concrete spec of every boundary: client
  API, Redis queue contract, worker↔model API, Wake-on-LAN, registry.
- [docs/deployment.md](docs/deployment.md) — .NET cross-platform build/RIDs, Raspberry Pi
  / arm64, node roles, GPU/Metal notes, wake configuration.
- [docs/decisions.md](docs/decisions.md) — ADR-lite log of the choices and their rationale.
