# Fleet management & lifecycle

How clusterbuck grows from a statically-configured broker into a **self-managing
fleet**: nodes enroll themselves, adapt to how their owners use them, and the
coordinator keeps the network's model mix matched to the workload it's actually asked
to serve. See [`../DESIGN.md`](../DESIGN.md) for the core design; everything here layers
on top of it without changing the queue/planes model.

**Terminology:** a *client* is an application submitting jobs. The machine-side install
is the **worker agent** (worker + probe + updater). This document is about nodes.

## Node lifecycle: install → enroll → serve

1. **Install** the worker agent (self-contained binary per platform — see
   [deployment.md](deployment.md)).
2. **Discover** the coordinator: mDNS/DNS-SD (`_clusterbuck._tcp`) on the LAN, with a
   manual URL fallback for networks where multicast is blocked.
3. **Enroll**: present a **one-time join token** (minted by the coordinator's admin);
   receive a node identity + per-node key used for all subsequent auth.
4. **Probe** the hardware: RAM (and unified-memory/VRAM), CPU arch, OS, accelerator
   (Metal / CUDA / CPU-only), free disk, and an optional micro-benchmark (tokens/sec on
   a tiny model) to calibrate real throughput rather than guessing from specs.
5. **Propose capabilities**: the coordinator maps probe + profile (below) to a suggested
   model set and capability queues. The owner confirms or edits — the proposal is a
   default, not a mandate.
6. **Serve**: subscribe to the agreed queues, start heartbeating.

## Participation modes

- **Managed worker** — runs the full agent: enrollment, heartbeats, model management,
  self-update. The normal case.
- **Attached endpoint** — a machine that may run **no clusterbuck code** (e.g. a
  corporate/MDM-managed device, or an appliance): it hosts only an OpenAI-compatible
  model server. The coordinator runs a **proxy worker** on its behalf — a worker process
  on the coordinator that pulls from the appropriate queues and drives the endpoint over
  HTTP. Health checks stand in for heartbeats; Wake-on-LAN still applies if its MAC is
  registered. This keeps the pull model intact without putting fabric code on machines
  where policy forbids it.

## Machine profiles: the owner's contract

Every node carries a profile describing how much of the machine clusterbuck may take.
Presets, all knobs overridable:

| Preset | Meaning |
|---|---|
| `dedicated` | The machine exists to serve; use whatever fits. |
| `shared` | A person uses this machine; clusterbuck is a background tenant. |
| `background` | Minimal footprint only — small models, idle-time work. |

Knobs: RAM cap per mode (below), allowed hours, battery rule (on battery → pause or
lighten), disk quota for model storage, and thermal backoff.

**Fast eviction:** the owner can always reclaim the machine instantly — a pause command
(or hotkey) aborts/finishes the current job, unloads the model (seconds — freeing RAM is
cheap; loading it is what's slow), and stops pulling. The visibility timeout returns any
aborted job to its queue; nothing is lost.

## Presence modes & the model ladder

A shared machine serves different models depending on whether its user is around:

- **Modes:** `active` (user present) / `away` (user gone) / `paused` (owner opt-out).
  Detected from screen lock and input idle time, or set manually; mode is carried in the
  heartbeat.
- **Model ladder:** per-node mapping from mode → model set. Example: `active` → a small
  resident model only; `away` → swap in the large RAM-hungry model and subscribe to the
  heavier capability queues.
- **Hysteresis:** cold-loading a big model costs tens of seconds, so mode changes are
  damped — climb the ladder only after N minutes stably away; descend immediately on
  user return (eviction is fast). Never thrash on a coffee break.

Queue subscriptions follow the ladder: a node advertises `q:70b` only while in a mode
whose model set can serve it. To the queue this is indistinguishable from a machine
waking/sleeping — the same one-queue/many-policies idea, extended to *partial*
availability.

## Dynamic registry

The static `fleet.yaml` (see [protocols.md](protocols.md)) seeds the fleet; the live
truth is the coordinator's **registry**, built from enrollment + heartbeats:

- per node: profile, mode, health, **installed** models vs **currently loaded** models,
  queue subscriptions, rolling throughput stats.

The installed/loaded distinction matters on RAM-limited nodes: "installed" is cheap,
"loaded" is what avoids a cold start. It enables **model-affinity scheduling** — prefer
routing a job to a node with the model already warm, and batch same-model jobs together
so one load is amortised over many jobs instead of thrashing swaps.

## Workload awareness & the fleet planner

clusterbuck should know what is asked of it, so the fleet can be shaped to fit:

- **Demand side:** clients may tag jobs with a task class (`extract` / `summarize` /
  `reason` / `code` / `embed`), typical context length, and latency class. Untagged jobs
  still count via their capability. The coordinator keeps a demand histogram: which
  capabilities are used, how often, how long jobs wait.
- **Supply side:** the registry knows every node's hardware, profile, and ladder.
- **The planner** compares the two and makes *suggestions*, not silent changes: "install
  model X on node N", "queue `q:32b` waits 40 min on average — the workstation's away-mode
  ladder could add it", "model Y hasn't served a job in 60 days — reclaim 9 GB of disk".

### Model catalog & upgrade watching

The coordinator maintains a **model catalog** — a curated list of known-good models per
capability, with size/quantisation/licence metadata (sourced from release feeds and
registries; curated by the admin at first). New releases produce **upgrade proposals**,
gated by three checks before anything changes:

1. **Fits** — the candidate fits the target node's hardware and profile (RAM at the
   relevant mode, disk quota).
2. **Eval gate** — it passes a small task-representative eval suite (prompts drawn from
   the fleet's actual task classes) at least as well as the incumbent; optionally
   shadow-test on a fraction of real jobs and compare.
3. **Approval** — a human approves the download (multi-GB weights are never fetched
   silently; per-node auto-approve is an explicit opt-in).

The catalog also lists **cloud models**: a capability with no viable local host (e.g. a
frontier-class tier) can be declared cloud-backed from the start.

## Cloud tier: fallback, overflow, privacy, budget

Cloud participation grows from "fallback when the fleet is away" to three distinct uses:

- **Unavailability** — the existing patience policies (`wait` / `wait_then_cloud` / `now`).
- **Overflow** — when the fleet is up but **overwhelmed**: if a job's projected queue wait
  exceeds its deadline (estimated from queue depth × rolling service times), spill to
  cloud rather than blow the deadline.
- **Bigger-than-local** — capabilities that only cloud models can serve, routed there by
  the catalog.

Two governors apply to all of it:

- **Privacy class** — every job carries `privacy: local_only | cloud_ok`, defaulting to
  `local_only`. A `local_only` job **never** leaves the LAN regardless of patience,
  overflow pressure, or deadline; if it can't be served locally in time it waits or
  fails explicitly. Submission validation rejects incoherent combos (e.g. `local_only`
  + a cloud-backed capability) fast, at the API.
- **Budget** — a monthly cloud spend cap with alert thresholds; per-client quotas later.
  When the cap is hit, overflow degrades gracefully back to queuing.

## Usage accounting

The coordinator logs every job (local and cloud):

```
usage_record = {
  job_id, timestamp, client_key, task_class?,
  capability, model, node | "cloud:<provider>",
  tokens_in, tokens_out, queue_wait_ms, run_ms, outcome,
  cost,          # cloud: actual $; local: nominal per-token rate or energy estimate
}
```

Rollups per model / node / client / day feed a `/usage` endpoint (and export). Raw
records age out (e.g. 90 days); rollups are kept. This is **metering, not billing** —
the goal is visibility ("which models earn their RAM", "what would this have cost in the
cloud") and planner input, not chargeback.

## Worker self-update

Changing the code shouldn't mean touching every machine:

- The coordinator hosts a **release manifest**: version, artifact per platform (RID),
  SHA-256, and a **signature**. Agents check it on heartbeat.
- **Signed or nothing:** an update channel is remote-code-execution by design, so agents
  verify the signature against a pinned public key before touching a byte. Guard the
  signing key accordingly.
- **Canary rollout:** one node updates first; only after it heartbeats healthy for a
  grace period does the rest of the fleet follow. A crash-looping update auto-rolls back
  to the retained previous binary.
- **Version skew:** the queue contract carries a protocol version; adjacent versions
  interoperate, and a worker that finds itself too far behind pauses pulling until it
  has updated.
- **Opt-outs:** per-node `auto_update: false` for machines where policy requires manual
  updates. Attached endpoints have no agent to update at all.

## Further ideas (unscheduled)

- **Observability dashboard** — queue depths, node modes, tokens/day, spend vs budget,
  model warm/cold state at a glance.
- **Energy-aware scheduling** — prefer scheduled-wake windows aligned to cheap-tariff
  hours for deep batch work.
- **Disk GC** — planner-driven reclamation of models that no longer earn their storage.
- **Response cache** — dedupe identical (or semantically near-identical) requests before
  they reach a model at all.

## Phasing

None of this displaces the MVP (static `fleet.yaml`, one worker, submit/poll, LiteLLM
front). The order of value afterwards is roughly: dynamic registry + heartbeats →
enrollment + probe → profiles/presence ladder → accounting → self-update → planner +
catalog → overflow/budget governors.
