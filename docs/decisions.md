# Design decisions (ADR-lite)

The reasoning behind the choices in [`../DESIGN.md`](../DESIGN.md), so the *why* isn't
lost. Each entry: the decision, why, and what was considered instead.

## 1. A job queue, not a serving cluster
**Decision:** model the system as a distributed task queue with LLM-aware routing.
**Why:** the defining constraint is a **heterogeneous, intermittent** fleet — machines
sleep, roam, and differ in capacity. That is a queueing problem.
**Considered:** vLLM cluster / Ray Serve / llama.cpp RPC — all assume always-on,
homogeneous nodes, which is exactly what we don't have.

## 2. Pull-based workers, not push dispatch
**Decision:** workers pull jobs from capability queues; nothing pushes to a named worker.
**Why:** no central liveness tracking, and a job is never dispatched to a node that just
slept or left the LAN. Subscription *is* the liveness signal; backpressure is natural.
**Considered:** a dispatcher that tracks live workers and pushes — fragile under
intermittency.

## 3. Queue-and-wait as the default, cloud as the impatience option
**Decision:** when no capable worker is available, patient jobs **wait in the queue**
(and may trigger a wake) rather than automatically failing over to cloud. Each job
carries a patience policy: `wait` / `wait_then_cloud` / `now`.
**Why:** most inference here is not interactive — "ready when I next look." Waiting for a
free, local, private run beats paying for cloud and sending data off-box by default.
**Considered:** always-fail-over-to-cloud (simpler, but costly and leaks data) and
always-wait (no escape hatch for interactive use).

## 4. Address by capability tier, not by machine
**Decision:** jobs request an abstract capability (`70b-reason`), not a host.
**Why:** lets a heterogeneous fleet grow/shrink without touching clients or jobs; routing
is just "which queue has a consumer."
**Considered:** naming target machines — brittle and couples clients to the fleet.

## 5. Adopt LiteLLM for the sync plane; build only the async plane
**Decision:** use LiteLLM (OpenAI-compatible) for route-now/health-check/load-balance/
cloud-fallback; build the durable queue, submit/poll API, coordinator, and worker.
**Why:** LiteLLM already solves synchronous routing well; it has no concept of holding a
job until a machine wakes. Don't reinvent the half that exists.
**Considered:** building our own OpenAI-compatible router — wasted effort.
**Note:** LiteLLM is Python. This was briefly a concern (a Python sidecar duplicating
the coordinator's registry/health/spend state in an otherwise-C# fabric), which the
split language decision resolves — with a **Python server** (ADR 7), LiteLLM is a native
in-process fit rather than a foreign sidecar.

## 6. clusterbuck is domain-agnostic
**Decision:** the fabric sees only jobs, capabilities, and results — never anything about
a client's application.
**Why:** it's reusable infrastructure with many potential tenants; coupling it to one
app's concepts would ruin that. Clients depend on clusterbuck, never the reverse.
**Considered:** baking a first tenant's needs in — rejected to keep it shareable.

## 7. Implementation language: split — Python server, C#/.NET worker
**Decision:** build the **server** (job API, sync front, coordinator, engines, planner)
in **Python**, and the **worker** (pull, call the local model server, probe, self-update,
CLI) in **C#/.NET Native AOT**. They meet only at documented seams (Redis queue contract
+ HTTP), never in shared code.
**Why:** the two halves have opposite needs. The server is the ecosystem-heavy half
(LiteLLM, eval/dataset tooling, provider libs) and runs on **one box you control**, so
Python's distribution weakness doesn't apply and its ecosystem + iteration speed win.
The worker fans out to **every heterogeneous node** and self-updates, needs a lean
single-file binary with no runtime install, and requires **no LLM libraries** — exactly
where Python's distribution weakness bites and .NET AOT shines. The protocol-first design
makes the split clean, and the worker's language stays **reversible** (it could later be
Python if the second toolchain stops being worth it).
**History:** this **revises an earlier all-C# decision**. That reasoning (author fluent
in both C# and Python; .NET AOT for cross-platform distribution) held for the worker but
under-weighted that (a) the LLM ecosystem the *server* needs is Python, and (b) adopting
LiteLLM as a C# sidecar meant a Python process anyway, duplicating coordinator state
(see ADR 5). A Python server makes LiteLLM a native fit and removes that duplication.
**Considered:**
- **All Python** — max ecosystem + velocity, but the worker's fleet distribution /
  self-update / footprint is genuinely worse (interpreter+venv or fragile per-OS bundles;
  Docker can't help Mac inference nodes — no Metal in Docker on macOS).
- **All C#/.NET** — clean single-binary distribution everywhere, but forces reimplementing
  the Python LLM ecosystem (gateway → Microsoft.Extensions.AI + registry; build the eval
  harness) for no gain on the one-box server.
- **Go / Rust** — strong single-binary stories, but zero/low author experience and no
  ecosystem edge over the split.

## 22. Cross-language contract via JSON Schema source of truth
**Decision:** because server (Python) and worker (C#) can't share a type library, the
wire contract lives as a machine-readable **JSON Schema in `contract/`** (job, result,
enrollment, heartbeat, reservation, attention, update-manifest), with a **conformance
test on each side** asserting round-trip agreement.
**Why:** the split (ADR 7) gives up the compile-time shared-types safety a single language
would have had; a schema + two conformance tests restores it — the type definitions
cannot silently drift, and [protocols.md](protocols.md) gains a machine-checkable
counterpart.
**Considered:** docs-only parallel types (drift); code-generating both sides from the
schema (viable later; hand-written types + conformance tests are simpler to start).

## 8. Every boundary is a documented protocol
**Decision:** define the client API, the Redis queue contract, the worker↔model API, and
the wake mechanism as protocols (see [protocols.md](protocols.md)).
**Why:** keeps the system polyglot and open — the C# worker is a *reference*
implementation, not a constraint; clients and model servers stay any-language/any-OS.
**Considered:** a single-language in-process design — simpler short-term, closed
long-term.

## 9. Dynamic registry via enrollment + heartbeats (static YAML as seed only)
**Decision:** nodes self-enroll (join token → hardware probe → proposed capability set)
and maintain live state via heartbeats; `fleet.yaml` remains only the MVP seed.
**Why:** a fleet that grows machine-by-machine shouldn't need hand-edited config; the
probe (RAM, accelerator, disk, micro-benchmark) grounds capability assignment in measured
reality. The heartbeat also carries *installed vs loaded* models, resolving cold-load-aware
routing.
**Considered:** static config forever — fine for 2 nodes, hostile at 5+.

## 10. Machine profiles + presence-mode model ladder
**Decision:** every node carries an owner-set profile (`dedicated`/`shared`/`background`)
and a mode-driven model ladder: small model while the user is `active`, large model when
`away`, with hysteresis and instant owner eviction.
**Why:** most spare compute lives on machines people actually use; clusterbuck is a guest
there. Making the owner's contract explicit (and reclaim instant) is what makes
contributing a personal machine acceptable. Hysteresis exists because cold-loads are
expensive; eviction is cheap because unloading is fast.
**Considered:** fixed per-node model sets — wastes the away hours of big machines.

## 11. Usage metering in scope (supersedes the earlier non-goal)
**Decision:** the coordinator logs per-job usage (tokens in/out, model, node, queue wait,
run time, cost — cloud actual, local nominal/energy) with rollups and a `/usage` endpoint.
**Why:** needed for the planner ("which models earn their RAM"), cloud budget governance,
and plain visibility. The original non-goal ("local compute is free") conflated *billing*
with *metering*; billing/chargeback stays out of scope.
**Considered:** keeping accounting out entirely — untenable once cloud spend exists.

## 12. Two participation modes: managed worker vs attached endpoint
**Decision:** a node either runs the full worker agent, or is an **attached endpoint**
(model server only, zero fabric code) driven by a coordinator-side **proxy worker** that
pulls from queues on its behalf.
**Why:** resolves the tension between the pull-based worker model and machines where
policy forbids installing fabric code (corporate/MDM devices, appliances). The queue
contract is preserved; the proxy just relocates the puller.
**Considered:** requiring the agent everywhere (excludes policy-bound machines) or
sync-plane-only participation for them (they could never drain the async queue).

## 13. Signed, canaried worker self-update
**Decision:** coordinator-hosted release manifest; agents verify a pinned-key signature,
canary ring first, auto-rollback on crash-loop, protocol-version skew gating, per-node
opt-out.
**Why:** "update the fleet by touching every box" doesn't scale past two machines; but an
update channel is remote-code-execution by design, so it ships signed-or-nothing with a
blast-radius limiter (canary) and an undo (rollback).
**Considered:** manual updates (doesn't scale), unsigned pull-from-git (unacceptable RCE
surface).

## 14. Per-job privacy classes bounding all cloud routing
**Decision:** every job carries `privacy: local_only | cloud_ok`, **defaulting to
`local_only`**; no policy, overflow, or deadline pressure may ever route a `local_only`
job off-LAN.
**Why:** the moment cloud fallback/overflow exists, "it was busy so it leaked" becomes
possible; privacy must be a per-job invariant, not an operator setting. Default-private
because the whole point of the fabric is local-first inference.
**Considered:** a global cloud on/off switch — too coarse; interactive public-data jobs
and sensitive batch jobs coexist in the same fleet.

## 15. Ability = per-task-class matrix on an anchored 1–10 scale
**Decision:** model quality is scored as `ability(artifact, task_class)` — artifact =
model + quantisation — measured by three tiers (programmatic checks → checklist judging
→ pairwise preference aggregated Bradley-Terry/Elo) and calibrated to 1–10 via pinned
**anchor artifacts** with a versioned scale. A workload-weighted headline scalar exists
for humans; the router uses the matrix.
**Why:** model quality is jagged — one number per model routes jobs wrongly. Pairwise
judging is far more stable than absolute scoring; programmatic checks are free and
unarguable where outputs are verifiable; anchoring makes the number legible; scale
versioning stops "8" deflating as the frontier moves.
**Considered:** a single per-model score (hides jaggedness); raw Elo exposed to users
(meaningless without anchors); reference metrics like ROUGE (poor signal for modern
LLMs); reusing public benchmarks (contaminated — models have memorised them).

## 16. Need-shaped client addressing: task_class + min_ability
**Decision:** the preferred job-addressing form is `{task_class, min_ability}` resolved
by the coordinator (filter by ability + privacy → prefer local → cheapest → fastest);
explicit capability tiers remain as advanced/internal addressing. Cloud models join the
same catalog with measured ability and per-token pricing under registered provider
accounts and a paced monthly budget.
**Why:** clients should state their *need*, not the fleet's hardware shape — naming
"32b" leaks supply-side detail into every client and breaks when the fleet changes.
Uniform ability + price across local and cloud enables the planner's cost-quality
arbitrage ("this cloud spend could be a local artifact clearing your min_ability").
**Considered:** capability-only addressing (couples clients to hardware tiers);
per-client model pinning (defeats fleet evolution).

## 17. Workload reservations: cold by default, warm by appointment
**Decision:** the fleet's resting state is asleep/unloaded; anticipated demand is served
by **reservations** — a client declares shape (task class, min ability, load class,
duration, priority, window/recurrence), the coordinator admission-checks against the
registry and answers confirmed / counter-offer / declined, wakes the node and
**pre-loads the artifact before the window opens**, drains the batch, then everything
returns to sleep. Reservations are soft commitments: owner eviction (ADR 10) always
wins, and the coordinator re-plans (another node, cloud if `cloud_ok`, or slip-and-notify).
**Why:** keeping workers hot "just in case" wastes the RAM and power the fabric exists
to conserve, while purely reactive wake pays the cold-load cost on the first job of
every batch and gives clients no predictability. Booking moves the cold-start off the
critical path and gives the planner forward-looking demand (recurrence → align with
owner wake windows and cheap-tariff hours).
**Considered:** always-hot workers (wasteful, hostile to shared machines); purely
reactive queue-depth wake only (cold-start latency, no ETA, no batch alignment); hard
SLA-style reservations (unkeepable promises on a home fleet of roaming machines).

## 18. Urgency as a trajectory: urgent / necessary / waitable + escalation
**Decision:** jobs carry an urgency class — `urgent` (client blocked / user waiting),
`necessary` (prompt but non-blocking), `waitable(N)` (backlog) — each with its own
**wake rights** (urgent may wake immediately; necessary may on-demand wake; waitable
never wakes, riding existing warmth). Escalation is first-class: waitable promotes to
necessary on **age** (`escalate_after_min`), a **backlog watermark**, or a client
**attention lease** (user became active → that client's pending work heats up, TTL'd,
demoting gracefully on expiry). Promotions **coalesce into warm windows** (implicit
reservations), never per-job wake stampedes. This supersedes the static patience
policy vocabulary of ADR 3 (`wait`/`wait_then_cloud`/`now` map to waitable(∞)/
waitable(N)+cloud_ok/urgent) while preserving its intent; privacy (ADR 14) still
bounds cloud at every urgency.
**Why:** a static label conflates deadline with cloud-willingness and cannot express
how background pipelines actually behave — lazy until it matters, where "matters" is
time passing, a backlog growing, or a user showing up. Tying wake rights to urgency is
what keeps the fleet cold-by-default (ADR 17) while monitors churn; leasing attention
prevents a stuck-escalated fleet.
**Considered:** static priorities (no aging → starvation or permanent over-provision);
keeping patience + urgency side by side (two overlapping knobs); clients re-submitting
jobs at higher priority (racy, duplicates work).

## 19. Worker as a Native AOT single-file binary
**Decision:** ship `cbk` (worker + probe + updater + CLI) as a Native AOT single-file
binary per RID.
**Why:** the worker fans out across the fleet and self-updates, so it must be lean,
dependency-light, and instant-start — AOT's sweet spot — and it needs no LLM libraries.
**Superseded in part by ADR 7:** this ADR originally also specified the *server* as a
self-contained **JIT C#** artifact sharing a `Clusterbuck.Core` library with the worker.
The split-language decision (ADR 7) makes the server **Python**, so there is no JIT-C#
server and no shared code project — the AOT/JIT distinction now applies only to the
worker (AOT), and the cross-language contract replaces the shared library (ADR 22). The
worker-AOT rationale here still stands.
**Considered:** framework-dependent worker (needs a .NET runtime on every node); a JIT
worker (start-up + distribution regress on every node).

## 20. Redis Streams + consumer groups for the queue (not BLPOP lists)
**Decision:** implement the queue contract on Redis Streams with consumer groups.
**Why:** the pending-entries list + `XAUTOCLAIM` provide the visibility-timeout / reaper
/ at-least-once semantics the protocol needs *natively* — the "laptop closed its lid
mid-job" recovery — instead of hand-rolling in-flight tracking and a reaper over plain
`BLPOP` lists. Redis stays the broker; SQLite is the durable system of record.
**Considered:** `BLPOP` lists (simple, but re-implements reliability primitives Streams
already ship); a real queue broker like RabbitMQ (heavier, another daemon on the
always-on node, no gain at this scale).

## 21. Dashboard: htmx over coordinator-served JSON APIs
**Decision:** the web dashboard is server-rendered static assets + htmx calling the
existing JSON endpoints, vendored (no CDN), served by `cbk-server`.
**Why:** no separate frontend build chain on an infrastructure repo; reuses the APIs
that already exist for the CLI and clients; LAN-only so vendoring is natural. Blazor
remains the C#-native upgrade path if real interactivity is later needed — available
because the server is JIT, not AOT (ADR 19).
**Considered:** a React/Vite SPA (build chain + dependency mass unwarranted for an admin
panel); Blazor from the start (heavier than needed for mostly-readonly views, but the
sanctioned upgrade).

## 23. Sync-plane cloud control is coarse (config-level), not per-request
**Decision:** on the sync plane (the OpenAI-compatible `/v1/chat/completions`), cloud
fallback is a **server-config** setting (`CBK_CLOUD_FALLBACK_MODEL`), **default off** ⇒
local-only. It is *not* a per-request invariant the way async jobs carry `privacy`
(ADR 14).
**Why:** the standard OpenAI request shape has no privacy field, and clusterbuck stays a
drop-in for any OpenAI SDK (protocols.md §1a) — so we can't demand a per-request privacy
class there without breaking compatibility. Defaulting fallback **off** keeps the sync
plane local-only unless an operator deliberately enables cloud, which is the safe cut.
The strong per-job `local_only` guarantee remains where the shape allows it: the async
job API. Recorded so the asymmetry is a deliberate design choice, not an oversight.
**Considered:** a custom header (`x-cbk-privacy`) to carry per-request privacy on the sync
plane (viable later; unneeded while fallback defaults off); mirroring async's default-
`local_only` per request (impossible without a field in the request shape).

## 24. M2 escalation grants wake rights; intra-queue priority ordering deferred
**Decision:** in M2, a job's urgency governs its **wake rights and escalation trajectory**
(ADR 18) — `urgent`/`necessary` may wake a node; `waitable(N)` promotes to `necessary`
on age — but does **not** reorder jobs *within* a capability's Redis stream. A running
worker drains its stream FIFO regardless of urgency; strict "necessary = head of the
queue" is a later refinement.
**Why:** the observable effect of urgency on an intermittent fleet is *whether capacity
gets created* (wake), which is what M2 (availability) is about. True intra-queue priority
needs urgency-tiered streams per capability (`q:<cap>:urgent` consumed ahead of
`q:<cap>`), which changes the queue topology and the worker's read loop — a contract-level
change out of proportion to its value while the fleet is small and jobs mostly wait on
*capacity*, not on *each other*. Deferring it keeps the M0 queue contract stable.
**Consequence:** once a worker is awake, an escalated `necessary` job is served in
submission order alongside `waitable` work on the same stream. Acceptable at M2 scale;
revisit when a single warm node routinely has mixed-urgency backlog contending.
**Considered:** tiered streams now (topology + worker churn, low payoff at this scale);
a Redis sorted-set priority queue (abandons the Streams reliability primitives ADR 20
adopted); reordering in the worker (can't — it can't see the whole stream cheaply).
