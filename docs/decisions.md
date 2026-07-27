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
wire contract lives as a machine-readable **JSON Schema in `contract/`**, with a
**conformance test on each side** asserting round-trip agreement.
**Scope in practice:** only the seams the C# worker actually parses or produces are in
`contract/` — job, result, enroll request/response, heartbeat request/response,
update-manifest. Reservations and attention are *server-only* HTTP shapes that no worker
touches, so they stay Pydantic models rather than shared schemas; adding them would imply a
cross-language contract that does not exist.
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
remains a possible upgrade path if real interactivity is later needed, though it would mean
a second runtime beside the Python server (this ADR originally justified it by the server
being JIT C#, which ADR 7 superseded).
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

## 25. Model discovery is API-first; installation needs a vendor adapter
**Decision:** a worker learns its **installed** models from the **generic OpenAI
`GET /v1/models`** endpoint (which Ollama, LM Studio, vLLM and llama.cpp-server all serve),
*not* by scanning vendor model directories. **Installing/removing** a model, however, has
no OpenAI-standard equivalent, so it lives behind a small pluggable **model-manager
adapter** (`ollama` today; `none` = manual), kept strictly separate from the inference path.
`loaded` (warm now) and per-artifact **digests** also need the adapter and degrade to
"unknown" without it.
**Why:** discovery over the documented wire protocol keeps one portable code path and
honours the no-vendor-SDK rule (ADR 8, protocols.md §3). Filesystem scanning is *worse*
coupling than an SDK — Ollama's store is a content-addressed blob dir with manifests, an
undocumented internal layout that can change, and a blob the running server hasn't
registered isn't servable anyway. Installation is the honest exception: "pull a model" is
inherently vendor-specific (Ollama `POST /api/pull`; llama.cpp has no concept of it; vLLM
fetches at launch), so rather than pretend otherwise we isolate it in one swappable seam and
keep the hot path vendor-neutral.
**Consequences:** the coordinator **decides** (it holds the catalog, ability matrix, demand
and budget) and the worker only **executes** an approved action — keeping the worker lean
(ADR 19) and policy centralised. Because ability is pinned to an artifact (ADR 15), a
changed digest or a fresh install means the stored score is **not** inherited: it earns a
re-measurement. Installs are quota-bounded (the owner's disk contract, ADR 10) and
presence-gated (no multi-GB transfer under an active owner); reclaim proposals exist so
install and GC ship together, and reclaim is **never** auto-approved.
**Considered:** filesystem scanning as primary (undocumented layouts, unservable blobs);
requiring the operator to hand-list models forever (the hostility ADR 9 rejects); putting
install decisions in the worker (duplicates catalog/ability state on every node).

## 26. One operator shared secret, off by default
**Decision:** the coordinator's HTTP surface is gated by a single **operator shared secret**
(`CBK_API_KEY`), presented as `X-CBK-Api-Key`, `Authorization: Bearer`, or a `cbk_key`
cookie. **Unset ⇒ auth is disabled**, logged as a warning at startup. Two paths are exempt
because they carry their own credential and are how a node bootstraps: `POST /nodes/enroll`
(one-time join token, burned on use) and `POST /nodes/{id}/heartbeat` (per-node key);
`/healthz` and `/static/*` are exempt as a probe and vendored assets.
**Why:** DESIGN.md's security section always called for "a shared key at minimum on the
gateway/queue" and it was simply never built — an audit found a complete anonymous chain
from `POST /nodes/tokens` through `auto_approve` to an approved multi-GB model pull on
another owner's machine, and to `reclaim` deleting an owner's model files. clusterbuck is
LAN-only single-operator infrastructure, so one secret is the proportionate control: there
are no tenants to distinguish, and per-node keys already exist for the machine-side seam.
Default-off keeps local dev and the e2e scripts working unchanged, which is why the missing
key is *warned* rather than silently tolerated.
**Consequences:** the worker loop needs no operator secret (its two endpoints are exempt),
so only the admin CLI verbs (`submit`, `status`, `fleet`) present the key. A key in a URL
can leak via logs and `Referer`, so `/?key=…` is a one-time browser affordance that
exchanges it for an HttpOnly cookie, not the recommended path. This is authentication, not
authorisation: any holder of the key is the operator. **Redis itself is a separate
exposure** — it holds prompts and completions in plaintext and needs its own
`requirepass`; the worker now carries a password through `redis://:secret@host`.
**Considered:** per-client API keys (no tenants to separate, and it would invite treating
metering as billing, which ADR 11 excludes); mTLS (correct for a hostile network, dead
weight on a home LAN); binding to loopback only (breaks the whole point — other machines
must reach the coordinator); leaving it open because the network is private (the destructive
reclaim path and the cloud-key-burning sync plane make "private" too thin a guarantee).

## 13a. Self-update: what is built, and what is deliberately not (amends ADR 13)
**Built and proven end-to-end** by `deploy/e2e/selfupdate.sh` against two REAL published
single-file binaries: the coordinator signs a release manifest per platform and offers it on
the heartbeat; the worker verifies the signature against a **pinned public key**, fetches,
verifies the sha256 of what actually arrived, retains the outgoing binary as `cbk.prev`,
swaps itself and re-execs. Order matters and is the security boundary — the signature covers
the `url`, so it is checked **before** any fetch, which means a redirected download is refused
without ever contacting the attacker's host.
**Three refusals are proven, not assumed:** no pinned key ⇒ refuse outright (signed-or-nothing,
there is no "trust once" path); a tampered digest ⇒ refuse with the binary untouched; a node
that has not opted in ⇒ never even offered an update. `auto_update` is per-node and off by
default, mirroring `auto_approve` for model installs, because an update channel is RCE by
design.
**Deliberately NOT built:** canary rings and automatic crash-loop rollback. `cbk.prev` makes
rollback possible and `UpdateApplier.Rollback()` performs it, but *deciding* that a release is
crash-looping requires observing several nodes over time, which cannot be honestly verified on
one machine. Manual rollback is the honest half; the automatic half is unimplemented rather
than faked.
**Consequence to remember:** the worker pauses claiming before swapping itself, so an update
never strands a claimed job — and if the swap fails midway the old binary is put back rather
than leaving the node with none.

## 28. Ship the worker as self-contained single-file, not Native AOT (revises ADR 19)
**Decision:** release `cbk` as a **self-contained single-file** binary per RID, built by a
GitHub Actions matrix covering `win-x64`, `win-arm64`, `osx-arm64`, `osx-x64`, `linux-x64`
and `linux-arm64`. Native AOT stays the aspiration, not the shipping format.
**Why:** ADR 19 specified Native AOT, and the first actual `PublishAot=true` run — never
attempted until now — **fails for two independent reasons**:
1. The macOS link line requires `-lssl -lcrypto`, and Apple no longer ships OpenSSL as a
   linkable library (the platform's crypto is Security.framework), so `ld: library 'ssl' not
   found`. Fixable with a Homebrew OpenSSL and linker flags, but that is a build-host
   dependency on every macOS builder.
2. **`Spectre.Console.Cli` is not AOT-safe** — it emits IL2104/IL3053 trim and AOT-analysis
   warnings because its command binding is reflection-based, and IL3000 for
   `Assembly.Location` under single-file. Warnings here mean the CLI could fail at *runtime*
   after a clean link, which is the worst failure shape.
Self-contained preserves the property that actually motivated ADR 19 — **no .NET runtime
installed on the node** — for a heterogeneous fleet. It costs binary size (~72–79 MB vs an
expected ~15–25 MB) and gives up AOT's instant start and smaller baseline heap. Note ADR 19's
footprint target was *resident memory*, not artifact size, so the regression is on disk and
cold start rather than the thing the target named.
**Verified, not assumed:** all six RIDs were cross-published from a single macOS host,
producing genuine `PE32+ (x86-64)`, `PE32+ (Aarch64)`, `Mach-O x86_64`, `ELF x86-64` and
`ELF aarch64` executables; the osx-arm64 artifact runs and parses CLI arguments. Because
self-contained cross-publishes cleanly, one CI runner builds every platform — but each
artifact is still smoke-tested on its native OS, since "it linked" and "it runs" are
different claims.
**To get AOT later:** replace Spectre.Console.Cli with a small hand-rolled parser (the CLI
surface is seven verbs, so this is modest), then resolve the macOS OpenSSL link. Until both
are done, `IsAotCompatible=true` in the csproj is an analyzer setting, not a shipping claim.
**Considered:** framework-dependent (smallest artifact, but requires a .NET runtime on every
node — exactly what ADR 19 rejected); fixing AOT first (blocks all releases on a CLI rewrite);
per-OS native runners for every RID (unnecessary once cross-publish was shown to work, and
slower).

## 27. The coordinator judges worker *fitness*, not just protocol compatibility
**Decision:** every worker reports a **build-stamped** `agent_version` (read from the
assembly, not a constant) plus its `protocol_version` on each heartbeat. The coordinator
assesses it against three knobs — `current` (behind ⇒ **stale**, flagged but serving),
`minimum` (below ⇒ **quarantine**), and a **block-list** of specific releases (⇒
**quarantine**) — and returns a `fitness` verdict the worker honours by ceasing to claim
jobs. Unset policy ⇒ everything is `ok`, so a fleet works before an operator has opinions.
Version and verdict are shown on `/nodes` and the dashboard.
**Why:** protocols.md §7 only ever gated *protocol skew*, and even that was unimplemented —
the worker sent `protocol_version` and the server parsed and discarded it. But protocol
compatibility is **necessary, not sufficient**: a worker can speak the contract perfectly and
still carry bugs that produce plausible-looking wrong results. The concrete case from this
codebase: a worker predating the `params.model` requirement ignores the artifact pin, so eval
jobs are measured on whatever model that node defaults to and **ability scores are attributed
to the wrong artifact**, silently corrupting routing. No protocol check catches that.
The **block-list is the load-bearing knob**, because **bugs are not monotonic** — 1.4.2 can be
broken while 1.4.1 and 1.4.3 are fine, which a floor cannot express. Version *visibility* also
matters independently: before this, `GET /nodes` showed hardware and models but not what code
a node ran, so a three-week-old worker looked identical to a fresh one.
**Consequences:** an unfit worker is also denied model-management actions — a build that may
misbehave should not be installing multi-GB weights. Unparseable versions are treated as
`stale` normally, but `quarantine` when a floor is declared: a worker that cannot be *shown*
to meet the floor fails it rather than being assumed adequate.
**Enforcement is COOPERATIVE, and that is a real limit.** Workers claim straight from Redis
(ADR 2, pull-based), so the coordinator cannot hard-block one that ignores its verdict — it
can only withhold what it controls and ask the worker to stand down. That is adequate for the
threat model here (buggy builds, not hostile ones). Hard enforcement would require per-node
Redis credentials issued at enrollment and revocable on quarantine, which would also give
Redis auth a purpose it currently lacks; noted as the upgrade path, not built.
**Considered:** protocol-version gating alone (cannot express "this build is buggy"); a
minimum-version floor alone (cannot express non-monotonic bugs); refusing enrollment to unfit
workers (too late — the fleet's problem is *running* workers that drifted); hard-failing
heartbeats from unfit workers (loses the telemetry that shows you the drift, and gives the
worker no instruction).
