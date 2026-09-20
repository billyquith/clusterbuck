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
> **Superseded by ADR 34.** Its own revisit condition — "a single warm node routinely has
> mixed-urgency backlog contending" — was met: a client's urgent probe waited ~15 minutes
> behind a `waitable` backlog on one warm worker, and the fleet docs had meanwhile been
> promising the priority this ADR deferred.

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
> **Superseded by ADR 29.** This ADR's central justification — that self-contained
> "still needs no .NET runtime on the node, which is the property that actually matters" — was
> later measured and found **false on macOS**: the binary hard-links Homebrew's brotli and
> `dyld` refuses to launch it without it. The .NET worker was replaced by the Python zipapp
> (ADR 29); the tag `worker-dotnet` preserves the C# implementation.

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
**Update — the Spectre half is done, the link half is not.** Spectre.Console.Cli has been
replaced by a hand-rolled dispatcher (`Cli.cs`, ~130 lines for seven verbs). That removed
**every** IL2104/IL3053/IL3000 trim and AOT warning, and AOT now compiles all the way to the
native link. Measured side benefits on the shipping self-contained build: 74 MB → 73 MB, and
resident **67 MB → 59 MB** (-12%), with one fewer dependency and no reflection in the CLI path.

The remaining blocker is **environmental and worse than it first looked**. The macOS AOT link
line requests `-lssl -lcrypto -lbrotlienc -lbrotlidec -lbrotlicommon -licucore`, several of
which Apple does not ship. Pointing the linker at Homebrew's OpenSSL (`LinkerArg` with
`-L$(brew --prefix openssl@3)/lib`) resolves `ssl`/`crypto` and then fails on `brotlienc` — a
cascade. Chasing it was abandoned deliberately, for a reason beyond tedium: **linking against
Homebrew dylibs would make the binary depend on Homebrew at runtime**, which destroys the
self-containment that is the entire point. Static-linking those `.a` files might work but is
fragile and unproven.

So `IsAotCompatible=true` remains an analyzer setting, not a shipping claim — but the *code*
is now AOT-clean, and the obstacle is purely the macOS native link. **Untested and the obvious
next probe: `linux-x64` AOT**, where these libraries are normally present via distro packages.
CI could answer that on ubuntu-latest without touching a developer machine. If Linux AOT works,
the honest shape may be AOT on Linux and self-contained on macOS.
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

## 29. One `py3-none-any` artifact, because the .NET bundle was not portable
**Decision:** the worker ships as a single **`py3-none-any` zipapp** (`cbk.pyz`, ~2.8 MB, at
`worker/`). Nodes declare `agent_flavour: python` on each heartbeat, and the coordinator
selects the release artifact from that. The C#/.NET implementation was removed (tag
`worker-dotnet` preserves it).

**Why.** ADR 28 accepted self-contained single-file on the grounds that it "still needs no
.NET runtime on the node, which is the property that actually matters". That claim turned out
to be false on macOS. `otool -L` on the published binary shows hard load commands against
`/opt/homebrew/opt/brotli/lib/libbrotli{dec,enc}.1.dylib`, because Apple does not ship brotli
and .NET linked whatever was on the build machine. Repointing those paths on a copy and
running it gives `dyld: Library not loaded` — the process does not start. So the 73 MB
"self-contained" artifact silently required Homebrew on every target Mac, and CI could not have
caught it because GitHub's macOS runners have Homebrew too. Native AOT remains dead for the
same underlying reason (`-lssl -lcrypto -lbrotli*` are not linkably present).

The Python worker sidesteps the whole category: there is no native link step, so there is
nothing to be accidentally non-portable about. **One artifact replaces six**, at 2.8 MB instead
of 72–81 MB — a 26× reduction that also makes self-update a small download rather than a 73 MB
one on every node.

**Consequences.**
- Nodes need Python 3.11+. That is the honest trade for dropping 73 MB and five artifacts: a
  runtime the machine almost certainly already has, versus a bundled one that was not as
  self-contained as advertised.
- **`cryptography` is deliberately not vendored** into the zipapp. It is the update *verifier*,
  so it cannot be delivered through the channel it secures; and it is the only dependency with
  compiled wheels, which would make the artifact platform-specific and defeat the point.
  Absent, self-update refuses and inference is unaffected. The worker reports that case
  distinctly from a bad signature — conflating them sent an operator hunting for a key problem
  when the fix was one `pip install`.
- **Cross-platform coverage is honest, not total.** CI runs the Python unit suites on Linux,
  macOS and Windows (so `probe.py`'s per-OS memory query and the CLI/verify paths are exercised
  on each), and the full Redis-backed loop suite on Linux. But the self-update **re-exec** is
  proven only on POSIX: the end-to-end `selfupdate-py.sh` runs on Linux/macOS, and the unit
  test monkeypatches `os.execv`. On Windows `execv` does not replace the process image the way
  it does on POSIX, so the swap-then-re-exec step is **unverified on Windows** — a known gap,
  not a claim. A node is expected to be Linux or macOS.

**Considered:** rewriting in Go (one static binary, ~10 MB, genuinely no runtime — the best
technical answer on artifact properties alone, but a third language for a project whose stated
value is that every seam is a documented protocol); a pure-Python ECDSA implementation so
`cryptography` could be vendored (trades an audited implementation for packaging convenience on
precisely the RCE boundary — rejected); shipping the Python worker as a wheel installed into a
venv per release (more moving parts than a file swap, and `pip` would fetch unsigned
dependencies at update time, so the signature would no longer cover everything that runs);
linking Homebrew's dylibs explicitly to unblock AOT (makes the binary require Homebrew at
runtime — the exact defect, formalised).

## 30. Cloud provider accounts are executed by the coordinator, never by a worker

**Decision:** a registered provider account (Anthropic, OpenAI, …) is a `fleet.yaml`
capability with `cloud: true` and **no `model_server`** — it has no host node. Its API key
lives only in the coordinator's own environment (`api_key_env` names the variable; the key
itself is never written to `fleet.yaml`). A job routed to one of these never reaches a
worker's queue: the coordinator's own **`CloudExecutor`** (`cloud_executor.py`) drains that
capability's Redis stream directly, under the *same* consumer group real workers use, and
calls the provider via LiteLLM in-process. Nothing about the queue contract changes —
`contract/` is untouched, and a cloud job is an ordinary `JobRecord` on an ordinary stream.

**Why.** This is the third piece of the cloud design (model-evaluation.md → "Provider
accounts, budget, and the cost-quality loop"; fleet-management.md → "Cloud tier") actually
built, and the key-custody question was the one that mattered: ADR 26 closed an anonymous
chain reaching model installs and deletions specifically because handing trust to every
node on a heterogeneous, owner-controlled fleet (ADR 10) is a real attack surface, not a
theoretical one. Handing real Anthropic/OpenAI keys to every worker's `worker.env` would
reopen that exact class of hole for a strictly more valuable secret. The repo had already
answered this shape of problem twice:
- **ADR 12** already splits "runs the fabric" from "drains the queue": an attached endpoint
  runs no fabric code at all, and a **coordinator-side proxy worker pulls from its queues on
  its behalf**. A provider account is that shape exactly — no fabric code is possible on
  Anthropic's or OpenAI's servers, so the coordinator is the proxy worker, full stop.
- **The sync plane already does this.** `sync.build_router`'s `CBK_CLOUD_FALLBACK_MODEL`
  path has always called cloud models in-process via LiteLLM, with the key held by the
  gateway process. This decision gives the async plane the same custody, not a different
  one — there was no principled reason for the two planes to disagree.
- **`orm/usage.py`'s `node` column comment** — `"worker id, or 'cloud:<provider>' (future)"`
  — and fleet-management.md's usage-record shape (`node | "cloud:<provider>"`) already
  assumed a cloud job has no real node. The schema was drawn before this ADR existed.

**Considered:**
- **A short-lived, scoped credential minted per job**, handed to the worker for that one
  call. Rejected on inspection, not merely on principle: neither Anthropic nor OpenAI mints
  per-request scoped tokens, so building this would mean a coordinator-side endpoint the
  worker calls with a clusterbuck-issued token, which the coordinator then exchanges for the
  real call — i.e. direction 1 again, plus an extra hop that relays prompt bodies through
  the worker for zero custody gain over just answering the job coordinator-side.
- **Giving every worker every provider key directly.** Simplest to wire, but exactly the
  anonymous-chain shape ADR 26 exists to prevent, widened to cover real third-party billing
  credentials instead of local model management.

**Consequences.**
- **Budget enforcement is now real** (`budget.py`), not display-only. The prior `/usage`
  budget figure was inert for a structural reason beyond "no cloud path existed yet":
  `usage_scan` hardcoded `venue="local"` for every row, so `cloud_spend_in_month` was
  always zero regardless of what ran. Fixed alongside this ADR by deriving `venue` from the
  capability's own `cloud` flag — the same flag privacy filtering already reads. A
  configured `CBK_CLOUD_BUDGET_MONTHLY` is **paced**: `necessary` jobs may spend the
  monthly cap minus a reserve (`CBK_CLOUD_BUDGET_RESERVE_FRACTION`, default 20%), scaled by
  how much of the month has elapsed (the cumulative form of "week one can't burn the
  month" — a flat daily slice would let unused early days evaporate); `urgent` jobs may
  additionally spend the reserve, bounded by the full monthly cap. `waitable` never reaches
  the budget check at all — ADR 18 already settled that "no wake, no cloud, no demand" is a
  wake-rights question, not a spending one, so it is excluded earlier in
  `routing.resolve_capability` regardless of budget state.
- **Explicit `capability` addressing is no longer a free pass around privacy or urgency.**
  Before a no-host cloud capability could exist, `resolve_capability`'s `capability is not
  None` branch returning immediately was harmless. The moment one exists,
  `{capability: "claude-sonnet", privacy: "local_only"}` would have routed a `local_only`
  job off-LAN — the exact incoherent combo fleet-management.md says submission validation
  must reject "fast, at the API". Closed in the same function: an explicit cloud capability
  is checked against privacy, urgency, and budget exactly like a need-shaped one.
- **Ability is measured, never assumed, for a cloud artifact too** (ADR 15's rule extended,
  not relaxed). `eval_runner.artifacts_needing_eval` gained a second source alongside
  `store.installed_artifacts()`: every no-host cloud capability with a resolvable key. Its
  eval jobs carry `privacy: cloud_ok` (a `local_only` eval job aimed at a cloud artifact
  would just be refused by the privacy rule) and bypass `budget.py` entirely — the harness
  builds `JobRecord`s directly rather than calling `resolve_capability`, the same way local
  eval jobs already do. This is a deliberate, bounded exception
  (`MAX_ARTIFACTS_IN_FLIGHT=2` × a handful of seed-suite items ≈ a dozen small calls per
  artifact), not an oversight — model-evaluation.md already says judge/eval calls draw on
  the cloud budget because the cost is trivial next to normal usage.
- **`routing.resolve_capability` now prefers local over cloud correctly.** The prior sort
  key was price alone; a cheap cloud artifact could beat a pricier qualifying local one,
  contradicting ADR 16's stated order ("prefer local → cheapest"). The sort key is now
  `(is_cloud, price, capability)`.
- **The sync plane stays separate and unmetered, on purpose** (ADR 23 stands). A registered
  provider account may also be used as a sync-plane deployment, but sync completions are
  never captured by `usage_scan` (they return straight through LiteLLM, no job, no result
  blob) — wiring the budget gate to a spend figure that structurally can never see sync
  traffic would be an unenforceable claim, worse than the honest asymmetry recorded here.
  Per-request sync metering is the named next step, not a promise this ADR makes.
- **The worker's missing `Authorization` header is fixed, narrowly.** `model_client.py` sent
  no auth header at all, which was already a real gap independent of this ADR: a node's own
  configured model server might sit behind an authenticated gateway. `CBK_MODEL_SERVER_API_KEY`
  (worker env) fixes exactly that, node-local, same trust boundary as
  `CBK_MODEL_SERVER_URL` — **not** a channel for provider keys, which never reach a worker
  under this decision. `api_key`/`api_base` join `_PARAMS_NOT_FORWARDED` so a job can never
  supply or override either.

## 31. Idempotent submit is a separate opt-in header, never `submitter.request_id`
**Decision:** `POST /jobs` accepts an optional `Idempotency-Key` HTTP header, enforced by
a UNIQUE index on `jobs.idempotency_key`; a repeat returns the existing job with `200` and
`Idempotency-Replayed: true`. `submitter.request_id` (ADR-less, shipped with caller
provenance) keeps its documented meaning: identification only, never enforced.
**Why:** a client that loses the response to a submit cannot distinguish "submitted" from
"not submitted", so it must either risk a duplicate job or risk losing the work — the one
gap that stopped a real client retrying submits at all. Enforcement needs a constraint,
and a constraint needs a field whose contract *is* enforcement. `request_id` is explicitly
the opposite: a client reusing one is **describing** a retry, which is the signal that
makes a burst of identical jobs diagnosable. Overloading it would destroy that signal and
silently change behaviour for anyone already sending it.
**Consequences:** the repo's first UNIQUE index, which must be declared as a separate
`Index` (not `Field(unique=True)`) so the legacy pre-Alembic bootstrap can create it with
`CREATE UNIQUE INDEX` — a column-level unique renders inside `CREATE TABLE`, which
`ALTER TABLE ADD COLUMN` cannot reproduce, so the two schema paths would diverge and the
constraint would simply not exist on an upgraded database. NULLs are distinct in SQLite,
so unkeyed submits never collide and every existing client is unaffected. The claim is the
SQLite insert, deliberately after the `400`/`422` checks so a rejected request does not
burn a key. `IntegrityError` is matched on the offending column (`jobs.idempotency_key` —
SQLite names the column, not the index) rather than caught wholesale, so a future
constraint cannot be silently converted into a `200`.
**Considered:** overloading `request_id` (destroys the retry signal; reverses a documented
contract); a body field (`JobSubmit` is `extra: "forbid"`, so it is a client-seam schema
change, and the worker has no use for it — the `observed_ip` precedent applies); `409` on
replay (the client asked for at-most-once and got it: that is success); check-then-insert
like `record_usage` (racy by construction, and a retry storm is precisely N concurrent
writers); a request fingerprint (deferred, and named as deferred); a timed key window
(cannot be expressed as a UNIQUE index, so it would force the racy idiom back).

## 32. Cancel is coordinator-side and best-effort; `cancelled` is not a result status
**Decision:** `DELETE /jobs/{id}` withdraws the queued entry and terminalises the job. It
reports `cancelled` **only** when the work provably never ran (the entry was deleted and
no consumer held it) and `cancelling` otherwise. `cancelled` lives in SQLite and in the
assembled `GET /jobs/{id}` view, **not** in `contract/result.schema.json`.
**Why:** abandoned work really does run — an observed job completed nearly two hours after
its client stopped polling, and nobody ever collected the result. But a model call in
flight cannot be interrupted: the worker's run loop makes no coordinator round trip between
claiming and running, does not poll mid-inference, and does not read the result key before
running, so "write a cancelled blob and the worker will skip it" is a no-op dressed as a
mechanism. Reporting `cancelled` for work still executing would be a lie; `/perf/runs/{id}/cancel`
already set the honest precedent with `cancelling`. `cancelled` stays out of the result
schema because no worker can ever emit one, and putting a status a worker cannot produce
into the cross-language seam is drift.
**Consequences:** `XACK` does not remove a stream entry, so XDEL-plus-empty-PEL cannot by
itself distinguish "never delivered" from "already ran" — the result blob must be the first
gate. Withdrawal needs the stream entry id, so every enqueue records its delivery. Since
Redis 7, `XAUTOCLAIM` *drops* pending entries whose stream entry is gone, so withdrawing a
claimed entry and then losing that worker would leave a job the reaper cannot see; a
`cancel_requested` flag plus a floored `deadline_epoch` hands it to the expiry sweep
instead. Every terminal path writes a usage row as well as a status, or
`jobs_awaiting_usage` re-selects the job on every tick forever. `TERMINAL_STATUSES` also
had to be added to the escalation and attention queries, which filtered on urgency alone —
without it a cancelled job could still promote and **wake a physical machine**.
**Considered:** a worker-side pre-run check of the result key (a new worker capability, and
still no help for a job already inside the model call); mid-run abort (touches the inference
path for a case a small fleet can absorb); `XAUTOCLAIM`-then-ack (the reaper can only ack
what it claimed, which a cancel handler cannot do for an unclaimed entry); scanning the
stream for the job id instead of recording the entry id (O(depth) per call, and still cannot
tell claimed from unclaimed).

## 33. The executor measures inference; `completed_at` was a start time
**Decision:** an executor stamps `started_at` immediately before the model call and
`finished_at` immediately after. `completed_at` is retained for one release, equal to
`started_at`, and documented as deprecated. Both new fields are **optional** in
`contract/result.schema.json`.
**Why:** `completed_at` was taken *before* inference in both the worker and the cloud
executor, so the only completion timestamp in the system was really a processing-start
time, wrong by a whole inference duration — and it was therefore unusable as the
`finished_at` a polling client needs. Correcting the field in place would silently change
every existing reader's numbers with nothing to signal it; emitting the deprecated alias as
`started_at` keeps them exactly as they were while readers migrate.
**Consequences:** they must stay optional because not every terminal result comes from an
executor — the reaper's dead-letter, the expiry sweep and a cancellation are written by the
coordinator for jobs that never reached a model server, and there is no honest value to
supply. A conformance test pins both halves, because making them required is an easy and
plausible tightening that would silently invalidate all three coordinator paths. The
coordinator prefers the executor's measurements over its own tick-based observations when
both exist.
**Considered:** fixing `completed_at` in place (silent meaning change for existing
readers); adding `finished_at` alone and documenting `completed_at` as a quirk (leaves a
field whose name permanently lies); requiring the new fields (breaks every
coordinator-written result).

## 34. Urgency-tiered streams per capability (implements ADR 24's deferral)
**Decision:** each capability gets two streams — `q:<cap>:urgent` and `q:<cap>` — sharing
the one `cbk-workers` group. Tier is a pure function of urgency (`urgent`/`necessary` →
urgent tier, `waitable` → base), derived in one place. Every consumer reads the urgent
stream first and the base stream only if it was empty. Escalation and client attention move
a promoted job's queued **entry** across, not just its urgency row.
**Why:** ADR 24 deferred this and named the condition for revisiting it — "a single warm
node routinely has mixed-urgency backlog contending". That happened: an urgent probe job
waited ~15 minutes behind a backlog of large `waitable` jobs on one warm worker. Worse,
`docs/fleet-management.md` had been promising `necessary` "head of the async queues" and
that `waitable` "yields to urgent/necessary work" the whole time — so a client reporting
this was not misreading the system, it was reading our documentation. Implementing it makes
the documentation true; deleting the promise instead would have left the fleet unable to
express urgency at all once a node is awake.
**Consequences:** the rollout is the risk, not the topology. A worker built before this
reads only the base stream, so an urgent-tier write while such a node is enrolled would
**strand** the job. The gate is therefore evidence-based, not version-based: a node reports
the streams it reads in its heartbeat `queues`, and a capability is tiered only once every
node **enrolled** for it lists an urgent stream. Enrolled rather than live, and at least one
required — "every live node is tier-aware" is vacuously true on a cold fleet, and the asleep
node is exactly the one that will wake and claim. `CBK_URGENT_STREAMS=auto|on|off` forces it
either way; `off` restores the previous single-stream behaviour exactly.
Several things had to learn about tiers or they would corrupt routing silently:
`known_capabilities` SCANs `q:*` and must strip the suffix, or the reaper reclaims entries
under a bogus `<cap>:urgent` capability and requeues them onto the **base** stream,
demoting the escalated work; `depth` sums work across tiers but takes the **max** of
consumers, since one worker is a consumer on both groups; the reaper covers both tiers;
`read_one` must tolerate a missing tier, because a capability may legitimately have no
urgent stream yet. Moving a promoted entry reuses the cancel path's withdraw primitive, so
it happens only when the entry is provably unclaimed — `claimed`/`gone`/no-delivery all mean
leave it alone, since a copy on the urgent tier would run the job twice. The payload is read
off the stream *before* withdrawal, because it is not stored in SQLite.
**Deliberate limits:** ordering is between tiers, not within one (two urgent jobs still run
in submission order). The reaper requeues onto the tier it reclaimed from rather than
re-deriving the tier, which keeps it free of the rollout gate; a job promoted while claimed
therefore finishes its retries on the base stream. Demotion (an attention lease lapsing) does
**not** move the entry back — a demoted job running slightly too eagerly is not worth
doubling the machinery for.
**Considered:** a Redis sorted-set priority queue (abandons the Streams reliability
primitives ADR 20 adopted); reordering inside the worker (it cannot see the whole stream
cheaply); gating the rollout on `agent_version` (weaker than the worker's own declaration of
what it reads); deferring again (the documentation would have stayed false).


## 35. Joining a worker costs one password; the operator key stays on the coordinator
**Decision:** a new machine clones the repo and runs `install/worker/join.sh` (or
`join.ps1`) with the coordinator URL and a model name. It prompts for a **join password**
(`CBK_JOIN_PASSWORD`, 16 characters minimum) and exchanges it at `POST /nodes/bootstrap` for
a **single-use join token**, the broker URL, and the coordinator's capability registry; then
downloads the coordinator's blessed `cbk.pyz` from `GET /worker/artifact` and hands off to
the existing platform installer for service creation and enrolment. Both routes are exempt
from the ADR 26 operator key — a joining machine has none, which is the premise. Unset or
under-length password ⇒ both routes **404**, so no surface is added by default.
**Why:** onboarding previously hand-carried **two** secrets onto every new machine — an
operator-key-minted join token and the Redis URL *including its password*, passed as a
command-line argument and therefore into shell history. The operator key is the one secret a
worker must never hold: it mints tokens, approves model installs and deletes other owners'
model files (ADR 26's finding). One password, held in a password manager and typed at a
prompt rather than in argv, replaces both, and leaves the destructive key on the one box that
needs it. Serving the artifact from the coordinator rather than building locally means every
node runs the same build instead of whatever its checkout happened to contain — the same
reasoning as ADR 13's signed update, applied to the first install.
**Consequences:** the coordinator gains three settings the installer does not write
(`CBK_JOIN_PASSWORD`, `CBK_WORKER_ARTIFACT`, `CBK_BROKER_ADVERTISE_URL`), so joining is
opt-in and a fresh coordinator answers 404 until an operator turns it on. The third exists
because Redis normally runs on the coordinator, so its own `CBK_REDIS_URL` is loopback, and
advertising that points each worker at its **own** localhost — a failure invisible at join
time, since install, enrolment and service start all succeed. The coordinator therefore
**refuses** to advertise a loopback broker, answering 503 and naming the variable. The join
also warns when the node's probed capabilities are absent from the coordinator's registry:
enrolment and heartbeats look healthy while no job can ever route. The password is a weaker
secret than the operator key by design and guards a narrower thing; rotating it is an edit
and a restart, and already-joined nodes are unaffected because they authenticate with their
own per-node key.
**Considered:** recognising a machine by **LAN subnet** and letting the operator acknowledge
it from the dashboard (a same-subnet check authenticates the network, not the machine, and
any guest device or compromised IoT box on a home LAN passes it); a **per-machine API key**
(multiplies the thing that must be distributed, and ADR 26 settled that there is one
operator); leaning on the **existing VPN** so every node shares one embedded key (a hard
dependency on a third-party overlay for what is meant to be plain-LAN infrastructure, and it
still distributes the operator key); `curl | bash` **without a repo clone** (the platform
installers need sibling files — `deploy/systemd/cbk-worker.service` — so the checkout is
required anyway, and piping a script from a private repo does not work); and
reimplementing service installation inside the join script (two proven installers already do
it per platform; new logic belongs in one place, OS plumbing stays where it works).

## 36. Speed is a job-level floor the node enforces, not a routing sort key

**Decision:** speed enters addressing as **`min_tps`** — an output-tokens/sec floor the client
states alongside `min_ability` — filtered at submit against the measured `stats.tps` of the
nodes serving each capability, and **checked again by the node itself** before it answers.
A node that cannot meet the floor returns a `failed` result naming the shortfall rather than
a slow answer.

**Why a second axis at all.** Ability scores an *artifact* and is machine-independent by
design (ADR 15): the same model scores identically on a 12 GB GPU and a CPU-only box and
performs nothing alike. That is a deliberate property — quality belongs to the artifact,
throughput to artifact × node — and it means the ability matrix structurally *cannot* answer
"will this come back quickly here". `stats.tps` is the measurement that can, and until now
nothing consumed it: `resolve` sorted on `(is_cloud, price)`, while
model-evaluation.md's stated order is local → cheapest → **fastest**.

**Why not simply add it to the sort.** Because it does not fit there. Routing selects a
**capability**, which is a queue name; `tps` belongs to a **node**, and a capability is
drained by whichever subscribed node claims first. By the time a tier is chosen the ability
to choose a machine is already gone. The same gap is why the heartbeat's `loaded` field —
documented as enabling "model-affinity routing" — has never routed anything.

**Why the node decides.** Only the node knows how fast it is *right now*: which model is
resident, how busy its owner's machine is, whether the last job was a cold start. The
coordinator's view is a rolling median reported seconds ago, good enough to exclude a
capability wholesale and not good enough to be the last word. So the coordinator filters on
what it knows and the node refuses what it cannot serve — the same division of labour as the
artifact pin, and the same failure mode as every other unmet floor in this system: explicit,
not silent.

**Unknown is never slow.** A node that has finished no jobs has no measurement, and both
gates let it through. Reading absence as slowness would fail every speed-sensitive request on
a healthy new install, and the worker-side check catches the case where it does turn out to
be too slow.

**Considered — per-node streams** (`q:<cap>:<node>`, the coordinator picking a node on
`tps` × `loaded` × presence). It gives real speed- and warmth-aware dispatch, and it gives up
the self-balancing property that made the pull model worth choosing (ADR 2): the coordinator
must then model which node is free, and a node that sleeps between the choice and the pickup
strands the job. **Considered — XACK-and-requeue declining**, so a slow node hands the job
back: re-adding changes the stream entry id, which breaks delivery bookkeeping, the reaper's
view, and queue-position estimates, and a single-worker fleet hot-loops on the job forever.
**Considered — a `:fast` stream tier** alongside `:urgent` (ADR 34): jobs carry many
different floors, so one threshold cannot express them, and a second tier axis multiplies
streams per capability combinatorially with urgency.

**Known limitation, accepted.** Two nodes of *different* speed serving the *same* capability
can still race: the slow one may claim a job the fast one could have served, and refuse it.
The job fails rather than being under-served, which is the correct half; getting it to the
fast node instead needs node-addressed dispatch, and that is the design above whose costs are
not yet worth paying at home-fleet scale. On the common shape — a small always-on box and a
large workstation serving *different* tiers — the race does not arise.

## 37. What a model *can do* is a filter, not a score

**Decision:** jobs may carry **`requires`** — `context_tokens`, `tools`, `json_schema`,
`vision` — and routing applies it as a **hard filter before ability is compared at all**.
Local artifacts declare these in the **model catalog**; provider accounts, which have no host
and never enter that catalog, declare them on their `fleet.yaml` capability. An artifact that
does not declare a required feature is **excluded, by name**.

**Why not a score.** Ability (ADR 15) is a graded 1–10 judgement of how *well* a model does a
task class. These are not that shape. A context window is a number with a hard edge; tool
calling is a boolean. A 4k-context model and a 128k one can both honestly be "a 6 at
summarize", and routing a 60k-token document to the first silently truncates it — the ability
matrix cannot see the difference, because there is no quality axis along which to see it. So
a client needing tool calling could not ask for it: it either named a `capability` explicitly,
abandoning need-shaped addressing, or submitted and hoped.

**Why filter before comparing ability.** Otherwise a capable-but-unsuitable artifact wins on
score and the requirement is decided by an unrelated number. Filter on what a model *can* do,
then compare how *well* it does it.

**Why undeclared reads as "no".** This is the opposite of the `min_tps` rule (ADR 36), and
deliberately so. An unmeasured *speed* is genuinely unknown and self-corrects — the node
checks again and refuses if it turns out to be too slow. An undeclared *feature* has no such
backstop: serving a tool-calling job on a model nobody has checked fails at the model server,
where it reads as a model bug rather than a routing one, or worse succeeds while quietly
ignoring the tools. The refusal names the artifact, so the fix is one `POST /catalog` away.

**Why the catalog rather than probing the node.** The portable `GET /v1/models` returns an id
and nothing else — no window, no capability list. The facts exist only behind vendor-specific
endpoints (Ollama's `/api/show`, LM Studio's own route), which is exactly the dependency
`inventory.py` keeps optional for `loaded` and `digests`. Curation also covers artifacts no
node has yet, which a probe by definition cannot. A vendor adapter can populate these later
without changing the routing contract.

**Considered:** a free-form JSON `features` blob (extensible without migrations, but a typo'd
key silently matches nothing, and these four are a small stable set); **inferring** the window
from parameter count or family (wrong often enough to be worse than absent, and it would
manufacture exactly the confident-but-unfounded metadata this system keeps removing); and
enforcing `context_tokens` **worker-side** against the running server's configured window (the
right long-term check, since a runtime started with a smaller window than the model supports
will still truncate — but it needs a per-server adapter, and the catalog's published figure is
the useful first cut).

## 38. The update channel serves its artifact unauthenticated, because the signature is the boundary

**Decision:** `GET /releases/{filename}` is exempt from the operator key and serves only the
files named by the current release manifest. This is the URL a worker's signed update
manifest points at.

**Why this is not a reversal of `/worker/artifact`'s rule.** That route's comment — *"this
coordinator serves files to no unauthenticated caller"* — is about the **bootstrap** path: a
machine with no identity yet, whose installer already holds the join password, so gating
costs nothing. `/releases/` is fetched by a worker **already in the field**. It holds a node
key and must never be given the operator key (ADR 26), and the join password is an install-
time secret it has no reason to keep. Requiring a credential here would mean distributing a
second secret to every node, and would break every worker already deployed — precisely the
fleet that auto-update exists to serve.

**Why it costs nothing.** ADR 13 makes the channel untrusted by construction: the worker
verifies an ECDSA P-256 signature over `(version, rid, sha256, channel, url,
protocol_version)` and then the digest of what it downloaded, both **before writing a byte**.
`url` is inside the signed payload precisely so an on-path attacker cannot redirect the
fetch. An attacker who can serve this file cannot make a worker install it; one who cannot
forge a signature gains nothing from reading it, since it is Apache-2.0 code.

**Why the coordinator rather than a separate file server.** A second service would be simpler
to add and worse to keep: it has no idea what the coordinator considers current, so the file
it serves and the manifest the coordinator signs can drift apart silently — and it is another
unit to install, secure and restart on every coordinator. Serving from the process that signs
the manifest makes drift impossible: the same `CBK_UPDATE_RELEASE` file defines both.

**The allowlist matters more than the auth would have.** `filename` is matched against the
basenames the manifest blesses and is **never joined as a caller-controlled path**, so there
is nothing for `..` to traverse and the release directory is not a web root. A file sitting
beside the artifacts — including the manifest itself — is not served.

**Considered:** a **separate static server** (drift, and an extra unit per coordinator);
teaching the worker to **authenticate the download with its node key** (cleanest in the
abstract, but a manifest `url` may point anywhere, so the worker would have to decide which
hosts deserve a credential — a leak hazard invented to solve a problem the signature already
solves — and it cannot bootstrap a fleet already in the field).
