# clusterbuck: how it works

One document for how the system is shaped, why it is shaped that way, and how to run it.
It replaces the former `architecture.md`, `fleet-management.md`, `model-evaluation.md`,
`deployment.md`, `implementation.md` and `DESIGN.md`, which said the same things in
several places and disagreed in a few.

Two companions: [protocols.md](protocols.md) for the exact wire shapes, and
[related-projects.md](related-projects.md) for why the adjacent tools do not fit.

> **Where the old ADRs went.** `docs/decisions.md` held 40 numbered decisions. The
> rationale worth keeping is folded into the sections below, at the point it explains
> something. The full original text is one command away:
> `git show docs-before-merge:docs/decisions.md`.

---

## 1. What it is

A broker between the tools you use and the machines you own. A client submits a job to one
stable endpoint; clusterbuck runs it on whichever machine on the LAN is capable and free —
now if something is awake, later if it has to wait — and falls back to a cloud provider
when the local fleet cannot do it.

It deals only in **jobs, capabilities and results**. It knows nothing about the
applications that use it, and must not: it is infrastructure, Apache-2.0, and shared
publicly. No client's domain concepts, no real hostnames, no employer. Treat a leak of
that as a bug.

Two things are the point:

- **Privacy** — a job marked `local_only` never leaves the LAN, at any urgency.
- **Economics** — work that would have gone to a paid API runs on hardware you already
  own. The headline metric is *avoided cloud spend*.

**What it is not.** Not a serving cluster. vLLM, Ray Serve and friends assume machines
that are up, homogeneous and yours to schedule; they make one model fast across many GPUs.
clusterbuck assumes the opposite — a handful of mismatched machines, some asleep, some
someone else's to interrupt — and its scarce resource is *availability*, not throughput.
It sits above a serving engine, not beside one: a node may well run vLLM underneath.

## 2. The core idea: buffered work costs only latency

Most background work is patient. A nightly summarisation does not care whether it runs at
23:05 or 02:40; it cares that it runs, and that nobody paid a per-token bill for it. A
queue turns "no machine is awake" from a failure into a delay, and a delay is something
background work can afford.

That single trade is what the whole design falls out of. Everything below is a consequence
of taking a fleet that is *intermittent* seriously.

## 3. Two planes over one pool of machines

One front door. Behind it, a thin decision — answer now, or queue — and two paths that
share the same worker machines and model servers.

```
                    ┌─────────── coordinator (the always-on node) ───────────┐
  client ─ job ───► │                                                        │
                    │  interactive, and something awake to serve it?         │
                    │     ├─ YES → sync plane (LiteLLM) ─► live worker ─► reply
                    │     │                                                  │
                    │     └─ NO, or "patient" → async plane (Redis Streams)  │
                    │            • enqueue on q:<capability>                 │
                    │            • wake a node if the job has earned it      │
                    │            • a worker pulls, calls its model server    │
                    │            • result written to the result store        │
  client ◄ result ◄ │            • client polls                              │
                    └────────────────────────────────────────────────────────┘
```

**The sync plane is not ours.** LiteLLM already does OpenAI-compatible routing, health
checks, load balancing and cloud failover. Reimplementing it would be a year of work to
arrive back where we started. clusterbuck builds the async plane and the coordination
around it, and adopts the rest.

| Concern | Owner |
|---|---|
| OpenAI-compatible endpoint, route-now, health check, failover | **LiteLLM** (adopted) |
| Durable job queue, submit/poll API, urgency and escalation | **coordinator** (built) |
| Wake policy, reservations, the model planner, metering | **coordinator** (built) |
| Pull a job, call the local model server, write the result | **worker** (built) |
| The actual inference | **model server** (adopted: Ollama, llama.cpp, vLLM, LM Studio) |
| Queue and result storage | **Redis** (adopted) |

Nothing binds to a vendor SDK. The worker speaks the OpenAI HTTP wire protocol, so any
model server that does too can host a node.

## 4. Addressing: a capability, or a need

A job never names a machine. It names one of two things.

**A capability tier** — `8b-extract`, `32b-reason` — an abstract label a worker subscribes
to. This is what makes a heterogeneous fleet work: a `70b-reason` job waits on that queue
until some 70B-capable worker appears, with no dispatcher tracking who is alive. Adding or
removing a machine changes only which queues have consumers, never a client.

**Or a need** — `task_class` plus `min_ability` ("summarise this, with a model that scores
at least 6"). The coordinator resolves that to a concrete artifact through the ability
matrix (§6), cheapest first, and pins the artifact it chose onto the job. The pin matters:
without it, `min_ability` was checked against a name in `fleet.yaml` that nothing
reconciled with what the node actually loaded.

Needs are preferred. Naming a tier is the advanced form — it ties a client to your supply
side, so changing which model serves a tier silently changes what that client gets.

## 5. Pull, never push

Workers pull; the coordinator never dispatches to a named node. For an intermittent fleet
this is decisive:

- No liveness table to maintain, and no job ever handed to a node that just slept.
- **Subscribing *is* the liveness signal.** A consumer on the group is a machine that is
  up, by construction.
- Backpressure is free: an absent capability just means its queue grows until a worker
  drains it, or the coordinator wakes one.

Redis **Streams with consumer groups**, not lists. A `BLPOP` list loses a job if the worker
dies after popping and before writing a result; a stream keeps the entry in the pending
list until it is acknowledged, so a dead worker's job can be reclaimed. That reclaim is the
**visibility-timeout reaper** — `XAUTOCLAIM` over entries idle longer than the threshold,
requeued with `attempts` incremented, dead-lettered past `max_attempts` so a polling client
gets an answer instead of waiting forever. The idle threshold must exceed the longest
plausible inference, or the reaper steals work from a healthy-but-busy node and runs it
twice.

Two further backstops catch what the reaper structurally cannot see, because `XAUTOCLAIM`
only walks entries a worker has *claimed*:

- **Orphan sweep** (always on) — a job whose row was committed but whose `XADD` never
  happened, because the coordinator died between the two. Nothing will ever deliver it.
- **Maximum queue age** (`CBK_MAX_QUEUE_AGE_S`, off by default, deliberately) — a properly
  enqueued job nobody ever claimed. That one genuinely is policy: on a fleet whose machines
  sleep for days, a patient job outliving any fixed cutoff is *correct*, so the default is
  to let it wait.

## 6. Quality: what "a good enough model" means

Model quality is **jagged**. The same model can be strong at structured extraction and
weak at multi-step reasoning, so a single per-model number routes jobs wrongly. The
primitive is therefore a matrix:

```
ability(artifact, task_class) → 1–10
```

- **artifact** = model *and* quantisation. An 8B-Q4 and an 8B-Q8 are different artifacts.
  Quality belongs to the artifact; throughput belongs to artifact × node.
- **task_class** = `extract | summarize | reason | code`.
- Scores are coarse on purpose: **half-point granularity**, stored with the item count
  behind them. Small suites cannot honestly tell 6.3 from 6.5.

A task class must be both **measurable and servable** to exist. `embed` was once listed
and seeded here, but no suite item measured it and no worker could serve it — the worker
speaks `/v1/chat/completions` only — so a request for it routed on a permanently
unimprovable guess and came back as prose. Adding embeddings back means an
`/v1/embeddings` execution path and real suite items, not a row in a list.

### The instrument, and its ceiling

**Tier 1 — programmatic checks.** Wherever an output is mechanically verifiable, verify
it: JSON validity *and the requested field values*, exact-answer retrieval, length and
format compliance, arithmetic with one known answer. Free, unarguable, and it covers most
extraction-shaped work. This is the tier that is built.

Two limits are enforced in code rather than left to discipline:

- **It cannot reach the top of the scale.** Programmatic checks establish *compliance*,
  not quality — they cannot tell a 7 from a 9. A perfect tier-1 pass rate therefore maps
  to `TIER1_MAX_ABILITY` (7.0, the "strong local model" anchor) and the 8–10 band stays
  reserved for judged tiers. The cap is on the *instrument*, not the scale. Consequence
  worth knowing: on a default install a `min_ability` of 8+ fails explicitly, for local
  *and* cloud alike, because cloud artifacts run through the same clamped harness. The
  error says so rather than sending you looking for a better model.
- **It needs enough items to mean anything.** A score is not recorded below
  `MIN_ITEMS_FOR_SCORE` items per class. Half-point granularity over a handful of items
  is a fiction.

Items must be **un-gameable by echoing**. A check that passes when the model repeats the
prompt or emits an empty object measures verbosity. *Known gap:* three of the ten
`summarize` items currently check only a word limit, so any short string clears them —
worth ~30% of that class for free. Tightening them changes what every existing score
means, so it needs a `SUITE_VERSION` bump; a test pins the three so they stay a known
quantity.

**Tiers 2 and 3 are designed, not built.** Tier 2 is checklist judging — never "rate this
1–10", which is noisy and biased, but per-item coverage and faithfulness questions against
a gold checklist. Tier 3 is pairwise preference with randomised A/B order aggregated by
Bradley-Terry, because judges are good at "which is better" and bad at "how good is this".
Both need a judge model: materially stronger than what it judges, never judging itself,
with length-neutral rubrics.

### The harness is just another client

Measuring an unmeasured artifact is not a special mode. The coordinator submits the suite
as **ordinary fleet jobs** — `waitable`, `local_only`, with the artifact pinned per job —
and real workers drain them. Cloud artifacts go the same way, dispatched to the
coordinator's own executor instead of a worker.

A re-measurement is a **new generation**, not a continuation. Ability derives from the
recorded runs for an (artifact, task_class), so invalidating a score has to invalidate its
runs too — otherwise a fresh batch averages with the superseded artifact's items and the
old measurement survives under a new digest. Runs still in flight when an artifact is
superseded are retired: they are measuring something no longer installed. The retry budget
resets with the generation, so an artifact whose last round failed for infrastructure
reasons is not barred forever.

Re-measure when: a new artifact is installed, its digest changes (a changed digest is a
**new artifact** and inherits nothing), the quantisation or runtime changes, or the scale
version is rebased.

### The scale is versioned

Anchors pin the 1–10 scale — illustratively 9–10 frontier cloud, 7 strong 70B-class, 5
good 8B at Q4, 3 a 3B, 1 sub-1B or broken quantisation. When the frontier moves, anchors
are rebased and scores re-stamped against the new version; otherwise "8" silently deflates
over the years. Ability is always reported with its scale version.

Suites are **bespoke and rotating**. Never reuse a well-known public benchmark verbatim —
models have memorised them, and a contaminated score is worse than no score. An
installation may add private items that never leave it.

### Three registries, and the trap

This is the thing most likely to waste an afternoon. Three separate records describe
"what models exist", they do not reconcile with each other, and nothing warns you when
they disagree:

| Registry | Holds | Maintained by | Where |
|---|---|---|---|
| **Node registry** | what each node *is* — RAM, VRAM, accelerator, installed models, presence | automatic, via enrollment + heartbeat | SQLite, `GET /nodes` |
| **Capability registry** | what a tier *means* — its queue, model server, model, prices | hand-edited, needs a restart | `server/fleet.yaml` |
| **Model catalog** | what a node *may install* — candidates plus gate metadata | `POST /catalog`, seeded once | SQLite, `GET /catalog` |

Editing the wrong one produces a change that appears to succeed and does nothing. Two
guards exist: the coordinator **pins** the artifact it selected onto the job and a worker
returns a `failed` result naming a pin it cannot serve, and `GET /nodes` reports
`capability_warnings` for every tier a node advertises whose model it does not have. The
dashboard's models table joins all three, one row per (artifact, node), which is the view
to reach for first.

The seed catalog only seeds an **empty** table, so left alone it freezes at whatever
shipped. Curating it is a periodic manual job — there is no upstream feed, by choice: for
a handful of nodes, adding a model by hand is the right size.

## 7. The fleet

### A node's life: install → enroll → probe → serve

The worker agent is one `py3-none-any` zipapp for every platform. Joining is one command
(§ README), and it does three things: trade a **join password** for a single-use join
token at `POST /nodes/bootstrap`, enroll to receive a node identity and a per-node key
used for all later auth, then start heartbeating.

The join password exists so the operator key never leaves the coordinator. A joining
machine holds one password; it receives a token that burns on use, the broker URL and the
worker artifact. A password under 16 characters leaves the route disabled rather than
weakly guarding the Redis credential.

**The probe** reports RAM, **VRAM** (the card's total on CUDA; on Metal the share of
unified memory the GPU may wire down), CPU arch, OS, accelerator, and free disk **on the
volume the model server actually writes weights to** — not the filesystem root, which on
many machines is a different disk.

Throughput is deliberately **not** probed. A benchmark at enrollment times a cold model on
an idle machine, once. The worker instead samples real jobs and reports `stats.tps`
(median output tokens/sec) and `stats.load_s` (seconds to bring a model up cold) on its
heartbeat. Both are omitted until measured — a node that has served nothing has no speed,
which is not the same as being slow. `load_s` is what the reservation pre-warm lead is
computed from.

**Capability proposal** budgets against **VRAM where it is known**, not system RAM. A
workstation with 64 GB behind an 8 GB card can hold a 70B only by spilling it across the
bus every token, so proposing that tier advertises a capability it cannot honour. The
proposal is a default the owner edits, not a mandate.

One refinement worth knowing: a mixture-of-experts that overflows VRAM is **not**
"degraded" the way a dense model is. A dense model reads every parameter per token; an MoE
reads only its active ones, so most of what sits in system RAM is never touched. Measured,
not reasoned about: a 30B-A3B held 54% resident on a 12 GB card ran at 71 tok/s while the
same artifact fully resident in 48 GB on another node managed 52. The gate reads
`active_params_b`, and an entry that omits it is treated as dense — a genuinely oversized
dense model must not slip through on a technicality.

### The registry is a declaration, not a liveness signal

Everything in a node's registry row — mode, installed models, stats — is **what the node
last said about itself**, and none of it expires on its own. Read literally, a machine
powered off months ago still says `active`.

So the dashboard ages the heartbeat stamp: a node silent for longer than
`CBK_NODE_SILENT_S` (default 60s — six missed heartbeats at the worker's 10s interval,
and the worker heartbeats from its own task so a long inference never delays one) is shown
as **silent**, next to the time it was last heard from. Deliberately not called *stale*,
which already means agent-version drift on a node that is still talking; the table renders
both pills one column apart.

This is presentation only. A silent node is still in the registry and still counts
wherever the coordinator reads enrolled nodes — urgency tiering, for one. The queue
panel's worker count is a separate and stricter signal: live stream consumers,
idle-filtered.

### The owner always wins

Every node carries a **profile** — `dedicated`, `shared` or `background` — describing how
much of the machine clusterbuck may take: RAM cap per mode, allowed hours, a battery rule,
a disk quota for weights, thermal backoff.

**Presence modes** follow the owner: `active` (person present), `away`, `paused` (explicit
opt-out). A **model ladder** maps mode to model set — `active` might allow only a small
resident model, `away` swaps in the large one and subscribes to heavier queues. Climbing
the ladder is damped by hysteresis because a cold load costs tens of seconds; descending
is immediate, because freeing RAM is cheap and the owner is waiting. Never thrash on a
coffee break.

Eviction is instant and lossless: `cbk pause` stops pulling, unloads, and any aborted job
returns to its queue through the visibility timeout. To the queue, a node dropping down
the ladder is indistinguishable from one going to sleep.

*Built with a caveat:* presence is **set manually** (env, CLI, `cbk pause`). Detecting it
from screen lock or input idle is deferred, so the hysteresis currently damps a signal
only a human changes.

### Reservations: cold by default, warm by appointment

The resting state is asleep and unloaded. Nothing is kept hot "just in case". Warmth comes
from three places: the always-on node's small resident model, a queue-depth wake, and
**reservations** — a client declaring expected demand in advance so the fleet can set up
rather than react.

A reservation names a task class and ability floor, a load class, a duration, and a
window. The coordinator answers **confirmed** with a plan (node, artifact, warm-by time),
a **counter-offer** (different window, lower local ability, or cloud now), or
**declined**. Lifecycle: `scheduled → warming → open → draining → closed`. At lead time —
derived from that node's measured `load_s` — it wakes the node and pre-loads the artifact,
so the cold-load cost is paid before the window opens rather than by the first job.

The commitment is **soft**. This is a home fleet: the reserved node roams off-LAN, or its
owner comes back and the presence ladder evicts, which always wins. The coordinator
re-plans — another node, cloud if the jobs are `cloud_ok`, or slip the window.

### Urgency is a trajectory, not a label

| Urgency | Meaning | Wake rights |
|---|---|---|
| `urgent` | A user is waiting | Sync plane; may wake immediately; cloud if `cloud_ok` and in budget |
| `necessary` | Prompt, but blocks nobody | Head of the async queues; may trigger an on-demand wake |
| `waitable(N)` | Backlog | **Never wakes a machine**; runs as soon as existing warmth has spare cycles |

**`waitable` is eager, not deferred.** N is a *patience bound, not a delay*: the job runs
the moment any capable node is awake with spare cycles. It simply never *creates* capacity
for itself — no wake, no cloud, no demand — and yields to busier work. Only if it is still
unserved when N expires does it promote to `necessary` and gain the right to demand.

Urgency also **orders the queue**. Each capability has two streams, `q:<cap>:urgent` and
`q:<cap>`, and a worker drains the urgent one first. Two limits: ordering is *between*
tiers, not within one (two urgent jobs are still FIFO), and a job already claimed cannot
be moved, so a promotion mid-run affects only what is still queued.

Escalation triggers are **age** (`escalate_after_min`, built) and a **backlog watermark**
(designed, not built). A third — a client attention lease — was built and removed unused
in September 2026: nothing ever posted to it and its table never held a row.

Escalation **coalesces, never stampedes**: a watermark promoting dozens of jobs becomes
one warm window — one wake, one load, one drain — not dozens of wakes.

### The planner proposes; a human disposes

The coordinator compares demand (which capabilities are used, how often, how long jobs
wait) against supply (the registry's hardware and ladders) and raises **proposals**:
upgrade, re-evaluate on a digest change, or reclaim disk from a model that has served
nothing. Three gates before anything changes:

1. **Fits** — RAM/VRAM at the relevant mode, and the owner's disk quota.
2. **Beats the incumbent** — on measured ability, not assumed.
3. **A human approves.** Multi-GB weights are never fetched silently. Per-node
   `auto_approve` is an explicit opt-in, and **reclaim is never auto-approved** — deleting
   someone's model files is not a thing to do unattended.

An approved action is executed by the worker through a per-server model-manager adapter,
presence-gated so a large pull never lands under an active owner. A fresh install or a
changed digest **never inherits** an ability score.

## 8. The cloud tier

Three distinct uses, not one: **unavailability** (the local fleet cannot serve in time),
**overflow** (it is up but overwhelmed — projected wait exceeds the deadline), and
**bigger-than-local** (a tier no local model can serve). Overflow is designed, not built.

Two governors apply to all of it:

- **Privacy.** Every job carries `privacy: local_only | cloud_ok`, defaulting to
  `local_only`. A `local_only` job **never** leaves the LAN — not at `urgent`, not under
  overflow pressure, not to hit a deadline. It waits, or fails explicitly.
- **Budget.** A monthly cap, and it is *enforced*, not merely displayed. `necessary` may
  spend the **paced pool** — the cap minus a reserve, scaled by how much of the month has
  elapsed, so week one cannot burn the month. `urgent` may additionally draw the reserve.
  `waitable` never reaches the budget check at all, because for it cloud is a wake-rights
  question decided earlier: no wake, no cloud, no demand.

Cloud models are **artifacts with a price and no host node**. They enter the same catalog
and the same ability matrix as local ones, measured rather than assumed — and by the same
tier-1 harness, so they are clamped to the same 7.0 ceiling until judged tiers exist.

The provider key lives on the coordinator and is never given to a worker. Cloud jobs are
executed by the coordinator's own executor, which records itself on the job as
`cloud:<provider>` — so the row positively says who ran it.

## 9. Usage accounting

Every job is logged, local and cloud: ids, timestamps, capability, model, node or
`cloud:<provider>`, tokens in/out, queue wait, run time, outcome, cost.

**Metadata only — never prompt or completion text.** The accounting layer must not become
a copy of every private thing the fleet has processed. Payloads live only in the broker,
under their TTLs.

This is **metering, not billing**: visibility and planner input, not chargeback. The
headline is **avoided cloud spend** — local tokens priced at the rate they would otherwise
have paid, minus actual cloud spend. That is the project's reason to exist, made
measurable, and "which models earn their RAM" falls out of it.

## 10. Deployment and platforms

### Two roles; one machine can be both

- **Coordinator** — the always-on node. Hosts the job API, the LiteLLM sync front, the
  coordinator loop and Redis. Needs to be reliable and low-power more than fast: its work
  is almost pure I/O.
- **Worker** — runs the agent bound to the queues it can serve, plus a local model server.
  These are the machines that sleep and roam.

A **Raspberry Pi is an excellent coordinator** and a useless inference worker. The server
role is Redis ops, health checks and shuttling results; Python, Redis and LiteLLM all run
on `linux-arm64`. But a Pi cannot usefully run an 8B, so the clean fork is *Pi = pure
coordinator*, or *a small x86/Mac box = coordinator plus one resident small model*. The
protocol boundaries are identical either way.

### The worker artifact

One `py3-none-any` zipapp, ~2.9 MB, every OS and architecture, no build matrix.

```bash
cd worker && python build.py        # → dist/cbk.pyz
```

Needs Python 3.11+ on the node. Self-update additionally needs `cryptography`, which is
**deliberately not vendored**: it is the update verifier, so it must not arrive through
the channel it secures. Absent, the worker runs normally and refuses updates rather than
applying an unverified one.

### Accelerators

**macOS / Metal.** A Metal-backed model is capped by `iogpu.wired_limit_mb`, roughly
67–75% of RAM by default. That cap, not total RAM, is what the worker reports as
`vram_gb` and what the fits gate budgets against — so a 64 GB machine offers a model about
48 GB, not 64.

Worker sizing is a property of the **model server**, not of clusterbuck, which only needs
to know the resulting capability a node advertises.

### The macOS trap that costs a day

On macOS 15+, a process needs **Local Network** permission to reach a private-range
address, and **the grant is per binary**. A worker started by `launchd` has no grant, so
every LAN connection fails with `errno 65` — surfacing from redis-py as `No route to
host`.

It is easy to misdiagnose, because the obvious checks all pass:

| check | result |
|---|---|
| `ping` / `nc -z` to the broker from a shell | works |
| the worker's own interpreter, run from a terminal | works |
| `curl` from a launchd job | works — Apple binaries already hold the grant |
| **the worker's interpreter from a launchd job** | **fails** |

The discriminating test is running the *same third-party binary* under launchd and from a
shell. A terminal passes because the child inherits the terminal's grant.

Two fixes. **Grant it** — System Settings → Privacy & Security → Local Network, enable the
interpreter; it must have attempted a connection once to appear, and there is no CLI
equivalent (`tccutil` resets a grant, it cannot create one). **Or route off the local
network** — an overlay address (Tailscale/WireGuard, `100.64.0.0/10`) is not
local-network scoped, so the gate does not apply; on the same LAN it is usually a direct
connection, and it encrypts broker traffic that would otherwise cross in plaintext.

Only broker and coordinator connections are affected. A worker's own model server on
`127.0.0.1` is loopback, never gated.

### Windows

The worker runs under **Task Scheduler** as **NetworkService** — a built-in account, so
there is nothing to create and no password to store. Task Scheduler cannot source an env
file, so a generated `.cmd` shim reads `worker.env` at each launch; it sets `PYTHONUTF8`
because the service account's redirected stdout otherwise falls back to the ANSI codepage
and dies on the worker's own output.

**Self-update refuses on Windows.** The in-place swap cannot work while zipimport holds
the handle, and `os.execv` there spawns a child rather than replacing the image — which,
with the task's restart policy, would leave two workers on one Redis consumer name.
Updating a Windows node means replacing the `.pyz` and restarting the task. The
coordinator still marks it `stale`, so the drift stays visible.

Never write a file the worker parses with `Set-Content -Encoding UTF8`: on PowerShell 5.1
that emits a BOM, and cmd's `for /f` folds a BOM into the *first variable's name* — so the
first setting in the file is silently never applied.

### Wake

**Wake-on-LAN** needs "wake for network access" enabled per node and its MAC in the
registry, and works **only on the same LAN**: a laptop at another site is
opportunistic-only and simply does not drain until it comes home. **Scheduled wake**
(`pmset repeat wake` on macOS) opens predictable batch windows.

### Network and security

- **LAN-only by default.** Bind the coordinator, Redis and model servers to the local
  network, and put a shared key on the API.
- **Redis is its own exposure.** It holds prompts and completions in plaintext and needs
  its own `requirepass`.
- **One operator secret** (`CBK_API_KEY`), presented as `X-CBK-Api-Key`, a bearer token
  or a cookie. Unset means auth is **disabled** and warned at startup, which keeps dev and
  the e2e scripts working. Exempt: `/healthz`, `/static/*`, `/releases/*`, enrollment
  (one-time token), heartbeat (per-node key), and bootstrap (join password). This is
  authentication, not authorisation — any holder of the key is the operator.
- **A model server only needs to be LAN-reachable for the SYNC plane.** LiteLLM dials it
  directly. The async plane does not: a worker calls its own model server over loopback.
  So a node whose model server stays bound to `127.0.0.1` serves queued jobs perfectly
  while `/v1/chat/completions` for its tier cannot connect — worth knowing before opening
  an unauthenticated inference port.

## 11. The stack, and how to work on it

A coordinator and a worker, meeting only at documented seams — the Redis queue contract
and HTTP — never in shared code.

**`cbk-server` (Python)**: FastAPI + Uvicorn, LiteLLM for the sync plane, redis-py over
Streams with consumer groups, SQLite via SQLModel with Alembic migrations as the durable
system of record, a plain asyncio coordinator loop (no scheduler library), uv for the
environment. Serves the htmx dashboard, all assets vendored — LAN-only, so no CDN.

**`cbk` (Python)**: one zipapp. `redis` and `httpx` only; `cryptography` needed just to
verify an update. One asyncio loop with two tasks (pull loop and heartbeat), so the
heartbeat can mutate the loop's paused flag and capability set with no lock discipline to
get wrong. Hardware probe is stdlib only — `sysctl`, `/proc/meminfo`,
`GlobalMemoryStatusEx` via `ctypes` — with safe fallbacks, rather than a dependency that
would break the pure-Python artifact.

> The worker was originally C#/.NET, chosen for single-file distribution. That advantage
> did not survive contact: no working AOT, and a macOS bundle that needed Homebrew. One
> `py3-none-any` zipapp runs everywhere with no build matrix, so the worker is Python and
> the .NET implementation is gone. What survives is the *shape* — two artifacts that share
> no code and meet only at a documented seam.

**The contract is the source of truth.** `contract/*.schema.json` defines every message
crossing the seam, and both components carry a conformance test, so a type definition
cannot silently drift from the wire. Change the schema and both sides update or their
tests fail.

### Build, test, run

```bash
docker run -d --name cbk-redis -p 6379:6379 redis:7-alpine

cd server && uv venv && uv pip install -e ".[dev]" && uv run pytest
CBK_API_KEY=… uv run cbk-server      # API + sync plane + dashboard on :8018

cd worker && uv venv && uv pip install -e ".[dev]" && uv run pytest
uv run ruff check src tests build.py
uv run python build.py               # → dist/cbk.pyz, the shipped artifact
```

```bash
python contract/validate.py          # schemas + fixtures
bash deploy/e2e/ci.sh                # the whole end-to-end suite
```

The e2e scripts each prove one property against real processes and a real Redis; they
share `deploy/e2e/lib.sh` for the harness. `USE_OLLAMA=1` runs them against real
inference instead of the zero-weight stub.

CI gates every PR on: both unit suites, the contract validator, the full e2e suite, the
worker's cross-platform tests on macOS and Windows, and a parse/lint pass over every
PowerShell and shell script.

### Worker self-update

The coordinator hosts a **release manifest** — version, artifact, SHA-256, signature —
which agents check on heartbeat. **Signed or nothing**: an update channel is
remote-code-execution by design, so the agent verifies an ECDSA P-256 signature against a
pinned public key, then the digest of what it downloaded, before writing a byte. The
channel itself is deliberately unauthenticated, because the signature is the boundary; an
attacker who can serve the file still cannot make a worker install it.

The previous artifact is retained as `cbk.prev.pyz`, so a bad release can be reverted by
hand. Per-node `auto_update: false` opts out.

`release.json` is **unsigned source data** — version, channel, protocol version, and an
artifact per release-key with its URL and SHA-256. The coordinator signs a manifest
**per request** from the private key, so the file on disk carries no signature and
publishing needs no key handling. It is read per request too, so editing it needs no
restart.

`GET /releases/<file>` serves **only files named by the current manifest**, matched on
basename. That is an allowlist, not a path join: `..` and absolute paths cannot escape the
release directory. One consequence to know before you need it — the moment the manifest
moves to a new version, the *previous* artifact stops being servable even though it is
still on disk. Rolling the channel back means restoring the old `release.json`, not just
pointing at the old file.

### Releasing a worker version touches three settings, not one

They all name a version, nothing reconciles them, and disagreement is silent:

| Setting | Governs | Read by |
|---|---|---|
| `CBK_WORKER_CURRENT_VERSION` | whether a node is judged `ok` or `stale` | the fitness check, on every heartbeat |
| `CBK_WORKER_ARTIFACT` | the build a **joining** node downloads | `GET /worker/artifact`, which bootstrap points the joiner at |
| `CBK_UPDATE_RELEASE` → `release.json` | the manifest **existing** nodes are offered | `_build_update_for`, on every heartbeat |

Bumping only the first two produces the worst of the three states: every node is told it
is `stale` while being offered nothing, because the release manifest still names the
version they already run. It reads exactly like "self-update is not configured" when it is
fully configured — signing key, channel, `auto_update` and all.

So a release is: publish `cbk-<ver>.pyz` into the release directory, rewrite
`release.json` (keeping a `.bak-<oldver>`), refresh the join artifact, and bump
`CBK_WORKER_CURRENT_VERSION`. Miss one and the fleet drifts quietly.

This is the same shape as the three registries in §6 — several records that each name
part of the truth and never check each other. Worth watching for as a pattern.

*Deferred:* canary rings and automatic crash-loop rollback. The binary swap and retention
are built and proven; deciding that a release is crash-looping needs multi-node
observation that does not exist yet.

## 12. Decisions worth not undoing

Short list, because reversing one of these quietly breaks something.

- **A queue, not a serving cluster.** The scarce resource is availability. Everything
  follows from taking an intermittent fleet seriously.
- **Pull, not push.** Subscription *is* liveness. A dispatcher would need a liveness table
  that is wrong the moment a laptop closes.
- **Streams, not lists.** The pending-entries list is what makes a dead worker's job
  recoverable at all.
- **Adopt LiteLLM.** The sync plane is a solved problem; rebuilding it buys nothing.
- **Address by capability or need, never by machine.** Adding or removing hardware must
  not touch a client.
- **Domain-agnostic, always.** Clients depend on clusterbuck; clusterbuck never names a
  client, a hostname or an employer.
- **`local_only` is absolute.** Not overridable by urgency, deadline or overflow. A
  privacy guarantee with an exception is not one.
- **Ability is a matrix, per artifact and task class.** A single number routes jobs
  wrongly, because quality is jagged.
- **Measure, never assume.** A fresh install or a changed digest inherits no score. Seeds
  are placeholders and are labelled as such.
- **A human approves weights.** Multi-GB pulls and disk reclamation are not unattended
  operations.
- **Every boundary is a documented protocol**, with a schema and a conformance test on
  both sides. It is what keeps the system polyglot and the two halves honest.
