# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## What clusterbuck is

A **LAN LLM worker network**: a domain-agnostic, distributed inference broker. Clients
submit jobs to one stable endpoint; clusterbuck runs them on whichever machine on the
LAN is capable and available — routing live requests now (sync plane), or queuing patient
work until a worker is contactable, waking one if worth it (async plane). The name is a
nod to *"pass the buck"* — the broker hands each job to whichever worker is up.

**Status: M0–M5 complete** (the whole roadmap). The design is complete and the system is
built and proven end-to-end (fake stub + Ollama), server + worker, across the
cross-language contract. Milestones:

- **M0 — async core loop:** submit → Redis Streams queue → C# worker → model server →
  result → poll.
- **M1 — sync plane:** LiteLLM serving OpenAI-compatible `/v1/chat/completions` +
  `fleet.yaml` registry + `cbk fleet`.
- **M2 — availability:** escalation engine (waitable → necessary on age, ADR 18),
  Wake-on-LAN coordinator (liveness = consumer `idle`, ADR 24), basic reservations
  (reconciler-tick lifecycle, ADR 17).
- **M3 — visibility:** usage metering (metadata-only) + `/usage` avoided-cloud-spend
  headline + the htmx dashboard at `/` (vendored, no CDN).
- **M4 — self-managing fleet:** dynamic registry (`/nodes/enroll` + heartbeat + join
  tokens, ADR 9), client attention lease (§9), worker self-enrollment (hardware probe,
  presence ladder + `cbk pause`, ADR 10), and signed self-update (cross-language ECDSA
  P-256 verify + skew gate, ADR 13).
- **M5 — optimisation:** ability matrix (ADR 15) + tier-1 programmatic eval + need-shaped
  ability routing (ADR 16), with `/ability`.
- **M6 — model management (ADR 25):** workers **discover** their models over the generic
  OpenAI `/v1/models` (portable; filesystem scanning deliberately not primary) and report
  installed/loaded/digests; the coordinator holds a **model catalog** and raises
  **proposals** (upgrade / re-eval on digest change / reclaim) through three gates — fits
  (RAM + the owner's disk quota) → beats the incumbent's measured ability → **human
  approval** (per-node `auto_approve` is opt-in; reclaim is never auto-approved); an
  approved action is executed by the worker via a per-server **model-manager adapter**
  (Ollama), presence-gated so no multi-GB pull lands under an active owner. A fresh install
  or changed digest **never inherits** an ability score — it demands re-measurement.

A single coordinator loop runs the escalation / reservation / attention / usage ticks.
Deliberately deferred (needs real hardware, a judge model, real usage data, or multi-node —
documented in code + ADRs): true Wake-on-LAN to real MACs, multi-node canary/rollback +
binary self-replacement, OS presence detection, judge-based eval tiers (2/3) +
Bradley-Terry/Elo + calibration, the model catalog + real weight downloads, and
cost-quality-arbitrage planning. See *Build / test / run* below.

## CRITICAL: keep it domain-agnostic

clusterbuck is **infrastructure**, Apache-2.0, and shared publicly. It must know **nothing**
about the applications that use it.

- **Never** add references to specific client applications, their domain concepts, personal
  machine names/hostnames, home-network specifics, or any employer. Clients depend on
  clusterbuck; clusterbuck never depends on, or names, a client.
- clusterbuck only ever deals in **jobs, capabilities, and results**.
- Example hardware in docs must stay generic ("an always-on ~16 GB node", "a 64 GB
  workstation that sleeps") — no real hostnames.
- If a concrete integration example is needed, describe a generic client, not a named one.

Treat any leak of the above as a bug.

## Where the design lives

- [DESIGN.md](../DESIGN.md) — the overview: purpose, one-queue/many-policies, the two
  planes, pull-based capability queues, components, job model, wake, MVP scope.
- [docs/architecture.md](../docs/architecture.md) — internal shape; what we build vs.
  adopt (LiteLLM = sync plane; we build the async plane + coordinator); request lifecycles.
- [docs/protocols.md](../docs/protocols.md) — the concrete spec of every boundary.
- [docs/deployment.md](../docs/deployment.md) — .NET RIDs/AOT, Raspberry Pi/arm64, node
  roles, GPU/Metal notes, wake config.
- [docs/fleet-management.md](../docs/fleet-management.md) — self-managing fleet: node
  enrollment + hardware probe, machine profiles, presence modes/model ladder, planner +
  model catalog, cloud governors (privacy classes, budget), usage accounting, self-update.
- [docs/model-evaluation.md](../docs/model-evaluation.md) — the ability score: per-task-class
  measurement (programmatic / checklist-judge / pairwise-Elo), anchored versioned 1–10
  scale, need-shaped addressing (`task_class` + `min_ability`), cost-quality arbitrage.
- [docs/decisions.md](../docs/decisions.md) — ADR-lite rationale for the key choices.
- [docs/related-projects.md](../docs/related-projects.md) — survey of adjacent projects and
  why they don't fit this niche (keep collating).

## Planned stack (once code starts)

Full plan in [docs/implementation.md](../docs/implementation.md). In brief — **two
components, two languages**, meeting only at documented seams (Redis queue contract +
HTTP), never in shared code:

- **`cbk-server` — Python** (one always-on box; ecosystem-heavy): FastAPI, **LiteLLM**
  (native fit now the server is Python), redis-py (Redis **Streams + consumer groups**),
  SQLite as durable system of record, APScheduler, uv. Serves the htmx dashboard.
- **`cbk` — C#/.NET Native AOT** (fans out to every node; single-file self-update):
  worker loop + hardware probe + updater + Spectre.Console CLI; StackExchange.Redis;
  `HttpClient` to the local model server (**no vendor SDK**).
- **Shared contract:** JSON Schema in `contract/` (source of truth) + a conformance test
  on each side so the two type definitions can't drift.

See [docs/decisions.md](../docs/decisions.md) ADR 7 (split rationale) and ADR 22 (contract).
- **Broker + result store:** Redis (language-agnostic queue contract).
- **Sync gateway:** LiteLLM (adopted, off the shelf) — do not reimplement OpenAI routing.
- **Model servers:** Ollama / llama.cpp / vLLM / LM Studio — off the shelf, called over
  the OpenAI HTTP API. Never bind to a vendor SDK; the worker speaks the wire protocol.

Every seam is a **documented protocol** (see protocols.md) so the system stays polyglot:
the C# worker is a *reference* implementation, not a constraint.

## Build / test / run

Two components, built independently; they meet only at `contract/` + Redis + HTTP.

**Prereqs:** .NET 10 SDK, Python 3.12+, `uv`, Docker (for Redis), and a model server
(Ollama for dev; a zero-weight stub for tests — `server/tools/fake_model_server.py`).

```bash
# Redis (broker + result store) — one container for dev
docker run -d --name cbk-redis -p 6379:6379 redis:7-alpine

# --- server (Python) ---
cd server
uv venv && uv pip install -e ".[dev]"
uv run pytest                 # contract conformance + submit/poll API + sync plane + fleet
uv run cbk-server             # async job API + sync /v1/chat/completions + dashboard at /
                              #   (CBK_PORT, CBK_REDIS_URL, CBK_DB_PATH, CBK_FLEET_PATH,
                              #    CBK_CLOUD_FALLBACK_MODEL, CBK_CLOUD_BUDGET_MONTHLY)

# --- worker (C#/.NET) ---
cd worker
dotnet build                  # JIT for dev; AOT publish is a later packaging step (ADR 19)
dotnet test                   # contract conformance + serialization round-trip (no infra)
dotnet run --project src/Clusterbuck.Worker -- work    # start the worker loop
# CLI: `cbk work | submit --prompt … | status <id> | fleet | enroll --token … | pause | resume`

# --- end-to-end (proves the loop on one node; USE_OLLAMA=1 for real inference) ---
bash deploy/e2e/run.sh            # M0 async: submit → queue → worker → result → poll
bash deploy/e2e/queue-and-wait.sh # async: job parks as queued, drains when a worker joins
bash deploy/e2e/sync.sh           # M1 sync: /v1/chat/completions via LiteLLM
bash deploy/e2e/escalation.sh     # M2a: waitable job escalates to necessary (no worker)
bash deploy/e2e/reservation.sh    # M2b: reservation confirmed → warming → open (reconciler)
bash deploy/e2e/usage.sh          # M3a: completed job metered; avoided-cloud-spend > 0
bash deploy/e2e/enroll.sh         # M4b: enroll → registry → heartbeat → pause
bash deploy/e2e/ability.sh        # M5: need-shaped {task_class,min_ability} routing
bash deploy/e2e/discovery.sh      # M6a: models learned by observation, not config
bash deploy/e2e/install.sh        # M6c: propose → approve → pull → discover → re-eval
```

The **contract** (`contract/*.schema.json`) is the source of truth; both sides' tests
assert conformance so the two type definitions can't drift (ADR 22). CI should gate every
PR on the contract + both conformance suites (see [docs/implementation.md](../docs/implementation.md) → CI/CD).

## Conventions

- Design phase: keep DESIGN.md as the concise overview; put depth in `docs/`. Update
  `docs/decisions.md` when a choice with alternatives is made, so the *why* is preserved.
- When code is added, document build/test/run commands here (e.g. `dotnet build`,
  `dotnet test`, publish per RID) and prefer a CI matrix for per-OS artefacts.
- Match existing doc voice; small, focused changes.

## Repo / git

- License: Apache-2.0, © 2026 Nick Trout (see LICENSE/NOTICE).
- Repo-local git identity: `billyquith <chinbillybilbo@gmail.com>`.
- Remote: `git@github.com:billyquith/clusterbuck.git` (branch `main`).
- End commit messages with the `Co-Authored-By` trailer per the harness convention.
