# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## What clusterbuck is

A **LAN LLM worker network**: a domain-agnostic, distributed inference broker. Clients
submit jobs to one stable endpoint; clusterbuck runs them on whichever machine on the
LAN is capable and available — routing live requests now (sync plane), or queuing patient
work until a worker is contactable, waking one if worth it (async plane). The name is a
nod to *"pass the buck"* — the broker hands each job to whichever worker is up.

**Status: design only.** No application code yet. The deliverable so far is the design.

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
