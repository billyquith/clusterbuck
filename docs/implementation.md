# Implementation

The concrete stack for building clusterbuck. The design docs define *what* and *why*;
this is *how*. Choices favour a small dependency surface, the no-vendor-SDK rule the
protocols impose (everything spoken over documented wire protocols), and playing each
component to its strength. Nothing here is built yet — this is the plan the MVP follows.

## Two components, two languages

clusterbuck's two halves have opposite needs, so they're built in different languages —
which the protocol-first design makes clean, since they only ever meet at documented
seams (Redis queue contract + HTTP), never in shared code.

| Component | Language | Why | Distribution |
|---|---|---|---|
| **`cbk-server`** — job API, sync front, escalation/reservation engines, coordinator (WoL, registry, planner), dashboard | **Python** | The ecosystem-heavy half (LiteLLM, eval/dataset tooling, provider libs) and it runs on **one box you control**, so Python's distribution weakness doesn't apply | one always-on node; admin-updated |
| **`cbk`** — worker loop, hardware probe, self-updater, CLI | **C#/.NET (Native AOT)** | Fans out to every heterogeneous node and self-updates; needs a lean, dependency-light, instant-start single binary — Python's distribution weakness bites hardest exactly here, and the worker needs **no** LLM libraries anyway | every node; single-file self-update via signed manifest |

Rationale and the alternatives weighed (all-Python, all-C#, split) are in
[decisions.md](decisions.md) ADR 7. The worker's language is **reversible** behind the
protocol — if the C# distribution advantage ever stops being worth the second toolchain,
the worker could be rewritten in Python without touching the server.

### The shared contract

With two languages the job/result types can't be one shared library, so **the wire
contract is the source of truth**: [protocols.md](protocols.md) plus a machine-readable
**JSON Schema** in `contract/` (job, result, enrollment, heartbeat, reservation,
attention, update-manifest). Both sides validate against it; a contract test in each
language asserts round-trip conformance, so the two type definitions can't silently
drift. Schema change → both sides update or their contract tests fail.

## Server stack (Python)

| Concern | Choice | Notes |
|---|---|---|
| Runtime | **Python 3.13** | |
| Web framework | **FastAPI** + Uvicorn | async, OpenAPI emitted for free (→ any-language clients), Pydantic models validate the contract |
| Sync gateway | **LiteLLM** — in-process (library) or as the proxy | native fit now the server is Python; routing/fallback/spend to local workers + cloud. Provider keys held here |
| Broker / queue | **Redis** via **redis-py**, using **Streams + consumer groups** | Streams' pending-entries list + `XAUTOCLAIM` give visibility-timeout / reaper semantics *natively* — no hand-rolled in-flight tracking (ADR 20) |
| System of record | **SQLite** (`sqlite3` / SQLModel) | registry, usage metering, ability matrix, reservations, catalog. Zero-ops, runs on a Pi. Redis stays purely the broker |
| Scheduling | **APScheduler** (or asyncio tasks) + cron expressions | escalation scans, reservation warm-timers, heartbeat aging, scheduled wakes |
| Cloud/model calls | LiteLLM (for sync); `httpx` for anything direct | **no raw provider SDKs** in app code — LiteLLM owns provider quirks |
| Wake-on-LAN | ~10 lines of `socket` | magic packet = 6×`0xFF` + 16×MAC |
| Update signing | **ECDSA P-256** (`cryptography`) | server signs release manifests with a key the workers pin |
| Packaging / deploy | **uv** for env + lockfile; launchd/systemd unit | one box, so a venv + service is fine — no bundling needed |
| Tests | **pytest** + **Testcontainers** (real Redis) + FastAPI `TestClient` | plus the cross-language contract test |

## Worker stack (C#/.NET)

| Concern | Choice | Notes |
|---|---|---|
| Runtime | **.NET 10 (LTS)**, Native AOT single-file per RID | lean, instant-start, no runtime install on the node |
| Redis | **StackExchange.Redis** | consumes the same Streams consumer-group contract |
| HTTP out (local model server) | `HttpClient` + `System.Text.Json` (source-gen, AOT-friendly) | speaks the OpenAI wire protocol to Ollama/llama.cpp/etc. — **no vendor SDK** |
| CLI | **Spectre.Console.Cli** | `cbk work / submit / status / fleet / enroll-token / pause` |
| Hardware probe | small per-OS shims (`sysctl` / `/proc+/sys` / WMI); throughput bench = a timed call to the local model server | no heavyweight hardware-info dependency |
| Update verify | **ECDSA P-256** (`System.Security.Cryptography`) | verifies the server's signed manifest against a pinned public key before applying |
| mDNS | OS-native responder files in `deploy/`; browse via **Zeroconf** later | MVP uses a configured coordinator URL — the designed fallback |
| Tests | **xUnit** + Testcontainers (Redis) | plus the shared contract test |

## CI/CD

- **Worker:** GitHub Actions RID matrix (`osx-arm64`, `linux-x64`, `linux-arm64`,
  `win-x64`) → AOT publish → sign → publish release manifest (matches
  [deployment.md](deployment.md); manifest generation *is* the release step).
- **Server:** build/test on Linux + arm64 (Pi target); ship as a uv-locked service.
- **Contract:** the JSON Schema + both-language conformance tests gate every PR.

## UI — three surfaces

1. **Web dashboard** (primary), served by `cbk-server` (FastAPI static + templates) with
   **htmx** hitting the existing JSON APIs — no separate frontend build chain, all assets
   vendored (LAN-only: no CDN). Shows: fleet (nodes, presence mode, loaded vs installed
   models), queue depths by capability/urgency with oldest-age, reservations timeline,
   the **avoided-cloud-spend headline** + budget burn, catalog upgrade proposals with
   approve buttons, and join-token minting (ADR 21).
2. **CLI (`cbk`)** — admin/dev surface, shipped inside the worker binary so every node
   has it: `submit`, `status`, `fleet`, `enroll-token`, `pause`.
3. **Owner controls on shared machines** (mode toggle, eviction hotkey) — **deferred**.
   No clean cross-platform tray story without a heavy dependency; MVP is `cbk pause` +
   config, with per-OS menubar shims later. An honest cut.

## Suggested layout

```
clusterbuck/
├── contract/                     # JSON Schema — the source-of-truth wire contract
├── server/                       # cbk-server — Python (FastAPI, LiteLLM, redis-py, SQLite)
│   ├── clusterbuck/              #   api, engines (escalation/reservation), coordinator, planner
│   ├── web/                      #   htmx templates + vendored static assets
│   ├── tests/                    #   pytest + Testcontainers + contract conformance
│   └── pyproject.toml            #   uv-managed
├── worker/                       # cbk — C#/.NET AOT
│   ├── src/Clusterbuck.Worker/   #   worker loop + probe + updater + CLI
│   ├── tests/                    #   xUnit + Testcontainers + contract conformance
│   └── Clusterbuck.Worker.sln
├── deploy/                       # launchd/systemd units, Avahi/mDNS service files
└── docs/                         # (this design set)
```

## Build order (follows the design's phasing)

- **M0 — the loop.** Contract schema → Redis Streams queue → C# worker pulls → calls
  Ollama → result written → Python submit/poll API. Proves the async plane end to end,
  and the cross-language contract, on one node.
- **M1 — usable.** LiteLLM for the sync plane; `fleet.yaml` seed; `cbk` CLI.
- **M2 — availability.** Urgency + escalation engine; Wake-on-LAN; basic reservations.
- **M3 — visibility.** Dashboard; SQLite usage metering; avoided-cloud-spend headline;
  budget governor.
- **M4 — self-managing.** Enrollment + hardware probe; presence ladder; signed
  self-update with canary + rollback.
- **M5 — optimisation.** Model catalog + eval-gated upgrades; planner suggestions;
  cost-quality arbitrage.

Evaluation and the catalog come last deliberately: they need real usage data and a real
model mix to produce anything worth acting on.
