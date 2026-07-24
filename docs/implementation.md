# Implementation

The concrete stack for building clusterbuck. The design docs define *what* and *why*;
this is *how*. Choices favour a small dependency surface, cross-platform single-file
distribution, and the no-vendor-SDK rule the protocols impose (everything spoken over
documented wire protocols). Nothing here is built yet — this is the plan the MVP follows.

## Two artifacts: AOT worker, JIT server

Not everything wants Native AOT. Split the build in two:

| Artifact | Publish | Contains | Distribution |
|---|---|---|---|
| **`cbk`** | Native AOT, single-file, per RID | worker loop, hardware probe, self-updater, and the CLI (`work` / `submit` / `status` / `fleet` / `pause` / `enroll-token`) | every node; self-updates via the signed manifest |
| **`cbk-server`** | Self-contained (JIT) | job API, escalation + reservation engines, coordinator (WoL, registry, planner), dashboard | the one always-on node; admin-updated |

Rationale (ADR 19): the worker is what fans out across the fleet and self-updates, so it
must be lean, dependency-light, and instant-start — Native AOT. The server lives on
exactly one machine and is updated by hand, so it keeps ASP.NET Core's full (non-AOT)
feature set — which is what lets the dashboard use whatever it likes and the server use
reflection-friendly libraries (Dapper, YamlDotNet) freely.

Both depend on a shared **`Clusterbuck.Core`** project — job/result types, the queue
contract, capability/urgency/privacy enums, config models. Core *is* the compile-time
enforcement of [protocols.md](protocols.md): if the wire contract changes, Core changes,
and both artifacts fail to build until they agree.

## Stack

| Concern | Choice | Notes |
|---|---|---|
| Runtime | **.NET 10 (LTS)** | current LTS; Native AOT is mature |
| Web framework | **ASP.NET Core minimal APIs** | AOT-friendly pattern, source-generated JSON, emits OpenAPI → any-language clients for free |
| Broker / queue | **Redis** via **StackExchange.Redis**, using **Streams + consumer groups** | Streams' pending-entries list + `XAUTOCLAIM` give the visibility-timeout / reaper semantics *natively* — no hand-rolled in-flight tracking over `BLPOP` lists (ADR 20) |
| Coordinator state | **SQLite** (`Microsoft.Data.Sqlite` + Dapper) | registry, usage metering, ability matrix, reservations, catalog. Durable, zero-ops, runs on a Pi. Redis stays purely the broker; SQLite is the system of record |
| Scheduling | `BackgroundService` + `PeriodicTimer`; **Cronos** for recurrence | escalation scans, reservation warm-timers, heartbeat aging, scheduled wakes |
| HTTP out (model servers, LiteLLM, cloud) | `HttpClient` + `System.Text.Json` + **`Microsoft.Extensions.Http.Resilience`** | **no OpenAI/Anthropic SDK** — same rule clients follow; the worker speaks the OpenAI wire protocol directly |
| Wake-on-LAN | ~20 lines of `UdpClient` | magic packet = 6×`0xFF` + 16×MAC; no dependency warranted |
| mDNS discovery | OS-native responder (launchd / Avahi service files in `deploy/`) advertising `_clusterbuck._tcp`; worker browses via **Zeroconf** later | .NET mDNS libs are weak; MVP uses a configured coordinator URL — the designed fallback anyway |
| Update signing | **ECDSA P-256** via `System.Security.Cryptography` | zero extra deps; agents verify the manifest signature against a pinned public key before applying |
| Hardware probe | small per-OS shims (`sysctl` / `/proc+/sys` / WMI); throughput bench = a timed call to the local model server | avoids a heavyweight hardware-info dependency; tok/s comes from the model server itself |
| CLI | **Spectre.Console.Cli** | tables/status rendering for `cbk status` / `cbk fleet` |
| Config | `Microsoft.Extensions.Configuration` + **YamlDotNet** for `fleet.yaml` | env + file; server-side (JIT) so YAML reflection is fine |
| Tests | **xUnit** + **Testcontainers** (real Redis) + `WebApplicationFactory` | integration tests exercise the actual queue contract |
| CI/CD | **GitHub Actions** RID matrix (`osx-arm64`, `linux-x64`, `linux-arm64`, `win-x64`) → sign → publish release manifest | matches [deployment.md](deployment.md); manifest generation *is* the release step |

## UI — three surfaces

1. **Web dashboard** (primary), served by `cbk-server`. Static assets + **htmx** hitting
   the existing JSON APIs — no separate frontend build chain on an infra repo, all assets
   vendored (LAN-only: no CDN, consistent with the CSP-free-but-offline posture). Shows:
   fleet (nodes, presence mode, loaded vs installed models), queue depths by
   capability/urgency with oldest-age, reservations timeline, the **avoided-cloud-spend
   headline** + budget burn, catalog upgrade proposals with approve buttons, and
   join-token minting. If richer interactivity is ever needed, Blazor is the C#-native
   upgrade path — available precisely because the server isn't AOT (ADR 21).
2. **CLI (`cbk`)** — admin/dev surface, shipped inside the worker binary so every node
   has it: `submit`, `status`, `fleet`, `enroll-token`, `pause`.
3. **Owner controls on shared machines** (mode toggle, eviction hotkey) — **deferred**.
   No good cross-platform tray/menubar story in .NET without pulling in Avalonia; MVP is
   `cbk pause` + config, with per-OS menubar shims added later. An honest cut.

## Suggested layout

```
clusterbuck/
├── src/
│   ├── Clusterbuck.Core/         # shared types, queue contract, config, enums
│   ├── Clusterbuck.Worker/       # cbk — AOT: worker loop + probe + updater + CLI
│   └── Clusterbuck.Server/       # cbk-server — JIT: API, engines, coordinator, dashboard
│       └── wwwroot/              # vendored htmx + static assets
├── tests/
│   ├── Clusterbuck.Core.Tests/
│   └── Clusterbuck.Integration/  # Testcontainers Redis + WebApplicationFactory
├── deploy/                       # launchd/systemd units, Avahi/mDNS service files
├── docs/                         # (this design set)
└── Clusterbuck.sln
```

## Build order (follows the design's phasing)

- **M0 — the loop.** Core types → Redis Streams queue → worker pulls → calls Ollama →
  result written → submit/poll API. Proves the async plane end to end on one node.
- **M1 — usable.** LiteLLM in front for the sync plane; `fleet.yaml` seed; `cbk` CLI.
- **M2 — availability.** Urgency + escalation engine; Wake-on-LAN; basic reservations.
- **M3 — visibility.** Dashboard; SQLite usage metering; avoided-cloud-spend headline;
  budget governor.
- **M4 — self-managing.** Enrollment + hardware probe; presence ladder; signed
  self-update with canary + rollback.
- **M5 — optimisation.** Model catalog + eval-gated upgrades; planner suggestions;
  cost-quality arbitrage.

Evaluation and the catalog come last deliberately: they need real usage data and a real
model mix to produce anything worth acting on.
