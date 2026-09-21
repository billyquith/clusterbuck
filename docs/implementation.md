# Implementation

The concrete stack for building clusterbuck. The design docs define *what* and *why*;
this is *how*. Choices favour a small dependency surface, the no-vendor-SDK rule the
protocols impose (everything spoken over documented wire protocols), and playing each
component to its strength. This describes the **implemented** stack; where it differs from the code, the code wins.

## Two components, two languages

clusterbuck's two halves have opposite needs, so they're built in different languages —
which the protocol-first design makes clean, since they only ever meet at documented
seams (Redis queue contract + HTTP), never in shared code.

| Component | Language | Why | Distribution |
|---|---|---|---|
| **`cbk-server`** — job API, sync front, escalation/reservation engines, coordinator (WoL, registry, planner), dashboard | **Python** | The ecosystem-heavy half (LiteLLM, eval/dataset tooling, provider libs) and it runs on **one box you control**, so Python's distribution weakness doesn't apply | one always-on node; admin-updated |
| **`cbk`** — worker loop, hardware probe, self-updater, CLI | **Python** | Fans out to every heterogeneous node and self-updates, so it needs a lean, dependency-light artifact and **no** LLM libraries. Ships as one 2.8 MB `py3-none-any` zipapp (ADR 29) | every node; self-update via signed manifest |

Rationale and the alternatives weighed (all-Python, all-C#, split) are in
[decisions.md](decisions.md) ADR 7. The C# distribution advantage did not hold up (no working
AOT, and a macOS bundle that needs Homebrew — ADR 29), so the worker is Python: one
`py3-none-any` zipapp that runs everywhere without a build matrix.

### The shared contract

With two languages the job/result types can't be one shared library, so **the wire
contract is the source of truth**: [protocols.md](protocols.md) plus a machine-readable
**JSON Schema** in `contract/` (job, result, enrollment, heartbeat, reservation,
update-manifest). Both sides validate against it; a contract test in each
language asserts round-trip conformance, so the two type definitions can't silently
drift. Schema change → both sides update or their contract tests fail.

## Server stack (Python)

| Concern | Choice | Notes |
|---|---|---|
| Runtime | **Python 3.12+** | |
| Web framework | **FastAPI** + Uvicorn | async, OpenAPI emitted for free (→ any-language clients), Pydantic models validate the contract |
| Sync gateway | **LiteLLM** — in-process (library) or as the proxy | native fit now the server is Python; routing/fallback/spend to local workers + cloud. Provider keys held here |
| Broker / queue | **Redis** via **redis-py**, using **Streams + consumer groups** | Streams' pending-entries list + `XAUTOCLAIM` give visibility-timeout / reaper semantics *natively* — no hand-rolled in-flight tracking (ADR 20) |
| System of record | **SQLite** (`sqlite3` / SQLModel) | registry, usage metering, ability matrix, reservations, catalog. Zero-ops, runs on a Pi. Redis stays purely the broker |
| Scheduling | **APScheduler** (or asyncio tasks) + cron expressions | escalation scans, reservation warm-timers, heartbeat aging, scheduled wakes |
| Cloud/model calls | LiteLLM (for sync); `httpx` for anything direct | **no raw provider SDKs** in app code — LiteLLM owns provider quirks |
| Wake-on-LAN | ~10 lines of `socket` | magic packet = 6×`0xFF` + 16×MAC |
| Update signing | **ECDSA P-256** (`cryptography`) | server signs release manifests with a key the workers pin |
| Packaging / deploy | **uv** for env + lockfile; launchd/systemd unit | one box, so a venv + service is fine — no bundling needed |
| Tests | **pytest** + a real Redis (CI service container; `CBK_TEST_REDIS_URL` makes an unreachable broker a failure rather than a skip) + FastAPI `TestClient` | plus the cross-language contract test |

## Worker stack — Python (the default)

| Concern | Choice | Notes |
|---|---|---|
| Runtime | **Python 3.11+**, shipped as one `py3-none-any` **zipapp** | ~2.8 MB, every OS and arch, no build matrix (ADR 29) |
| Redis | **redis-py** (`redis.asyncio`) | same Streams consumer-group contract |
| Concurrency | one asyncio loop, two tasks (pull loop + heartbeat) | the heartbeat mutates the loop's paused flag and capability set; one event loop means no lock discipline to get wrong |
| HTTP out | **httpx** (`AsyncClient`) | OpenAI wire protocol — **no vendor SDK** |
| CLI | **argparse** | `work / submit / status / fleet / enroll / pause / resume` |
| Hardware probe | `sysctl` / `/proc/meminfo` / `GlobalMemoryStatusEx` via `ctypes` | stdlib only, best-effort with safe fallbacks |
| Update verify | **ECDSA P-256** via `cryptography` (DER) | deliberately *not* vendored: it is the verifier, so it cannot arrive through the channel it secures. Absent ⇒ self-update refuses, inference unaffected |
| Tests | **pytest** + real Redis (`CBK_TEST_REDIS_URL` makes an unreachable broker a failure, not a skip) | plus its own contract conformance suite |
| Lint | **ruff** | |


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
├── worker/                       # cbk — Python (one py3-none-any zipapp, ADR 29)
│   ├── src/cbk_worker/           #   loop + probe + updater + inventory + CLI
│   ├── tests/                    #   pytest + real Redis + contract conformance
│   └── build.py                  #   → dist/cbk.pyz
├── deploy/                       # launchd/systemd units, Avahi/mDNS service files
└── docs/                         # (this design set)
```

## Build order (follows the design's phasing)

- **M0 — the loop.** Contract schema → Redis Streams queue → Python worker pulls → calls
  Ollama → result written → server submit/poll API. Proves the async plane end to end,
  on one node.
- **M1 — usable.** LiteLLM for the sync plane; `fleet.yaml` seed; `cbk` CLI.
- **M2 — availability.** Urgency + escalation engine; Wake-on-LAN; basic reservations.
- **M3 — visibility.** Dashboard; SQLite usage metering; avoided-cloud-spend headline;
  budget governor.
- **M4 — self-managing.** Enrollment + hardware probe; presence ladder; signed
  self-update with canary + rollback.
- **M5 — optimisation.** Ability matrix (ADR 15) + tier-1 programmatic eval + need-shaped
  `{task_class, min_ability}` routing (ADR 16).
- **M6 — model management (ADR 25).** Workers discover their models over the generic
  `/v1/models`; the coordinator holds a catalog and raises proposals through three gates
  (fits → beats the incumbent's measured ability → human approval); an approved install is
  executed by the worker via a per-server model-manager adapter, presence-gated.
- **M7 — eval harness.** Unmeasured artifacts are measured as *ordinary fleet jobs*, so a
  freshly installed model earns a score and becomes routable.
- **Hardening.** Shared-secret auth (ADR 26), the visibility-timeout reaper (ADR 20), CI, and
  the contract validator.

Cost-quality arbitrage, and judge-based eval tiers 2/3, remain deferred — they need real usage
data and a judge model respectively.

Evaluation and the catalog come last deliberately: they need real usage data and a real
model mix to produce anything worth acting on.
