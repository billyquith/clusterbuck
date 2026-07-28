# clusterbuck

A **LAN LLM worker network**. Submit inference jobs from any tool on the home
network; clusterbuck runs them on whichever machine is capable and available —
routing live requests now, or queuing patient work until a worker is contactable
(waking one if worth it). *(Name: "pass the buck" — the broker hands each job to
whichever worker is up.)*

Domain-agnostic infrastructure: clusterbuck only ever sees jobs, capabilities, and
results — never anything about the client applications that use it.

**Status:** implemented and proven end-to-end on a single node — async job plane, sync
OpenAI-compatible gateway, wake/escalation/reservations, usage metering + dashboard,
self-enrolling fleet with model discovery and human-approved model installs, and a tier-1
eval harness that measures new models as ordinary fleet jobs. Honest limits are listed
under *Not yet real* below.

## Quick start

Needs Python 3.12+, [uv](https://docs.astral.sh/uv/), Docker (for Redis), and an
OpenAI-compatible model server (Ollama for real inference; a zero-weight stub ships for tests).
The .NET 10 SDK is optional — only for the C# worker.

```bash
docker run -d --name cbk-redis -p 6379:6379 redis:7-alpine   # broker + result store

cd server && uv venv && uv pip install -e ".[dev]"
uv run pytest                    # server suite (needs Redis)
CBK_API_KEY=choose-a-secret uv run cbk-server    # job API + /v1/chat/completions + dashboard at /

cd ../worker/python && uv venv && uv pip install -e ".[dev]"
uv run pytest                    # worker suite
uv run cbk work                  # start pulling jobs
```

Then submit patient work to `POST /jobs` (`{task_class, min_ability}` or an explicit
`capability`) and poll `GET /jobs/{id}`, or point any OpenAI SDK at `/v1/chat/completions`.
`bash deploy/e2e/ci.sh` runs the end-to-end proofs — `CBK_WORKER=both` runs them against both
worker implementations.

### Two workers

The worker ships in two interchangeable implementations with the same verbs, the same `CBK_*`
environment and the same contract. A node runs whichever suits it, and the coordinator offers
each the release artifact it can actually execute.

| | [`worker/python`](worker/python) | [`worker/dotnet`](worker/dotnet) |
|---|---|---|
| Artifact | **one** `cbk.pyz`, `py3-none-any` | **six**, one per platform |
| Size | ~2.8 MB | 72–81 MB each |
| Node needs | Python 3.11+ | nothing¹ |
| Role | the default | reference implementation |

¹ Except on macOS, where the "self-contained" binary hard-links Homebrew's brotli and will not
launch without it — measured, see [ADR 29](docs/decisions.md). Native AOT does not link at all.
That defect is why the Python worker exists.

Both are held to the same JSON Schema in [`contract/`](contract) by their own conformance
suites, and CI runs every end-to-end proof against both: anything that passes for one and fails
for the other is a contract violation by definition.

`CBK_API_KEY` is the operator shared secret — **unset means the API is unauthenticated**,
which is fine on a trusted LAN and warned about at startup. Redis needs its own
`requirepass`: it holds prompts and completions in plaintext.

## Not yet real

Built and tested, but deliberately incomplete where a single dev machine cannot prove it:
judge-based eval tiers 2/3 (Bradley-Terry/Elo, anchor calibration), the self-update *apply*
path (signature verification is proven; binary replacement, canary and rollback are not),
real multi-GB weight downloads (the pull path is stub-proven), true Wake-on-LAN to real MACs
across machines, OS presence detection (presence is set manually), attached-endpoint proxy
workers, async-plane cloud overflow and budget *enforcement*, and callbacks (`callback_url`
is accepted but ignored — poll instead).

See [DESIGN.md](DESIGN.md) for the overview, and [`docs/`](docs/) for detail — [architecture](docs/architecture.md),
[protocols](docs/protocols.md), [deployment](docs/deployment.md),
[fleet management](docs/fleet-management.md), [model evaluation](docs/model-evaluation.md),
[implementation](docs/implementation.md), [decisions](docs/decisions.md),
[related projects](docs/related-projects.md).

## License

Licensed under the Apache License, Version 2.0. Copyright 2026 Nick Trout.
See [LICENSE](LICENSE) and [NOTICE](NOTICE).
