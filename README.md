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

Needs .NET 10 SDK, Python 3.12+, [uv](https://docs.astral.sh/uv/), Docker (for Redis), and an
OpenAI-compatible model server (Ollama for real inference; a zero-weight stub ships for tests).

```bash
docker run -d --name cbk-redis -p 6379:6379 redis:7-alpine   # broker + result store

cd server && uv venv && uv pip install -e ".[dev]"
uv run pytest                    # server suite (needs Redis)
CBK_API_KEY=choose-a-secret uv run cbk-server    # job API + /v1/chat/completions + dashboard at /

cd ../worker/dotnet && dotnet build && dotnet test      # worker suite (no infra needed)
dotnet run --project src/Clusterbuck.Worker -- work
```

Then submit patient work to `POST /jobs` (`{task_class, min_ability}` or an explicit
`capability`) and poll `GET /jobs/{id}`, or point any OpenAI SDK at
`/v1/chat/completions`. `bash deploy/e2e/ci.sh` runs the twelve end-to-end proofs.

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
