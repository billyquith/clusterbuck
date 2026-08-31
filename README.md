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
eval harness that measures new models as ordinary fleet jobs. A subsequent hardening pass
added shared-secret auth across the admin surface (ADR 26) and a visibility-timeout reaper
so an abandoned job is redelivered rather than stranded (ADR 20). Honest limits are listed
under *Not yet real* below.

## Quick start (one machine)

Needs Python 3.12+, [uv](https://docs.astral.sh/uv/), a Redis 7+ instance, and an
OpenAI-compatible model server (Ollama for real inference; a zero-weight stub ships for tests).

Redis is the broker and result store; run it however you like — a native package is fine and
is what the Linux installer uses:

```bash
sudo apt-get install -y redis-server        # native package, or...
docker run -d --name cbk-redis -p 6379:6379 redis:7-alpine   # ...a container

cd server && uv venv && uv pip install -e ".[dev]"
uv run pytest                    # server suite (needs Redis)
CBK_API_KEY=choose-a-secret uv run cbk-server    # job API + /v1/chat/completions + dashboard at /

cd ../worker && uv venv && uv pip install -e ".[dev]"
uv run pytest                    # worker suite
uv run cbk work                  # start pulling jobs
```

The coordinator listens on **8018** by default (`CBK_PORT` to change it). Submit patient work
to `POST /jobs` (`{task_class, min_ability}` or an explicit `capability`) and poll
`GET /jobs/{id}`, or point any OpenAI SDK at `/v1/chat/completions`. `bash deploy/e2e/ci.sh`
runs the end-to-end proofs; it flushes Redis between scripts via `docker exec cbk-redis` by
default, so against a natively installed Redis set `CBK_REDIS_CLI='redis-cli -h localhost'`
(CI does exactly this). `CBK_REDIS_URL` picks the instance itself.

The worker ships as a single **`py3-none-any` zipapp** (`cbk.pyz`, ~2.8 MB, [`worker/`](worker))
that runs on every OS and architecture. Nodes need Python 3.11+. The conformance suite in
[`contract/`](contract) ensures the worker and server agree on the wire format.

## Setting up a home network

Two kinds of machine, and they are deliberately not the same machine:

- **Coordinator** — one per deployment, always on, low power. A Raspberry Pi 4/5 is ideal.
  It runs the job API, Redis, the sync gateway and the fleet manager, and does no inference
  itself, so it can stay up around the clock for a couple of watts.
- **Workers** — as many as you have. Each is a machine with real RAM and a model server
  (Ollama by default). These are the boxes you *want* to sleep between jobs; a worker that
  is asleep is a feature, not an outage — the coordinator queues for it and wakes it.

Rough worker sizing: a quantised model needs about its weight in RAM, so 8 GB covers 3–8 B,
24 GB covers 14–32 B, and 70 B wants 48 GB+. Apple Silicon makes a particularly good worker
(fast unified memory, Metal for free, sleeps well).

**On macOS, prefer LM Studio over Ollama as the model server.** It can run **MLX** builds —
Apple's own array framework, tuned for Apple Silicon — which typically generate tokens
20–30% faster than the equivalent GGUF through llama.cpp/Metal. On a machine you are buying
for inference anyway, that is free throughput. Two flags change on such a node:

```bash
--model-server  http://127.0.0.1:1234/v1   # LM Studio's port, not Ollama's 11434
--model-manager none                       # the adapter speaks Ollama's native API
```

The trade-off is confined to model *management*, not to running jobs. Discovery still works,
because it goes through the portable OpenAI `/v1/models` endpoint that every server implements
(ADR 25). But loaded-model and digest reporting are Ollama-native, so on an LM Studio node they
go quiet: an approved install has to be done by hand in LM Studio, and a model updated in place
won't be spotted by digest change and re-measured automatically. Inference, routing, eval and
ability scoring are all unaffected.

### Installing

Scripted installers live in [`install/`](install) for both roles and all three platforms.
Coordinator first:

```bash
curl -fsSL https://raw.githubusercontent.com/billyquith/clusterbuck/main/install/coordinator/install.sh | sudo bash -s -- --lan-redis
```

It creates a `clusterbuck` system user under `/opt/clusterbuck`, installs Redis from apt and
gives it a `requirepass`, mints the API key, writes `/etc/clusterbuck/server.env`, and enables
the systemd unit. No Docker involved — Redis runs as an ordinary system service. (The Windows
coordinator installer is the one exception: it runs Redis in Docker Desktop, for want of a
native package.)
`--lan-redis` is what unbinds Redis from loopback — remote workers cannot reach the queue
without it. The generated secrets land in `/root/.cbk-secrets`.

Then each worker, once you have built `cbk.pyz` on the coordinator
(`cd /opt/clusterbuck/worker && python3 build.py`) and copied it over:

```bash
sudo bash install/worker/install.sh \
  --coordinator http://COORDINATOR_HOST:8018 \
  --redis-url   'redis://:REDIS_PW@COORDINATOR_HOST:6379/0' \
  --model       qwen2.5:7b \
  --artifact    /tmp/cbk.pyz \
  --token       JOIN_TOKEN
```

The worker probes its own hardware, enrols itself with the coordinator, reports the models it
finds, and starts pulling. `install.ps1` equivalents exist for Windows, and
[`cbk-install/`](cbk-install) has the same coordinator sequence broken into inspectable
per-step scripts if you would rather not pipe a script into `sudo bash`.

### The two reachability rules

Most setup problems on a real LAN are one of these, and they are easy to miss because the
two planes take different network paths:

1. **Async plane — every worker must reach Redis on the coordinator.** That is what
   `--lan-redis` is for. Redis holds prompts and completions in plaintext, so it must keep
   its `requirepass`; do not expose it beyond the LAN.
2. **Sync plane — the coordinator must reach each worker's model server.** LiteLLM calls the
   `model_server` URL from the registry *directly*; the worker is not in the sync path at all.
   A worker whose Ollama or LM Studio is bound to loopback therefore serves `POST /jobs`
   perfectly while being invisible to `/v1/chat/completions`. Bind the model server to the LAN
   (`OLLAMA_HOST=0.0.0.0` for Ollama, the equivalent listen-on-LAN setting in LM Studio) and give the
   capability a LAN address rather than `localhost`. The shipped
   [`server/fleet.yaml`](server/fleet.yaml) is a single-node example, so every `model_server`
   in it is `localhost`.

Check both with `GET /healthz` and the dashboard at `/`, then submit one job through each
plane. [docs/installation.md](docs/installation.md) is the full walkthrough — manual steps for
either role, service files, verification and updates — and [docs/deployment.md](docs/deployment.md)
covers node roles, GPU/Metal notes and wake configuration.

### Security on a home LAN

`CBK_API_KEY` is the operator shared secret — **unset means the API is unauthenticated**,
which is fine on a trusted LAN and warned about at startup. Set it if anything untrusted
shares the network: without it the admin surface, including model installs and deletions, is
open to anyone who can route to the port. Clients send `X-CBK-Api-Key` or a bearer token; the
dashboard takes `/?key=…` once and keeps a cookie. `/healthz`, `/static/*`, enrolment (join
token) and heartbeat (node key) are exempt.

## Not yet real

Built and tested, but deliberately incomplete where a single dev machine cannot prove it:
judge-based eval tiers 2/3 (Bradley-Terry/Elo, anchor calibration), canary rings and automatic
crash-loop rollback (the signed-update path, the binary swap, `cbk.prev` retention and manual
rollback are all built and proven end-to-end — what is missing is *deciding* a release is
crash-looping, which needs multi-node observation), real multi-GB weight downloads (the pull
path is stub-proven), true Wake-on-LAN to real MACs across machines, OS presence detection
(presence is set manually), attached-endpoint proxy workers, async-plane cloud overflow and
budget *enforcement* (the budget figure is display-only), and callbacks (`callback_url` is
accepted but ignored — poll instead).

See [DESIGN.md](DESIGN.md) for the overview, and [`docs/`](docs/) for detail — [installation](docs/installation.md),
[architecture](docs/architecture.md), [protocols](docs/protocols.md), [deployment](docs/deployment.md),
[fleet management](docs/fleet-management.md), [model evaluation](docs/model-evaluation.md),
[implementation](docs/implementation.md), [decisions](docs/decisions.md),
[related projects](docs/related-projects.md).

## License

Licensed under the Apache License, Version 2.0. Copyright 2026 Nick Trout.
See [LICENSE](LICENSE) and [NOTICE](NOTICE).
