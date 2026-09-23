# clusterbuck

A broker between the tools you use and the machines you own. Submit a job to one stable
endpoint; clusterbuck runs it on whichever machine on the LAN is capable and free — now if
something is awake, later if it has to wait — and falls back to a cloud model only when
you allow it and the local fleet genuinely cannot.

*(Name: "pass the buck" — the broker hands each job to whichever worker is up.)*

Two things are the point. **Privacy**: a job marked `local_only` never leaves the LAN, at
any urgency. **Economics**: work that would have gone to a paid API runs on hardware you
already own, and the headline metric is *avoided cloud spend*.

It is domain-agnostic infrastructure. clusterbuck only ever sees jobs, capabilities and
results — never anything about the applications using it.

## How it works, in one minute

One front door, two paths behind it, sharing the same machines:

- **Sync plane** — an OpenAI-compatible endpoint (`/v1/chat/completions`) served by
  LiteLLM, for when something is awake and a person is waiting.
- **Async plane** — a durable queue for patient work. A job names a **capability tier**
  (`8b-extract`) or, better, a **need** (`task_class` + `min_ability`: "summarise this
  with a model scoring at least 6"). It waits on that queue until a capable worker pulls
  it — and if the job has earned it, the coordinator wakes a machine.

Workers **pull**; nothing is ever dispatched to a named node. Subscribing to a queue *is*
the liveness signal, which is what makes a fleet that sleeps and roams work at all. **A
worker that is asleep is a feature, not an outage.**

Model quality is measured, not assumed: each artifact carries a per-task-class ability
score derived from a deterministic suite, which is what lets a client ask for
"good enough" without naming a model.

[**docs/design.md**](docs/design.md) is the full picture — how it works and how to run it.
[docs/protocols.md](docs/protocols.md) is the exact wire contract.
[llms.txt](llms.txt) is the compact client-integration guide for AI-assisted projects.

## Quick start (one machine)

Needs Python 3.12+, [uv](https://docs.astral.sh/uv/), Redis 7+, and an OpenAI-compatible
model server (Ollama for real inference; a zero-weight stub ships for tests).

```bash
sudo apt-get install -y redis-server                          # native package, or...
docker run -d --name cbk-redis -p 6379:6379 redis:7-alpine    # ...a container

cd server && uv venv && uv pip install -e ".[dev]"
uv run pytest                                     # server suite (needs Redis)
CBK_API_KEY=choose-a-secret uv run cbk-server     # API + sync plane + dashboard

cd ../worker && uv venv && uv pip install -e ".[dev]"
uv run pytest
uv run cbk work                                   # start pulling jobs
```

The coordinator listens on **8018** (`CBK_PORT` to change it). Submit patient work to
`POST /jobs` and poll `GET /jobs/{id}`, or point any OpenAI SDK at
`/v1/chat/completions`. The dashboard is at `/` — first visit takes `/?key=…` once and
keeps a cookie.

```bash
bash deploy/e2e/ci.sh     # the end-to-end proofs
```

Those scripts flush Redis via `docker exec cbk-redis` by default; against a native Redis
set `CBK_REDIS_CLI='redis-cli -h localhost'` (CI does exactly that). `CBK_REDIS_URL`
picks the instance.

## Setting up a home network

Two kinds of machine, deliberately not the same machine:

- **Coordinator** — one per deployment, always on, low power. A Raspberry Pi 4/5 is
  ideal: it runs the job API, Redis, the sync gateway and the fleet manager, does no
  inference, and costs a couple of watts to leave up.
- **Workers** — as many as you have, each with real RAM and a model server. These are the
  boxes you *want* to sleep between jobs.

Rough sizing: a quantised model needs about its weight in RAM — 8 GB covers 3–8 B, 24 GB
covers 14–32 B, 70 B wants 48 GB+. Apple Silicon makes a particularly good worker: fast
unified memory, Metal for free, sleeps well.

**On macOS, prefer LM Studio over Ollama.** It runs **MLX** builds — Apple's own array
framework — which typically generate tokens 20–30% faster than the equivalent GGUF
through llama.cpp/Metal. Two flags change on such a node:

```bash
--model-server  http://127.0.0.1:1234/v1   # LM Studio's port, not Ollama's 11434
```

One flag, because the default `--model-manager auto` probes for whichever native API
answers. **Do not set `none` here** — that is the value that turns the adapter off, and a
node running with it reports nothing warm and no digests, which is indistinguishable from
a healthy idle machine. (`lmstudio` may be pinned explicitly to skip the probe; it is not
required.)

What still differs on such a node, and only this: **installs** are Ollama-only, so an
approved proposal has to be carried out by hand there; and **digests** are absent, because
LM Studio publishes a quantization and a size but no content hash, so a model updated in
place will not be spotted by a digest change. Residency *is* reported, which is what makes
a cold start provable and therefore `stats.load_s` measurable. Unloading needs LM Studio
0.4.0 or newer, which is where its unload endpoint arrives; older builds are told so
rather than failing quietly. Inference, routing, eval and ability scoring are unaffected.

## The two reachability rules

**Read these before installing anything.** Most setup problems on a real LAN are one of
them, and they are easy to miss because the two planes take different network paths.

1. **Async plane — every worker must reach Redis on the coordinator.** That is what
   `--lan-redis` is for. Redis holds prompts and completions in plaintext, so it keeps its
   `requirepass` and stays on the LAN.
2. **Sync plane — the coordinator must reach each worker's model server.** LiteLLM calls
   that URL *directly*; the worker is not in the sync path at all. So a worker whose
   Ollama or LM Studio is bound to loopback serves `POST /jobs` perfectly while being
   invisible to `/v1/chat/completions`. Bind the model server to the LAN
   (`OLLAMA_HOST=0.0.0.0`, or the equivalent in LM Studio) and give the capability a LAN
   address rather than `localhost`. The shipped [`server/fleet.yaml`](server/fleet.yaml)
   is a single-node example, so every `model_server` in it says `localhost`.

Check both with `GET /healthz` and the dashboard, then submit one job through each plane.

## Install the coordinator

One machine, always on; everything else joins it. The repo is private, so clone it rather
than piping a URL into a shell:

```bash
git clone git@github.com:billyquith/clusterbuck.git
sudo bash clusterbuck/install/coordinator/install.sh --lan-redis
```

It creates a `clusterbuck` system user under `/opt/clusterbuck`, installs Redis from apt
with a `requirepass`, mints the operator API key, writes `/etc/clusterbuck/server.env` and
enables the systemd unit. No Docker — Redis runs as an ordinary system service. (The
Windows coordinator installer is the exception: Docker Desktop, for want of a native
package.) Generated secrets land in `/root/.cbk-secrets`. If you would rather not hand a
whole script to `sudo`, read `install.sh` first — it is one linear file with no hidden
steps.

### Three settings that make joining work

The installer does not write these, and until they are set a joining worker gets a **404**
on its first call. `server.env` is deliberately never overwritten on re-run, so add them
by hand:

```bash
openssl rand -hex 24                            # the join password (16 chars minimum)
cd /opt/clusterbuck/worker && python3 build.py  # the build joiners download
sudoedit /etc/clusterbuck/server.env
sudo systemctl restart cbk-server
```

```ini
CBK_JOIN_PASSWORD=<what openssl printed>
CBK_WORKER_ARTIFACT=/opt/clusterbuck/worker/dist/cbk.pyz
CBK_BROKER_ADVERTISE_URL=redis://:REDIS_PW@192.168.1.10:6379/0
```

- **`CBK_JOIN_PASSWORD`** is the one secret you ever carry to a new machine. Generate it
  *on the coordinator* and let the operator key stay there: that key mints join tokens,
  approves model installs and deletes models, so a worker has no business holding it.
  Unset — or shorter than 16 characters — and joining is simply switched off.
- **`CBK_WORKER_ARTIFACT`** is the `cbk.pyz` served to joiners, so every node runs the
  same blessed build rather than whatever its own checkout contained. Rebuild it to change
  what future joins get.
- **`CBK_BROKER_ADVERTISE_URL`** is the Redis address *other machines* use, which is not
  the loopback one the coordinator uses for itself. Handing out loopback points each
  worker at its own localhost, and that failure is silent — install, enrolment and service
  start all succeed, and the worker then waits forever on a broker that is not there. So
  the coordinator refuses to advertise loopback at all: bootstrap answers **503** and names
  this variable.

## Join a worker

On the new machine, this is the whole procedure:

```bash
git clone git@github.com:billyquith/clusterbuck.git
cd clusterbuck
sudo bash install/worker/join.sh --coordinator http://COORDINATOR_HOST:8018 --model qwen2.5:7b
```

Windows is the same flags from an Administrator PowerShell:

```powershell
.\install\worker\join.ps1 --coordinator http://COORDINATOR_HOST:8018 --model qwen2.5:7b
```

It prompts for the join password and nothing else. **On Windows, Ctrl+V does not paste
into that prompt** — it sends a keystroke, because the prompt reads raw keys so the
password is never echoed. Use right-click (or Ctrl+Shift+V), type it, or set
`CBK_JOIN_PASSWORD` in the environment; a mangled paste is refused with an explanation
rather than sent. From there it:

1. exchanges the password for a **single-use join token** and the broker URL, so no Redis
   credential and no operator key is ever typed on the new machine;
2. downloads the coordinator's worker build rather than building one locally;
3. hands off to the platform installer, which writes the config, creates the service
   (systemd, launchd or a Scheduled Task), enrols the node and starts it;
4. checks what it built — and warns if this machine would serve a capability tier the
   coordinator does not define, the failure that otherwise looks perfectly healthy while
   no job ever routes.

`--dry-run` reports what it would do and installs nothing. On a macOS worker add
`--model-server http://127.0.0.1:1234/v1` for LM Studio; the adapter is detected.

Both clones need git access to a private repo. That is the one prerequisite joining
cannot fetch for you.

`install/worker/install.sh` and `install.ps1` are what step 3 runs; there is normally no
reason to invoke them yourself — only to re-point an existing node, or to join a
coordinator with bootstrap switched off, supplying the `--token` and `--redis-url` that
joining would otherwise have fetched.

## Verifying, and updating

```bash
curl http://COORDINATOR_HOST:8018/healthz                      # {"status":"ok"}
curl -X POST http://COORDINATOR_HOST:8018/jobs \
  -H "X-CBK-Api-Key: $APIKEY" -H 'content-type: application/json' \
  -d '{"task_class":"summarize","min_ability":4,"prompt":"say hello","urgency":"waitable"}'
curl http://COORDINATOR_HOST:8018/jobs/JOB_ID -H "X-CBK-Api-Key: $APIKEY"
```

Updating the coordinator: pull, reinstall the venv **only if `server/pyproject.toml` or
`uv.lock` changed**, restart. Database migrations run automatically on startup.

```bash
sudo -u clusterbuck git -C /opt/clusterbuck fetch origin
sudo -u clusterbuck git -C /opt/clusterbuck merge --ff-only origin/main
sudo systemctl restart cbk-server
```

Releasing a new worker version touches **three** settings, and they do not reconcile with
each other:

| Setting | Governs |
|---|---|
| `CBK_WORKER_CURRENT_VERSION` | whether a node reads as `ok` or `stale` |
| `CBK_WORKER_ARTIFACT` | the build a **joining** node downloads |
| `CBK_UPDATE_RELEASE` → `release.json` | what **existing** nodes are offered on heartbeat |

Bump only the first two and every node is told it is `stale` while being offered nothing
— which looks exactly like a broken update channel on a channel that is working fine.

```bash
cd worker && uv run python build.py                    # → dist/cbk.pyz
# on the coordinator, as the clusterbuck user:
install -m 0755 cbk.pyz /var/lib/clusterbuck/releases/cbk-0.18.0.pyz
cp release.json release.json.bak-0.9.0                 # rollback needs the old manifest,
                                                       # not just the old artifact
# rewrite release.json with the new version, url and sha256, refresh the join artifact,
# then bump CBK_WORKER_CURRENT_VERSION in server.env and restart.
```

`release.json` is read per request, so it needs no restart of its own; only the
`server.env` change does. Nodes with `auto_update` take the new build on their next
heartbeat — except on Windows, where self-update refuses by design and the `.pyz` must be
replaced by hand.

## Security on a home LAN

`CBK_API_KEY` is the operator shared secret. **Unset means the API is unauthenticated** —
fine on a trusted LAN, warned about at startup, and not fine if anything untrusted shares
the network: without it the admin surface, model installs and deletions included, is open
to anyone who can route to the port. Clients send `X-CBK-Api-Key` or a bearer token.
`/healthz`, `/static/*`, `/releases/*`, enrolment (join token) and heartbeat (node key) are
exempt.

`CBK_JOIN_PASSWORD` is a **second, weaker secret with one job**: letting a new machine ask
for a join token and the worker build. The routes it guards are exempt from the API key —
a joining machine has none, which is the point — so that password is the only thing in
front of the Redis credential bootstrap hands out. Keep it 16+ characters and rotate it by
editing `server.env` and restarting; already-joined nodes use their own per-node key and
are unaffected.

Note this is authentication, not authorisation: any holder of the operator key is the
operator. There is no submit-only credential today, so a client that only posts jobs still
holds a key that could delete a model.

## Not yet real

Built and tested, but deliberately incomplete where one dev machine cannot prove it:

- **Judge-based eval tiers 2 and 3** (checklist judging, Bradley-Terry/Elo, anchor
  calibration). Tier 1 is built, and is capped at ability 7 because compliance checks
  cannot certify the frontier band.
- **Canary rings and automatic crash-loop rollback.** The signed-update path, the binary
  swap, `cbk.prev` retention and manual rollback are all built and proven; what is missing
  is *deciding* a release is crash-looping, which needs multi-node observation.
- **Self-update on Windows** — refused by design, not unimplemented (see design.md).
- **Real multi-GB weight downloads** (the pull path is stub-proven), **true Wake-on-LAN to
  real MACs across machines**, **OS presence detection** (presence is set manually),
  **mDNS autodiscovery** (joining takes the coordinator URL as an argument),
  **attached-endpoint proxy workers**, **async-plane cloud overflow**, and
  **cost-quality arbitrage planning**.

## Documentation

- [docs/design.md](docs/design.md) — how it works and how to run it. Start here.
- [docs/protocols.md](docs/protocols.md) — the exact wire contract at every boundary.
- [docs/related-projects.md](docs/related-projects.md) — the adjacent tools, and why they
  do not fit this niche.

The machine-readable contract lives in [`contract/`](contract), and both components carry
a conformance test against it. The pre-merge documentation set, including all 40 numbered
decisions, is preserved at the `docs-before-merge` tag.

## License

Licensed under the Apache License, Version 2.0. Copyright 2026 Nick Trout.
See [LICENSE](LICENSE) and [NOTICE](NOTICE).
