# Deployment & platforms

How clusterbuck's two components are built and where they run, plus the node-role and
wake configuration. clusterbuck is a **split build** — a Python server and a Python
worker (see [`../DESIGN.md`](../DESIGN.md) and [implementation.md](implementation.md)).

## Server build (Python) — one box

The server (`cbk-server`: job API, LiteLLM sync front, coordinator, engines, planner)
runs on a single always-on node, so it ships as an ordinary **uv-locked Python service**
(venv + a launchd/systemd unit) — no bundling or cross-compilation needed. Python 3.13
runs on everything the coordinator might be, including **`linux-arm64` (Raspberry Pi)**,
which is the recommended coordinator host (below).

## Worker build — every node

The worker (`cbk`) ships as a single **`py3-none-any` zipapp**: one artifact for every OS and
architecture, ~2.8 MB, no build matrix.

```bash
cd worker && python build.py       # → dist/cbk.pyz
scp dist/cbk.pyz node:/opt/cbk/ && ssh node 'python3 /opt/cbk/cbk.pyz work'
```

Needs **Python 3.11+** on the node (almost always already present). For self-update, that
interpreter also needs `cryptography` (`pip install cryptography`); without it the worker runs
normally and refuses updates rather than applying an unverified one. `cryptography` is
deliberately not inside the zipapp — it is the update verifier, so it cannot be delivered
through the channel it secures.

## Node roles

Two roles, and one machine can be both:

- **Always-on node** — hosts the **server** (job API + LiteLLM sync front + coordinator)
  and Redis. Needs to be reliable and low-power more than fast; it mostly orchestrates.
- **Worker node** — runs a **worker** bound to the capability queues it can satisfy, plus
  a local model server. These are the machines that may sleep/roam.

## Raspberry Pi as the always-on coordinator

A Pi is an excellent **coordinator** node: the server role is nearly pure I/O
(Redis ops, health checks, WoL packets), so a low-power Pi humming 24/7 is arguably a
better "always-on brain" than a bigger machine, while the beefy machines that sleep do
the actual inference. The coordinator is the **Python server** — Python 3.13, Redis, and
LiteLLM all run on `linux-arm64`, so the whole always-on server fits on a 64-bit Pi.

If a Pi is ever used as a (non-inference) **worker** instead, the Python zipapp runs
on Pi: Python 3.11+ is available on **64-bit (`linux-arm64`)** (Pi 3/4/5, Zero 2 W)
and all modern Pis. (A Pi can't be a meaningful *inference* worker regardless — see below.)

**Caveat:** a Pi **cannot also be a meaningful inference worker** — too little RAM, no
usable GPU, so an 8B won't run usefully. So the clean fork is:

- *Pi = pure coordinator* (no local model), or
- *a small Mac/x86 box = coordinator + a resident small model* (Tier-0-style).

The protocol boundaries are identical either way.

## GPU / accelerator notes

- **macOS / Metal** — a Metal-backed model is capped by `iogpu.wired_limit_mb` (~67-75% of
  RAM by default); raise it only after freeing resident RAM. Keep large models warm with
  sensible unload timeouts so a shared machine doesn't thrash. That cap, not total RAM, is
  what the worker reports as `vram_gb`, and what capability proposals and the catalog's
  fits gate budget against — so a 64 GB machine offers ~48 GB to a model, not 64.
- Worker sizing is a property of the **model server**, not clusterbuck — clusterbuck only
  needs to know the resulting *capability* a node advertises.

## macOS: Local Network privacy blocks a launchd worker

On macOS 15 and later, a process needs **Local Network** permission to open a connection
to a private-range address, and the grant is **per binary**. A worker started by `launchd`
has no grant, so every LAN connection fails with `errno 65 / EHOSTUNREACH` — reported by
redis-py as `No route to host`.

It is easy to misdiagnose, because the usual checks all pass:

| check | result |
|---|---|
| `ping` / `nc -z` to the broker from a shell | works |
| the worker's own interpreter, run from a terminal | works |
| `curl` or `nc` from a launchd job | works — Apple binaries already hold the grant |
| **the worker's interpreter from a launchd job** | **fails** |

The discriminating test is to run the *same* third-party binary under `launchd` and from a
shell; if only the launchd one fails, this is why. A terminal session passes because the
child inherits the terminal's own grant.

Two fixes:

- **Grant it.** System Settings → Privacy & Security → Local Network, and enable the
  interpreter the worker runs under. It has to have attempted a connection at least once
  to appear. This is the durable fix, but it is a GUI action with no CLI equivalent
  (`tccutil` can reset a grant, not create one).
- **Or route off the local network.** An overlay-network address (e.g. a WireGuard/
  Tailscale-style 100.64.0.0/10 address) is not local-network scoped, so the gate does not
  apply. On the same LAN such a link is usually a direct connection, so there is no
  latency cost, and it encrypts broker traffic that would otherwise cross the LAN in
  plaintext. The trade is a dependency on that overlay being up.

Only the **broker and coordinator** connections are affected. A worker's own model server
on `127.0.0.1` is loopback, not local network, and is never gated.

## Wake configuration

- **Scheduled wake** (macOS): `pmset repeat wake …` to open predictable batch-draining
  windows, then let the machine sleep.
- **Wake-on-LAN**: enable "wake for network access" on each wakeable node; record its MAC
  in the registry so the coordinator can target it. Works **only on the same LAN** —
  a machine off the network (e.g. a laptop at another site) is opportunistic-only and
  simply doesn't drain until it's home.

## Network & security

- **LAN-only by default.** Bind the server, gateway, Redis, and model servers to the
  local network. Put at least a shared key on the gateway and job API.
- **A model server only needs to be LAN-reachable for the SYNC plane.** LiteLLM dials
  `model_server` directly, so that URL must resolve from the coordinator. The async job
  plane does not: a worker calls its own model server over loopback and the coordinator
  never touches it. A node whose model server stays bound to `127.0.0.1` therefore serves
  queued jobs perfectly while `/v1/chat/completions` for its tier cannot connect — worth
  knowing before opening an unauthenticated inference port to the network.
- **Corporate / managed machines** participate as **dumb model-server endpoints** only —
  no clusterbuck code, no client credentials on them — and may be firewalled or VPN'd.
  Respect device policy. (Formally the *attached endpoint* participation mode: a
  coordinator-side proxy worker pulls jobs on their behalf — see
  [fleet-management.md](fleet-management.md).)
