# Deployment & platforms

How clusterbuck's two components are built and where they run, plus the node-role and
wake configuration. clusterbuck is a **split build** — a Python server and a C#/.NET
worker (see [`../DESIGN.md`](../DESIGN.md) and [implementation.md](implementation.md)).

## Server build (Python) — one box

The server (`cbk-server`: job API, LiteLLM sync front, coordinator, engines, planner)
runs on a single always-on node, so it ships as an ordinary **uv-locked Python service**
(venv + a launchd/systemd unit) — no bundling or cross-compilation needed. Python 3.13
runs on everything the coordinator might be, including **`linux-arm64` (Raspberry Pi)**,
which is the recommended coordinator host (below).

## Worker build (.NET) — every node

The worker (`cbk`) fans out across the fleet, so it publishes as a lean **Native AOT
single-file binary per runtime identifier (RID)**:

| Target | RID |
|---|---|
| macOS (Apple Silicon) | `osx-arm64` |
| macOS (Intel) | `osx-x64` |
| Linux x86-64 | `linux-x64` |
| Linux ARM64 (incl. Raspberry Pi 64-bit) | `linux-arm64` |
| Linux ARM32 | `linux-arm` |
| Windows x86-64 | `win-x64` |

Publish modes, from most portable to most self-contained:

- **Framework-dependent** — smallest artefact, but requires the .NET runtime installed
  on the node.
- **Self-contained** — bundles the runtime; no install needed. Good default for a
  heterogeneous fleet.
- **Single-file** — self-contained, collapsed to one executable. Easiest to distribute
  (`scp` and run).
- **Native AOT** — ahead-of-time compiled to a native binary: fastest start, smallest
  memory, no JIT. The worker's default — lean and instant-start on any node; some
  reflection-heavy libraries are AOT-unfriendly, so verify per dependency.

**Cross-compilation caveat:** unlike Go's single-flag cross-compile, .NET **Native AOT
generally publishes per target OS** (you build for each RID, ideally on a matching host
or via a CI matrix). Plain self-contained single-file is more forgiving. In practice a
GitHub Actions matrix (one job per RID) produces all worker binaries; fall back to
self-contained if a specific target is awkward for AOT.

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

If a Pi is ever used as a (non-inference) **worker** instead, the .NET worker also runs
on Pi: **64-bit (`linux-arm64`)** is first-class (Pi 3/4/5, Zero 2 W) incl. Native AOT;
**32-bit (`linux-arm`)** works; **ARMv6** (Pi 1, first-gen Zero) is unsupported — .NET
needs ARMv7+. (A Pi can't be a meaningful *inference* worker regardless — see below.)

**Caveat:** a Pi **cannot also be a meaningful inference worker** — too little RAM, no
usable GPU, so an 8B won't run usefully. So the clean fork is:

- *Pi = pure coordinator* (no local model), or
- *a small Mac/x86 box = coordinator + a resident small model* (Tier-0-style).

The protocol boundaries are identical either way.

## GPU / accelerator notes

- **macOS / Metal** — a Metal-backed model is capped by `iogpu.wired_limit_mb` (~67% of
  RAM by default); raise it only after freeing resident RAM. Keep large models warm with
  sensible unload timeouts so a shared machine doesn't thrash.
- Worker sizing is a property of the **model server**, not clusterbuck — clusterbuck only
  needs to know the resulting *capability* a node advertises.

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
- **Corporate / managed machines** participate as **dumb model-server endpoints** only —
  no clusterbuck code, no client credentials on them — and may be firewalled or VPN'd.
  Respect device policy. (Formally the *attached endpoint* participation mode: a
  coordinator-side proxy worker pulls jobs on their behalf — see
  [fleet-management.md](fleet-management.md).)
