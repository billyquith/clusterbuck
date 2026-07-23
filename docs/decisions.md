# Design decisions (ADR-lite)

The reasoning behind the choices in [`../DESIGN.md`](../DESIGN.md), so the *why* isn't
lost. Each entry: the decision, why, and what was considered instead.

## 1. A job queue, not a serving cluster
**Decision:** model the system as a distributed task queue with LLM-aware routing.
**Why:** the defining constraint is a **heterogeneous, intermittent** fleet — machines
sleep, roam, and differ in capacity. That is a queueing problem.
**Considered:** vLLM cluster / Ray Serve / llama.cpp RPC — all assume always-on,
homogeneous nodes, which is exactly what we don't have.

## 2. Pull-based workers, not push dispatch
**Decision:** workers pull jobs from capability queues; nothing pushes to a named worker.
**Why:** no central liveness tracking, and a job is never dispatched to a node that just
slept or left the LAN. Subscription *is* the liveness signal; backpressure is natural.
**Considered:** a dispatcher that tracks live workers and pushes — fragile under
intermittency.

## 3. Queue-and-wait as the default, cloud as the impatience option
**Decision:** when no capable worker is available, patient jobs **wait in the queue**
(and may trigger a wake) rather than automatically failing over to cloud. Each job
carries a patience policy: `wait` / `wait_then_cloud` / `now`.
**Why:** most inference here is not interactive — "ready when I next look." Waiting for a
free, local, private run beats paying for cloud and sending data off-box by default.
**Considered:** always-fail-over-to-cloud (simpler, but costly and leaks data) and
always-wait (no escape hatch for interactive use).

## 4. Address by capability tier, not by machine
**Decision:** jobs request an abstract capability (`70b-reason`), not a host.
**Why:** lets a heterogeneous fleet grow/shrink without touching clients or jobs; routing
is just "which queue has a consumer."
**Considered:** naming target machines — brittle and couples clients to the fleet.

## 5. Adopt LiteLLM for the sync plane; build only the async plane
**Decision:** use LiteLLM (OpenAI-compatible) for route-now/health-check/load-balance/
cloud-fallback; build the durable queue, submit/poll API, coordinator, and worker.
**Why:** LiteLLM already solves synchronous routing well; it has no concept of holding a
job until a machine wakes. Don't reinvent the half that exists.
**Considered:** building our own OpenAI-compatible router — wasted effort.

## 6. clusterbuck is domain-agnostic
**Decision:** the fabric sees only jobs, capabilities, and results — never anything about
a client's application.
**Why:** it's reusable infrastructure with many potential tenants; coupling it to one
app's concepts would ruin that. Clients depend on clusterbuck, never the reverse.
**Considered:** baking a first tenant's needs in — rejected to keep it shareable.

## 7. Implementation language: C#/.NET (server + reference worker)
**Decision:** write clusterbuck's own moving parts in C#/.NET.
**Why:** author fluency (a lot of C# experience, no Go); it's a widely-read language in
the author's work context; and modern .NET gives cross-platform reach plus
self-contained single-file / Native AOT binaries that drop onto any node with no runtime
— satisfying "efficient + very cross-platform" where efficiency means footprint,
concurrency, and distribution, not raw FLOPs (the compute is in the model servers).
**Considered:**
- **Go** — best-in-class single-binary distribution and concurrency, but zero author
  experience; the distribution edge over .NET AOT was too small to justify learning it.
- **Python** — most familiar and fastest to prototype, and LiteLLM (adopted) is Python
  anyway; but shipping an interpreter/venv to every heterogeneous node is exactly the
  cross-platform friction .NET AOT avoids. Fine for a spike, weak for the shipped thing.
- **Rust** — maximal efficiency, single binary, but overkill for I/O-bound glue and
  slower to iterate.

## 8. Every boundary is a documented protocol
**Decision:** define the client API, the Redis queue contract, the worker↔model API, and
the wake mechanism as protocols (see [protocols.md](protocols.md)).
**Why:** keeps the system polyglot and open — the C# worker is a *reference*
implementation, not a constraint; clients and model servers stay any-language/any-OS.
**Considered:** a single-language in-process design — simpler short-term, closed
long-term.
