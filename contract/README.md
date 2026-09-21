# contract/ — the wire contract source of truth

clusterbuck's server (Python) and worker (C#) are **different languages** and share
**no code** (ADR 7). They meet only at documented seams. The load-bearing seam — the one
that actually crosses languages — is the **Redis queue contract** (protocols.md §2):
the server writes a **job** onto a capability stream; the worker consumes it and writes a
**result** to the result store.

Because there is no shared type library, the wire types live here as **machine-readable
JSON Schema** (ADR 22). Both sides validate against these schemas, and a **conformance
test in each language** round-trips the shared fixtures in `examples/`, so the two type
definitions cannot silently drift. Change a schema → both sides update or their
conformance test fails.

## Files

| File | What it pins |
|---|---|
| `job.schema.json` | The enqueued job record (server → worker). Addressing already resolved to a concrete `capability`. |
| `result.schema.json` | The terminal result record (worker → result store → server). |
| `enroll-request.schema.json` / `enroll-response.schema.json` | A worker joining the fleet, and what it is told back. |
| `heartbeat-request.schema.json` / `heartbeat-response.schema.json` | The periodic worker → coordinator report, and the instructions returned on it. |
| `update-manifest.schema.json` | The signed self-update manifest a worker verifies before swapping itself. |
| `examples/*.valid.json` | Golden fixtures both conformance tests must accept. |
| `examples/*.invalid.json` | Fixtures both conformance tests must reject (guards the constraints). |

`examples/job.invalid.json` violates exactly one rule — it carries neither `messages`
nor `prompt` (the `anyOf`). `examples/result.invalid.json` violates exactly one rule —
`status: "done"` with `completion: null` (the done ⇒ completion rule). Each isolates a
single constraint so a conformance test proves that constraint actually fires.

## Scope

Seven schemas: every message that crosses the server↔worker seam. Reservations are a
server-only HTTP shape no worker touches, so they stay Pydantic models — adding them
would imply a cross-language contract that does not exist. The client submit request (`POST /jobs` body, protocols.md §1b)
is validated by the server's own Pydantic models rather than a shared schema: it is not a
cross-language seam (only the server reads it), so it stays out of `contract/` until a
non-Python client needs to pin it.

## Draft

JSON Schema **2020-12**.
