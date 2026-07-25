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
| `examples/*.valid.json` | Golden fixtures both conformance tests must accept. |
| `examples/*.invalid.json` | Fixtures both conformance tests must reject (guards the constraints). |

`examples/job.invalid.json` violates exactly one rule — it carries neither `messages`
nor `prompt` (the `anyOf`). `examples/result.invalid.json` violates exactly one rule —
`status: "done"` with `completion: null` (the done ⇒ completion rule). Each isolates a
single constraint so a conformance test proves that constraint actually fires.

## Scope (M0)

Only `job` and `result` — the two records the end-to-end loop needs. The other protocol
messages (enrollment, heartbeat, reservation, attention, update-manifest) join `contract/`
as their milestones land. The client submit request (`POST /jobs` body, protocols.md §1b)
is validated by the server's own Pydantic models rather than a shared schema: it is not a
cross-language seam (only the server reads it), so it stays out of `contract/` until a
non-Python client needs to pin it.

## Draft

JSON Schema **2020-12**.
