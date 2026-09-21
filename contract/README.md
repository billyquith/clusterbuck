# contract/

The wire contract between the coordinator and a worker, as JSON Schema. **This is the
source of truth**: both components validate against these files, and each carries a
conformance test, so a type definition cannot silently drift from what goes over the wire.
Change a schema and both sides update, or their tests fail.

| File | What it pins |
|---|---|
| `job.schema.json` | The enqueued job (coordinator → worker). Addressing is already resolved to a concrete `capability`. |
| `result.schema.json` | The terminal result (worker → result store → coordinator). |
| `enroll-request` / `enroll-response` | A worker joining the fleet, and what it is told back. |
| `heartbeat-request` / `heartbeat-response` | The periodic worker report, and the instructions returned on it. |
| `update-manifest.schema.json` | The signed manifest a worker verifies before replacing itself. |
| `examples/*.valid.json` | Golden fixtures both conformance suites must accept. |
| `examples/*.invalid.json` | Fixtures both must reject, so the constraints are proven to fire. |

Each invalid fixture violates exactly one rule, so a passing test proves *that* constraint
works: `job.invalid.json` carries neither `messages` nor `prompt`; `result.invalid.json`
is `status: "done"` with `completion: null`.

Reservations are a coordinator-only HTTP shape that no worker touches, so they stay
Pydantic models rather than shared schemas — putting them here would imply a
cross-language contract that does not exist.

```bash
python contract/validate.py     # every schema is valid, every fixture matches
```

JSON Schema **2020-12**. The seams themselves are specified in
[../docs/protocols.md](../docs/protocols.md).
