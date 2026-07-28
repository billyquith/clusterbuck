# `cbk` — clusterbuck worker agent (Python)

Meets the coordinator only at the documented seams — the Redis queue contract and HTTP — and
is held to the same JSON Schema in [`../contract`](../contract) by a conformance suite, so it
cannot drift from the wire (ADR 22).

## Run

```bash
uv venv && uv pip install -e ".[dev]"
uv run pytest                  # contract conformance + unit suites (no infra needed)
uv run cbk work                # start the worker loop
```

Verbs: `work | submit | status | fleet | enroll | pause | resume`. `cbk --help` lists the
flags for each.

## Packaging

The shipped artifact is a **zipapp**, `cbk.pyz`, built by [`build.py`](build.py):

```bash
python build.py                # → dist/cbk.pyz  (py3-none-any)
```

It vendors this package plus its pure-Python dependencies (`redis`, `httpx` and their
transitive pure deps), so the signed artifact is the code that runs. It is
platform-independent, which is why the coordinator names it `py3-none-any` rather than a
runtime id — see `artifact_key_for` in `server/clusterbuck/api.py`, which fails closed rather
than ever offering a worker an artifact for a different runtime.

`cryptography` is the one dependency deliberately **not** vendored. It is needed only to
verify an update signature, and a verifier has to pre-exist the update channel by
construction — it cannot be delivered *through* the thing it secures. Absent, self-update
refuses, which is the same fail-closed answer as a missing pinned public key (ADR 13). The
worker's actual job — running inference — is unaffected.

Requires Python 3.11+ on the node. One platform-independent artifact, ~2.8 MB, for every
OS and architecture (ADR 29).
