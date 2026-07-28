# `cbk` — clusterbuck worker agent (Python)

One of **two interchangeable workers**. The C# implementation in [`../dotnet`](../dotnet) is
the *reference* worker, not a constraint (ADR 7): both meet the coordinator only at the
documented seams — the Redis queue contract and HTTP — and both are held to the same JSON
Schema in [`../../contract`](../../contract) by their own conformance suites, so neither can
drift from the wire (ADR 22).

An operator should not have to know which one is installed on a node. Same verbs, same flags,
same `CBK_*` environment variables, same defaults.

## Why a second implementation

Not novelty — packaging. The .NET worker ships as a self-contained single-file binary, which
means **six per-platform artifacts** (win/osx/linux × x64/arm64) at 72–81 MB each, and Native
AOT does not link on macOS. Worse, the "self-contained" macOS binary turned out to hard-link
Homebrew's brotli, so `dyld` refuses to launch it on a Mac without Homebrew — measured, see
ADR 29.

This worker is **one artifact for every platform** and a fraction of the size. That collapses
the release matrix and makes a self-update a small download instead of a 73 MB one.

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

Requires Python 3.11+ on the node. That is the honest trade for dropping 73 MB and five
artifacts: a runtime the machine almost certainly already has, versus a bundled one that
turned out not to be as self-contained as advertised.
