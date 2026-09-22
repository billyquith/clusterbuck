# CLAUDE.md

Guidance for Claude Code (claude.ai/code) in this repository.

## What clusterbuck is

A broker between local clients and the machines on a LAN. A job goes to one stable
endpoint; clusterbuck runs it on whichever machine is capable and free — now if something
is awake, later if it has to wait — and falls back to a cloud model only when the job
allows it and the local fleet cannot. The name is a nod to *"pass the buck"*.

Read [docs/design.md](../docs/design.md) before changing anything structural. It is the
single description of how the system works and why it is shaped that way.
[docs/protocols.md](../docs/protocols.md) is the exact wire contract.

## CRITICAL: keep it domain-agnostic

clusterbuck is **infrastructure**, Apache-2.0, and shared publicly. It must know
**nothing** about the applications that use it.

- **Never** add references to specific client applications, their domain concepts,
  personal machine names or hostnames, home-network specifics, or any employer.
- It deals only in **jobs, capabilities and results**.
- Example hardware in docs stays generic: "an always-on ~16 GB node", "a 64 GB
  workstation that sleeps". No real hostnames.
- A concrete integration example describes a generic client, never a named one.

Treat any leak of the above as a bug.

## Shape

A coordinator and a worker, meeting only at documented seams — the Redis queue contract
and HTTP — never in shared code.

- **`cbk-server` — Python**, one always-on box. FastAPI, LiteLLM (the sync plane, adopted
  wholesale — do not reimplement OpenAI routing), redis-py over Streams + consumer
  groups, SQLite via SQLModel with Alembic migrations, a plain asyncio coordinator loop.
  Serves the htmx dashboard, assets vendored, no CDN.
- **`cbk` — the worker** (`worker/`). One `py3-none-any` zipapp, ~2.9 MB, every platform.
  `redis` + `httpx` only; `cryptography` solely to verify a self-update, deliberately not
  vendored.
- **Model servers** — Ollama, llama.cpp, vLLM, LM Studio. Called over the OpenAI HTTP
  API. **Never bind to a vendor SDK.**
- **`contract/`** — JSON Schema, the source of truth, with a conformance test in each
  component so no type definition can drift from the wire.

## Build / test / run

```bash
docker run -d --name cbk-redis -p 6379:6379 redis:7-alpine

cd server && uv venv && uv pip install -e ".[dev]" && uv run pytest
CBK_API_KEY=… uv run cbk-server      # API + sync plane + dashboard on :8018

cd worker && uv venv && uv pip install -e ".[dev]" && uv run pytest
uv run ruff check src tests build.py
uv run python build.py               # → dist/cbk.pyz, the shipped artifact
```

```bash
python contract/validate.py          # schemas + fixtures
bash deploy/e2e/ci.sh                # the whole end-to-end suite
```

Worker CLI: `work | submit | status | fleet | enroll | pause | resume`.

Each `deploy/e2e/*.sh` proves one property against real processes and a real Redis, and
they share `deploy/e2e/lib.sh` — put new harness helpers there, not in a script.
`abandon.sh` is the one that interrupts: it SIGKILLs a worker mid-generation (the stub's
`--stall-s` is what holds a job open long enough) and proves the coordinator recovers the
job. Check a new recovery property against it, not only against unit tests.
`USE_OLLAMA=1` runs them against real inference instead of the stub.

Configuration is read in `server/clusterbuck/config.py`; every knob is documented at its
definition, which is the place to look rather than a list here that would drift.

## Conventions

- **Check a claim before repeating it.** Several defects here survived because a comment,
  an ADR or a docstring asserted something the code did not do. If a doc says a thing is
  wired up, grep for the caller.
- **A green suite does not prove a deletion landed.** Deleting a function that is only
  shadowed, or a check that nothing exercises, leaves tests passing. Verify the change
  fails the test you expect it to fail.
- Match the existing voice: explain *why*, not *what*, and keep the reason next to the
  code it justifies. Prefer small, focused changes.
- Document a decision where it applies, in `docs/design.md` § *Decisions worth not
  undoing* if it is one a future reader might reverse by accident.

## Repo / git

- License: Apache-2.0, © 2026 Nick Trout (see LICENSE/NOTICE).
- Repo-local git identity: `billyquith <chinbillybilbo@gmail.com>`.
- Remote: `git@github.com:billyquith/clusterbuck.git` (branch `main`).
- End commit messages with the `Co-Authored-By` trailer per the harness convention.
- The pre-merge documentation set, including all 40 numbered ADRs, is at the
  `docs-before-merge` tag: `git show docs-before-merge:docs/decisions.md`.
