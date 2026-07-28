#!/usr/bin/env bash
# Worker dispatch helper for e2e scripts.
# Sourced, not executed. Expects $REPO and a fail() from the calling script.

_PY_VENV_CBK="$REPO/worker/.venv/bin/cbk"
_PY_VENV_PYTHON="$REPO/worker/.venv/bin/python"
_PY_PYZ="$REPO/worker/dist/cbk.pyz"
WORKER_DESC="Python (worker)"

worker_assert_ready() {
  if [[ ! -x "$_PY_VENV_CBK" && ! -f "$_PY_PYZ" ]]; then
    fail "worker not installed — run: (cd $REPO/worker && uv venv && uv pip install -e '.[dev]')"
  fi
}

# The argv for the worker, printed one arg per line so callers can build on it.
_worker_argv() {
  # Prefer the venv entry point; it uses the venv interpreter, which has cryptography.
  # Falling back to the zipapp, run it under that SAME interpreter rather than the
  # shebang's `python3` — the system one may lack cryptography, which would silently
  # turn self-update off and (before the distinct error) look like a signature failure.
  if [[ -x "$_PY_VENV_CBK" ]]; then printf '%s\n' "$_PY_VENV_CBK"
  elif [[ -x "$_PY_VENV_PYTHON" ]]; then printf '%s\n' "$_PY_VENV_PYTHON" "$_PY_PYZ"
  else printf '%s\n' "$_PY_PYZ"; fi
}

# Run a worker verb synchronously: `cbk_worker enroll --token …`
#
# Readiness is asserted here rather than at source time because the calling script defines
# fail() after it sources this file — and by the time a verb actually runs, it exists.
cbk_worker() {
  worker_assert_ready
  local -a argv=()
  while IFS= read -r line; do argv+=("$line"); done < <(_worker_argv)
  "${argv[@]}" "$@"
}

# Run a worker verb as a background process whose $! is the worker itself, not a subshell —
# so the caller's cleanup trap kills the right pid: `cbk_worker_bg work & PIDS+=($!)`
cbk_worker_bg() {
  worker_assert_ready
  local -a argv=()
  while IFS= read -r line; do argv+=("$line"); done < <(_worker_argv)
  exec "${argv[@]}" "$@"
}
