#!/usr/bin/env bash
# Worker dispatch helper for e2e scripts.
# Sourced, not executed. Expects $REPO and a fail() from the calling script.

# An isolated node identity for this run, UNLESS the calling script named one.
#
# `cbk work` falls back to ~/.clusterbuck/node.json when no state path is given, so on a
# machine that is itself a fleet node the e2e worker adopts that node's real identity --
# its id, its capabilities, its ladder -- and consumes the wrong streams entirely. The
# symptom is remote from the cause: test jobs sit `queued` forever while the worker
# cheerfully serves a tier the test never queued to. CI never hits it (no node.json
# there), so it only ever breaks for someone whose own machine is in the fleet.
#
# Defaulted here rather than in each script because the scripts that DO care already pass
# --state or set CBK_NODE_STATE, and the ones that do not should not have to know.
_E2E_STATE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/cbk-e2e-state.XXXXXX")"

_PY_VENV_CBK="$REPO/worker/.venv/bin/cbk"
_PY_VENV_PYTHON="$REPO/worker/.venv/bin/python"
_PY_PYZ="$REPO/worker/dist/cbk.pyz"
WORKER_DESC="Python (worker)"

# Is a worker artifact present? A predicate, not an assertion: a script that only wants to
# exercise a verb *if* the worker is around needs to ask without being exit'd on the answer.
worker_installed() {
  [[ -x "$_PY_VENV_CBK" || -f "$_PY_PYZ" ]]
}

worker_assert_ready() {
  if ! worker_installed; then
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
  # Only a default: an explicit --state flag or a caller-set CBK_NODE_STATE wins.
  : "${CBK_NODE_STATE:=$_E2E_STATE_DIR/node.json}"
  export CBK_NODE_STATE
  local -a argv=()
  while IFS= read -r line; do argv+=("$line"); done < <(_worker_argv)
  "${argv[@]}" "$@"
}

# Run a worker verb as a background process whose $! is the worker itself, not a subshell —
# so the caller's cleanup trap kills the right pid: `cbk_worker_bg work & PIDS+=($!)`
cbk_worker_bg() {
  worker_assert_ready
  # Only a default: an explicit --state flag or a caller-set CBK_NODE_STATE wins.
  : "${CBK_NODE_STATE:=$_E2E_STATE_DIR/node.json}"
  export CBK_NODE_STATE
  local -a argv=()
  while IFS= read -r line; do argv+=("$line"); done < <(_worker_argv)
  exec "${argv[@]}" "$@"
}
