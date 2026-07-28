#!/usr/bin/env bash
# Selects which worker implementation the e2e proofs exercise.
#
#   CBK_WORKER=dotnet   (default)  worker/dotnet — the C# reference worker
#   CBK_WORKER=python              worker/python — the platform-independent worker
#
# The point of running the SAME proofs against both is that it is the only thing which makes
# a second implementation evidence for the polyglot claim (ADR 7/22) rather than a second
# thing to keep in sync by hand. Anything that passes for one and fails for the other is a
# contract violation somewhere, by definition.
#
# Sourced, not executed. Expects $REPO and a fail() from the calling script.

CBK_WORKER="${CBK_WORKER:-dotnet}"

_DOTNET_DLL="$REPO/worker/dotnet/src/Clusterbuck.Worker/bin/Debug/net10.0/cbk.dll"
_PY_VENV_CBK="$REPO/worker/python/.venv/bin/cbk"
_PY_PYZ="$REPO/worker/python/dist/cbk.pyz"

case "$CBK_WORKER" in
  dotnet) WORKER_DESC="C#/.NET (worker/dotnet)" ;;
  python) WORKER_DESC="Python (worker/python)" ;;
  *)      echo "CBK_WORKER must be 'dotnet' or 'python', got '$CBK_WORKER'" >&2; exit 2 ;;
esac

# Assert the selected worker is actually built/installed, with the command that fixes it.
worker_assert_ready() {
  case "$CBK_WORKER" in
    dotnet)
      [[ -f "$_DOTNET_DLL" ]] \
        || fail "worker not built — run: dotnet build $REPO/worker/dotnet"
      ;;
    python)
      # Prefer the venv entry point for speed; fall back to the shipped zipapp, which is
      # also worth exercising because it is what a node actually installs.
      if [[ ! -x "$_PY_VENV_CBK" && ! -f "$_PY_PYZ" ]]; then
        fail "python worker not installed — run: (cd $REPO/worker/python && uv venv && uv pip install -e '.[dev]')"
      fi
      ;;
  esac
}

# The argv for the selected worker, printed one arg per line so callers can build on it.
_worker_argv() {
  case "$CBK_WORKER" in
    dotnet) printf '%s\n' dotnet "$_DOTNET_DLL" ;;
    python)
      if [[ -x "$_PY_VENV_CBK" ]]; then printf '%s\n' "$_PY_VENV_CBK"
      else printf '%s\n' "$_PY_PYZ"; fi
      ;;
  esac
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
