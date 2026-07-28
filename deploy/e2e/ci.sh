#!/usr/bin/env bash
# Run the whole end-to-end suite, in order, failing on the first broken script.
#
# Used by CI and handy locally. Every script picks a distinct port and cleans up its own
# processes, so they are safe to run back to back; `CBK_REDIS_CLI` lets CI point them at a
# service container instead of `docker exec`.
#
# CBK_WORKER selects which worker implementation is exercised (see worker.sh):
#   CBK_WORKER=dotnet   the C# reference worker (default)
#   CBK_WORKER=python   the platform-independent worker
#   CBK_WORKER=both     run the shared proofs against BOTH, then the per-flavour ones
#
# `both` is the mode that matters. These scripts assert coordinator-observable behaviour, so
# anything that passes for one implementation and fails for the other is a contract violation
# by definition — that is what makes a second worker evidence rather than a liability.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${CBK_WORKER:-dotnet}"

# Order matters only in that the cheap async proofs come first, so a fundamental break is
# reported before the slower fleet-management scripts run.
SHARED=(
  run queue-and-wait sync escalation reservation usage
  enroll ability discovery install eval auth version
)
# Self-update is per-flavour: the artifact and the swap mechanism differ (a single-file
# binary vs a zipapp), so each implementation proves its own.
declare -A SELFUPDATE=([dotnet]=selfupdate [python]=selfupdate-py)

failed=()

run_one() {   # flavour, script
  printf '\n\033[1m=== %s (%s) ===\033[0m\n' "$2" "$1"
  if CBK_WORKER="$1" bash "$HERE/$2.sh"; then
    printf '\033[32m%s (%s): PASS\033[0m\n' "$2" "$1"
  else
    printf '\033[31m%s (%s): FAIL\033[0m\n' "$2" "$1"
    failed+=("$2($1)")
  fi
}

case "$MODE" in
  both)     FLAVOURS=(dotnet python) ;;
  dotnet)   FLAVOURS=(dotnet) ;;
  python)   FLAVOURS=(python) ;;
  *) echo "CBK_WORKER must be dotnet | python | both, got '$MODE'" >&2; exit 2 ;;
esac

total=0
for flavour in "${FLAVOURS[@]}"; do
  for s in "${SHARED[@]}"; do run_one "$flavour" "$s"; total=$((total+1)); done
  su="${SELFUPDATE[$flavour]}"
  if [[ -f "$HERE/$su.sh" ]]; then run_one "$flavour" "$su"; total=$((total+1)); fi
done

printf '\n\033[1m=== summary ===\033[0m\n'
if ((${#failed[@]})); then
  printf '\033[31m%d/%d failed: %s\033[0m\n' "${#failed[@]}" "$total" "${failed[*]}"
  exit 1
fi
printf '\033[32mall %d e2e runs passed (%s)\033[0m\n' "$total" "${FLAVOURS[*]}"
