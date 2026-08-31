#!/usr/bin/env bash
# Run the whole end-to-end suite, in order, failing on the first broken script.
#
# Used by CI and handy locally. Every script picks a distinct port and cleans up its own
# processes, so they are safe to run back to back; `CBK_REDIS_CLI` lets CI point them at a
# service container instead of `docker exec`.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Order matters only in that the cheap async proofs come first, so a fundamental break is
# reported before the slower fleet-management scripts run.
SHARED=(
  run queue-and-wait sync escalation reservation usage
  enroll ability discovery install eval perf auth version
)

failed=()

run_one() {
  printf '\n\033[1m=== %s ===\033[0m\n' "$1"
  if bash "$HERE/$1.sh"; then
    printf '\033[32m%s: PASS\033[0m\n' "$1"
  else
    printf '\033[31m%s: FAIL\033[0m\n' "$1"
    failed+=("$1")
  fi
}

for s in "${SHARED[@]}"; do run_one "$s"; done
run_one selfupdate-py

total=$(( ${#SHARED[@]} + 1 ))
printf '\n\033[1m=== summary ===\033[0m\n'
if ((${#failed[@]})); then
  printf '\033[31m%d/%d failed: %s\033[0m\n' "${#failed[@]}" "$total" "${failed[*]}"
  exit 1
fi
printf '\033[32mall %d e2e runs passed\033[0m\n' "$total"
