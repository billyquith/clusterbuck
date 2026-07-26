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
SCRIPTS=(
  run queue-and-wait sync escalation reservation usage
  enroll ability discovery install eval auth
)

failed=()
for s in "${SCRIPTS[@]}"; do
  printf '\n\033[1m=== %s ===\033[0m\n' "$s"
  if bash "$HERE/$s.sh"; then
    printf '\033[32m%s: PASS\033[0m\n' "$s"
  else
    printf '\033[31m%s: FAIL\033[0m\n' "$s"
    failed+=("$s")
  fi
done

printf '\n\033[1m=== summary ===\033[0m\n'
if ((${#failed[@]})); then
  printf '\033[31m%d/%d failed: %s\033[0m\n' "${#failed[@]}" "${#SCRIPTS[@]}" "${failed[*]}"
  exit 1
fi
printf '\033[32mall %d e2e scripts passed\033[0m\n' "${#SCRIPTS[@]}"
