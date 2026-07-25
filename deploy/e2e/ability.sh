#!/usr/bin/env bash
# End-to-end proof of need-shaped ability routing (M5): a client submits {task_class,
# min_ability} — no capability — and the coordinator routes it, via the seeded ability
# matrix, to the cheapest artifact that clears the bar. No worker runs, so the job parks
# on the chosen capability's stream, which we can observe.
#
#   min_ability 4 → 8b-extract (llama3.2:3b, ability 4, cheapest)
#   min_ability 7 → 32b-reason (qwen2.5:32b, ability 7, cheaper than 70b)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8085}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[ability]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[ability] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }

docker exec cbk-redis redis-cli -n 0 FLUSHDB >/dev/null
( cd "$REPO/server" && exec env CBK_REDIS_URL="redis://localhost:6379/0" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" CBK_FLEET_PATH="$REPO/server/fleet.yaml" \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"
log "server up; ability matrix seeded"

submit_needshaped(){  # task_class, min_ability
  curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' -d "{
    \"task_class\": \"$1\", \"min_ability\": $2,
    \"messages\": [{\"role\":\"user\",\"content\":\"route me by need\"}],
    \"urgency\": \"waitable\", \"privacy\": \"local_only\"
  }" >/dev/null
}
depth(){ docker exec cbk-redis redis-cli -n 0 XLEN "q:$1" | tr -d '\r'; }

submit_needshaped summarize 4
submit_needshaped summarize 7
sleep 0.3

D8=$(depth 8b-extract); D32=$(depth 32b-reason); D70=$(depth 70b-reason)
log "queue depths → 8b-extract=$D8  32b-reason=$D32  70b-reason=$D70"
[[ "$D8" == "1" ]]  || fail "min_ability 4 did not route to 8b-extract (got depth $D8)"
[[ "$D32" == "1" ]] || fail "min_ability 7 did not route to 32b-reason (got depth $D32)"
[[ "$D70" == "0" ]] || fail "70b-reason should be untouched (got depth $D70)"

# The ability matrix is visible too.
HEAD70=$(curl -fsS "$URL/ability" | python3 -c 'import sys,json;print(json.load(sys.stdin)["headline"]["llama3.1:70b"])')
log "/ability headline llama3.1:70b = $HEAD70"
printf '\033[32m[ability] PASS — need-shaped routing picks the cheapest artifact clearing the bar\033[0m\n'
