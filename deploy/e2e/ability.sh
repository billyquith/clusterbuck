#!/usr/bin/env bash
# End-to-end proof of need-shaped ability routing (M5): a client submits {task_class,
# min_ability} — no capability — and the coordinator routes it, via the seeded ability
# matrix, to the cheapest artifact that clears the bar. No worker runs, so the job parks
# on the chosen capability's stream, which we can observe.
#
#   min_ability 4 → 8b-extract (llama3.2:3b, ability 4, cheapest)
#   min_ability 6 → 32b-reason (qwen2.5:32b, ability 6.5, cheaper than 70b)
#   min_ability 7 → 70b-reason (only the 70B seed sits ON the 7 anchor)
#   min_ability 9 → 422 (the judged band; no instrument here can certify it)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_NAME=ability
# shellcheck source=lib.sh
source "$REPO/deploy/e2e/lib.sh"
PORT="${CBK_PORT:-8085}"
URL="http://127.0.0.1:$PORT"

redis_cli -n 0 FLUSHDB >/dev/null
( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
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
depth(){ redis_cli -n 0 XLEN "q:$1" | tr -d '\r'; }

submit_needshaped summarize 4
submit_needshaped summarize 6
submit_needshaped summarize 7
sleep 0.3

D8=$(depth 8b-extract); D32=$(depth 32b-reason); D70=$(depth 70b-reason)
log "queue depths → 8b-extract=$D8  32b-reason=$D32  70b-reason=$D70"
[[ "$D8" == "1" ]]  || fail "min_ability 4 did not route to 8b-extract (got depth $D8)"
[[ "$D32" == "1" ]] || fail "min_ability 6 did not route to 32b-reason (got depth $D32)"
[[ "$D70" == "1" ]] || fail "min_ability 7 did not route to 70b-reason (got depth $D70)"

# A floor in the judged band (8-10) is refused outright. The judged tiers are deferred, so
# no artifact here has an instrument that could certify one — and serving the job on the
# best thing to hand would be exactly the silent under-serve need-shaped addressing exists
# to prevent.
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$URL/jobs" \
  -H 'content-type: application/json' -d '{
  "task_class": "summarize", "min_ability": 9,
  "messages": [{"role":"user","content":"nothing here can certify this"}],
  "urgency": "waitable", "privacy": "local_only"
}')
[[ "$CODE" == "422" ]] || fail "min_ability 9 should be refused, got HTTP $CODE"
log "min_ability 9 refused with 422 — explicit, not under-served ✓"

# The ability matrix is visible too.
HEAD70=$(curl -fsS "$URL/ability" | python3 -c 'import sys,json;print(json.load(sys.stdin)["headline"]["llama3.1:70b"])')
log "/ability headline llama3.1:70b = $HEAD70"
printf '\033[32m[ability] PASS — need-shaped routing picks the cheapest artifact clearing the bar\033[0m\n'
