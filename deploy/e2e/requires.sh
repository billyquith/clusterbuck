#!/usr/bin/env bash
# End-to-end proof of ADR 37: what a model CAN DO is a filter, not a score.
#
# `requires` gates candidates BEFORE ability is compared, so it can change which artifact
# a job routes to — not merely refuse one. That is the interesting property, and the one a
# unit test with a hand-built fleet can always be accused of arranging:
#
#   summarize/6, no requires          → 32b-reason  (qwen2.5:32b clears 6, cheapest)
#   summarize/6, context_tokens 100k  → 70b-reason  (qwen2.5:32b declares only 32768)
#   vision                            → 422         (no artifact in this fleet declares it)
#
# The declarations come from the catalog the coordinator seeds at startup, not from
# anything this script writes — the point is that routing reads the registry that was
# already there and had no consumer until now.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_NAME=requires
# shellcheck source=lib.sh
source "$REPO/deploy/e2e/lib.sh"
PORT="${CBK_PORT:-8098}"
URL="http://127.0.0.1:$PORT"

redis_cli -n 0 FLUSHDB >/dev/null
( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" CBK_FLEET_PATH="$REPO/server/fleet.yaml" \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"
log "server up; catalog seeded with per-artifact capability declarations"

submit(){  # requires-json  → prints HTTP status
  curl -s -o "$WORKDIR/body.json" -w '%{http_code}' -X POST "$URL/jobs" \
    -H 'content-type: application/json' -d "{
    \"task_class\": \"summarize\", \"min_ability\": 6, $1
    \"messages\": [{\"role\":\"user\",\"content\":\"filter me by capability\"}],
    \"urgency\": \"waitable\", \"privacy\": \"local_only\"
  }"
}
depth(){ redis_cli -n 0 XLEN "q:$1" | tr -d '\r'; }

# 1. Baseline: without `requires`, the cheapest artifact clearing the bar wins.
CODE=$(submit '')
[[ "$CODE" == "202" ]] || fail "baseline submit failed with HTTP $CODE"
[[ "$(depth 32b-reason)" == "1" ]] || fail "baseline did not route to 32b-reason"
log "no requires → 32b-reason (cheapest clearing ability 6) ✓"

# 2. The same job, needing a window qwen2.5:32b does not declare. It must move — not fail.
CODE=$(submit '"requires": {"context_tokens": 100000},')
[[ "$CODE" == "202" ]] || fail "context-window submit failed with HTTP $CODE ($(cat "$WORKDIR/body.json"))"
[[ "$(depth 32b-reason)" == "1" ]] || fail "32b-reason took a job it cannot hold"
[[ "$(depth 70b-reason)" == "1" ]] || fail "context_tokens did not re-route to 70b-reason"
log "context_tokens 100000 → 70b-reason; the 32768 artifact was filtered out BEFORE ability ✓"

# 3. A requirement nothing in this fleet declares is refused, naming what is missing —
#    undeclared reads as "no", so this is a 422 rather than a hopeful dispatch.
CODE=$(submit '"requires": {"vision": true},')
[[ "$CODE" == "422" ]] || fail "vision should be refused, got HTTP $CODE"
grep -q 'vision' "$WORKDIR/body.json" || fail "the 422 does not say which requirement failed"
log "vision → 422 naming the requirement ✓"

# 4. Explicit capability addressing is filtered too: the advanced form is not a bypass.
CODE=$(curl -s -o "$WORKDIR/body.json" -w '%{http_code}' -X POST "$URL/jobs" \
  -H 'content-type: application/json' -d '{
  "capability": "32b-reason", "requires": {"context_tokens": 100000},
  "messages": [{"role":"user","content":"naming a tier is not a bypass"}],
  "urgency": "waitable", "privacy": "local_only"
}')
[[ "$CODE" == "422" ]] || fail "explicit capability ignored `requires`, got HTTP $CODE"
grep -q '32b-reason' "$WORKDIR/body.json" || fail "the 422 does not name the capability"
log "explicit capability + unmet requirement → 422 ✓"

printf '\033[32m[requires] PASS — capability requirements filter before ability, on both addressing forms\033[0m\n'
