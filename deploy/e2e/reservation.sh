#!/usr/bin/env bash
# End-to-end proof of the reservation reconciler (ADR 17, M2b) in a real server: an `asap`
# reservation is admitted (confirmed) and its lifecycle advances scheduled → warming →
# open, driven by the background coordinator tick over SQLite state.
#
# (An `asap` window sets warm_by = starts = now, so it reaches `open` within a couple of
# ticks — enough to prove the reconciler live without a minutes-long script. The full
# open→draining→closed path is covered by the unit tests.)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8082}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[rsv]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[rsv] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
jqpy(){ python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null

( cd "$REPO/server" && exec env \
  CBK_REDIS_URL="redis://localhost:6379/0" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_FLEET_PATH="$REPO/server/fleet.yaml" CBK_ESCALATION_INTERVAL_S=1 \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"
log "server up (coordinator tick every 1s)"

RESP=$(curl -fsS -X POST "$URL/reservations" -H 'content-type: application/json' -d '{
  "task_class": "summarize", "min_ability": 4,
  "window": {"start": "asap"}, "duration_min": 30, "priority": "medium"
}')
STATUS=$(printf '%s' "$RESP" | jqpy '["status"]')
RID=$(printf '%s' "$RESP" | jqpy '["id"]')
NODE=$(printf '%s' "$RESP" | jqpy '["plan"]["node"]')
[[ "$STATUS" == "confirmed" ]] || fail "admission '$STATUS', not confirmed: $RESP"
log "reservation $RID confirmed → node $NODE"

STATE=""
for _ in $(seq 1 40); do
  STATE=$(curl -fsS "$URL/reservations/$RID" | jqpy '["state"]')
  [[ "$STATE" == "open" ]] && break
  sleep 0.25
done
[[ "$STATE" == "open" ]] || fail "reservation stuck at '$STATE', never opened"
log "lifecycle: scheduled → warming → $STATE  ✓"
printf '\033[32m[rsv] PASS — reservation reconciler advances the lifecycle\033[0m\n'
