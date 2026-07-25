#!/usr/bin/env bash
# End-to-end proof of the escalation engine (ADR 18, M2a): a waitable job left unserved
# past its patience bound is promoted to `necessary` by the background scan in a real
# running server. No worker is started, so the job stays queued and must escalate.
#
# WoL is pointed at loopback so the promotion's wake attempt doesn't broadcast on the LAN
# (the seed fleet.yaml's placeholder MACs match no machine anyway).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8081}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[esc]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[esc] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }

docker exec cbk-redis redis-cli -n 0 FLUSHDB >/dev/null

( cd "$REPO/server" && \
  CBK_REDIS_URL="redis://localhost:6379/0" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_FLEET_PATH="$REPO/server/fleet.yaml" CBK_ESCALATION_INTERVAL_S=1 \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"
log "server up (escalation scan every 1s, no worker running)"

# waitable with a 0-minute patience bound → due on the next scan.
JOB=$(curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' -d '{
  "capability": "8b-extract",
  "messages": [{"role": "user", "content": "escalate me"}],
  "urgency": "waitable",
  "escalate_after_min": 0
}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
log "submitted $JOB as waitable(0)"

URGENCY=""
for _ in $(seq 1 40); do
  URGENCY=$(curl -fsS "$URL/jobs/$JOB" | python3 -c 'import sys,json;print(json.load(sys.stdin)["urgency"])')
  [[ "$URGENCY" == "necessary" ]] && break
  sleep 0.25
done

[[ "$URGENCY" == "necessary" ]] || fail "job stayed '$URGENCY', never escalated"
log "urgency trajectory: waitable → $URGENCY  ✓"
printf '\033[32m[esc] PASS — escalation engine promotes stale waitable work\033[0m\n'
