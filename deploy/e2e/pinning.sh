#!/usr/bin/env bash
# End-to-end proof that the model whose ability cleared the bar is the model that runs.
#
# The failure this closes looked healthy from every angle. A capability is only a queue
# name: routing scores a tier by looking up fleet.yaml's `model:` in the ability matrix,
# while the worker answers with its own CBK_MODEL, for EVERY tier it consumes. Nothing
# reconciled the two. A node advertising both an 8B and a 32B tier while running one small
# model answered the 32B tier's jobs with that small model — enrolment fine, heartbeat
# green, HTTP 200, reported as having met a floor it never met.
#
# Here one node serves both tiers and holds only the 8B tier's model. The 8B job must
# succeed; the 32B job must FAIL with a reason naming the pin, because a visible failure is
# worth more than a plausible answer nobody can audit.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_NAME=pin
# shellcheck source=lib.sh
source "$REPO/deploy/e2e/lib.sh"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
PORT="${CBK_PORT:-8094}"
MODEL_PORT="${CBK_MODEL_PORT:-11448}"
URL="http://127.0.0.1:$PORT"
STATE="$WORKDIR/node.json"

redis_cli -n 0 FLUSHDB >/dev/null

# The node's model server holds ONLY the 8B tier's model — not the 32B tier's.
python3 "$REPO/server/tools/fake_model_server.py" --port "$MODEL_PORT" \
  --models "llama3.2:3b" >/dev/null 2>&1 & PIDS+=($!)
wait_for "http://127.0.0.1:$MODEL_PORT/healthz" "fake model server"

( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" CBK_FLEET_PATH="$REPO/server/fleet.yaml" \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"

TOKEN=$(curl -fsS -X POST "$URL/nodes/tokens" | python3 -c 'import sys,json;print(json.load(sys.stdin)["join_token"])')
cbk_worker enroll --token "$TOKEN" --server "$URL" --state "$STATE" >/dev/null
NODE_ID=$(python3 -c "import json;print(json.load(open('$STATE'))['node_id'])")

# The enrolment proposal only serves the FIRST tier while the owner is `active`, so put the
# node in `away` — the mode in which it serves the whole ladder, which is exactly when an
# over-advertised node does its damage.
python3 - "$STATE" <<'PY'
import json, sys
p = sys.argv[1]
s = json.load(open(p))
s["mode"] = "away"
json.dump(s, open(p, "w"), indent=2)
PY

# One worker, one model, TWO tiers — the ordinary over-advertised node.
CBK_NODE_STATE="$STATE" CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" CBK_MODEL="llama3.2:3b" \
  CBK_LADDER_HYSTERESIS_S=0 CBK_HEARTBEAT_MS=500 \
  cbk_worker_bg work >/dev/null 2>&1 & PIDS+=($!)
log "node $NODE_ID serving [8b-extract, 32b-reason] with only llama3.2:3b installed"

# 1. The coordinator NAMES the mismatch rather than waiting for it to produce bad answers.
WARN=""
for _ in $(seq 1 60); do
  WARN=$(curl -fsS "$URL/nodes" | python3 -c "
import sys,json
n=next((n for n in json.load(sys.stdin)['nodes'] if n['node_id']=='$NODE_ID'), {})
print(' | '.join(n.get('capability_warnings') or []))")
  [[ -n "$WARN" ]] && break; sleep 0.25
done
[[ "$WARN" == *"32b-reason"* ]] || fail "coordinator never flagged the unservable tier ($WARN)"
[[ "$WARN" != *"8b-extract"* ]] || fail "flagged the tier the node CAN serve"
log "coordinator flagged 32b-reason as unservable here ✓"

submit(){  # capability → job id
  curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' -d "{
    \"capability\": \"$1\",
    \"messages\": [{\"role\":\"user\",\"content\":\"answer me\"}],
    \"urgency\": \"necessary\", \"privacy\": \"local_only\"
  }" | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])'
}
await(){  # job id → status
  for _ in $(seq 1 80); do
    S=$(curl -fsS "$URL/jobs/$1" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
    [[ "$S" == "done" || "$S" == "failed" ]] && { echo "$S"; return; }
    sleep 0.25
  done
  echo timeout
}

# 2. The tier this node really can serve is answered normally.
OK_ID=$(submit 8b-extract)
OK_STATUS=$(await "$OK_ID")
[[ "$OK_STATUS" == "done" ]] || fail "8b-extract job should have succeeded, got $OK_STATUS"
log "8b-extract answered normally ✓"

# 3. The tier it cannot serve FAILS, with a reason — rather than returning a confident
#    answer from a model that never cleared that tier's bar.
BAD_ID=$(submit 32b-reason)
BAD_STATUS=$(await "$BAD_ID")
[[ "$BAD_STATUS" == "failed" ]] \
  || fail "32b-reason job should have failed rather than been silently under-served (got $BAD_STATUS)"
ERR=$(curl -fsS "$URL/jobs/$BAD_ID" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("error") or "")')
[[ "$ERR" == *"qwen2.5:32b"* ]] || fail "failure does not name the pinned artifact: $ERR"
log "32b-reason refused: ${ERR:0:90}…"

printf '\033[32m[pin] PASS — the model that cleared the bar is the model that runs\033[0m\n'
