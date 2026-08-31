#!/usr/bin/env bash
# End-to-end proof of self-enrollment (M4b): mint a join token, `cbk enroll` (real hardware
# probe) joins the fleet, the node appears in /nodes, `cbk work` heartbeats its presence,
# and `cbk pause` flips the mode — all across the cross-language contract.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
PORT="${CBK_PORT:-8084}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
STATE="$WORKDIR/node.json"
PIDS=()
log(){ printf '\033[36m[enroll]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[enroll] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
jqpy(){ python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null
python3 "$REPO/server/tools/fake_model_server.py" --port 11441 >/dev/null 2>&1 & PIDS+=($!)
wait_for "http://127.0.0.1:11441/healthz" "fake model server"
# exec so the backgrounded subshell *becomes* uvicorn — $! is the real server pid, so
# cleanup can actually kill it (a plain `( … & )` orphans the grandchild).
( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" CBK_FLEET_PATH="$REPO/server/fleet.yaml" \
  CBK_ESCALATION_INTERVAL_S=1 CBK_WOL_BROADCAST=127.0.0.1 \
  .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"

TOKEN=$(curl -fsS -X POST "$URL/nodes/tokens" | jqpy '["join_token"]')
log "minted join token"

cbk_worker enroll --token "$TOKEN" --server "$URL" --state "$STATE" \
  | sed 's/^/  /'
[[ -f "$STATE" ]] || fail "enroll did not write node state"
NODE_ID=$(python3 -c "import json;print(json.load(open('$STATE'))['node_id'])")

COUNT=$(curl -fsS "$URL/nodes" | jqpy '["nodes"].__len__()')
[[ "$COUNT" == "1" ]] || fail "expected 1 enrolled node, got $COUNT"
log "node $NODE_ID is in the registry"

# A reused token must be rejected.
cbk_worker enroll --token "$TOKEN" --server "$URL" --state "$WORKDIR/n2.json" >/dev/null 2>&1 \
  && fail "reused join token was accepted" || log "reused token rejected ✓"

# Start the worker (enrolled mode) and wait for a heartbeat to land.
CBK_NODE_STATE="$STATE" CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_MODEL_SERVER_URL="http://127.0.0.1:11441/v1" CBK_MODEL="fake" CBK_HEARTBEAT_MS=1000 \
  cbk_worker_bg work >/dev/null 2>&1 & PIDS+=($!)

for _ in $(seq 1 40); do
  MODE=$(curl -fsS "$URL/nodes" | python3 -c "import sys,json;print(next((n['mode'] for n in json.load(sys.stdin)['nodes'] if n['node_id']=='$NODE_ID'),''))")
  SEEN=$(curl -fsS "$URL/nodes" | python3 -c "import sys,json;print(next((n['last_heartbeat'] for n in json.load(sys.stdin)['nodes'] if n['node_id']=='$NODE_ID'),'') or '')")
  [[ "$MODE" == "active" && -n "$SEEN" ]] && break
  sleep 0.25
done
[[ "$MODE" == "active" && -n "$SEEN" ]] || fail "worker never heartbeated active (mode=$MODE seen=$SEEN)"
log "worker heartbeating: mode=active, last_heartbeat set ✓"

# Owner eviction: pause flips the mode; the next heartbeat reports it.
cbk_worker pause --state "$STATE" >/dev/null
for _ in $(seq 1 40); do
  MODE=$(curl -fsS "$URL/nodes" | python3 -c "import sys,json;print(next((n['mode'] for n in json.load(sys.stdin)['nodes'] if n['node_id']=='$NODE_ID'),''))")
  [[ "$MODE" == "paused" ]] && break; sleep 0.25
done
[[ "$MODE" == "paused" ]] || fail "pause never took effect (mode=$MODE)"
log "cbk pause → mode=paused ✓"
printf '\033[32m[enroll] PASS — enroll → registry → heartbeat → pause\033[0m\n'
