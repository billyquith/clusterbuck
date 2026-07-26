#!/usr/bin/env bash
# End-to-end proof of model discovery (M6a): the coordinator learns which models a node has
# by OBSERVING its model server, not from configuration. The models asserted below appear in
# no fleet.yaml and no worker flag — they arrive purely via the heartbeat's `installed`.
#
# Default: the fake model server advertising two synthetic models.
# USE_OLLAMA=1: real Ollama, asserting whatever is actually pulled on this machine.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8086}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
STATE="$WORKDIR/node.json"
WORKER_DLL="$REPO/worker/src/Clusterbuck.Worker/bin/Debug/net10.0/cbk.dll"
PIDS=()
log(){ printf '\033[36m[discovery]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[discovery] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }

${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null

if [[ "${USE_OLLAMA:-0}" == "1" ]]; then
  MODEL_URL="http://localhost:11434/v1"
  EXPECT="$(curl -fsS http://localhost:11434/v1/models \
    | python3 -c 'import sys,json; print(",".join(sorted(m["id"] for m in json.load(sys.stdin)["data"])))')"
  [[ -n "$EXPECT" ]] || fail "Ollama has no models pulled — nothing to discover"
  log "using real Ollama; expecting to discover: $EXPECT"
else
  python3 "$REPO/server/tools/fake_model_server.py" --port 11444 \
    --models "alpha:1b,beta:7b" --loaded "beta:7b" >/dev/null 2>&1 & PIDS+=($!)
  wait_for "http://127.0.0.1:11444/healthz" "fake model server"
  MODEL_URL="http://127.0.0.1:11444/v1"
  EXPECT="alpha:1b,beta:7b"
  log "using fake model server; expecting to discover: $EXPECT"
fi

( cd "$REPO/server" && exec env CBK_REDIS_URL="redis://localhost:6379/0" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" CBK_FLEET_PATH="$REPO/server/fleet.yaml" \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"

TOKEN=$(curl -fsS -X POST "$URL/nodes/tokens" | python3 -c 'import sys,json;print(json.load(sys.stdin)["join_token"])')
dotnet "$WORKER_DLL" enroll --token "$TOKEN" --server "$URL" --state "$STATE" >/dev/null
NODE_ID=$(python3 -c "import json;print(json.load(open('$STATE'))['node_id'])")
log "node $NODE_ID enrolled"

CBK_NODE_STATE="$STATE" CBK_REDIS_URL="redis://localhost:6379/0" \
  CBK_MODEL_SERVER_URL="$MODEL_URL" CBK_HEARTBEAT_MS=1000 \
  dotnet "$WORKER_DLL" work >/dev/null 2>&1 & PIDS+=($!)

node_field(){  # field name → comma-joined value for our node
  curl -fsS "$URL/nodes" | python3 -c "
import sys,json
n = next((n for n in json.load(sys.stdin)['nodes'] if n['node_id']=='$NODE_ID'), {})
print(','.join(sorted(n.get('$1') or [])))"
}

GOT=""
for _ in $(seq 1 60); do
  GOT=$(node_field installed)
  [[ -n "$GOT" ]] && break
  sleep 0.25
done
[[ "$GOT" == "$EXPECT" ]] || fail "discovered '$GOT', expected '$EXPECT'"
log "installed models discovered from the model server: $GOT ✓"

WARM=$(node_field loaded)
log "warm right now: ${WARM:-<none>}"
if [[ "${USE_OLLAMA:-0}" != "1" ]]; then
  [[ "$WARM" == "beta:7b" ]] || fail "expected warm 'beta:7b', got '$WARM'"
  log "loaded/installed distinction reported correctly ✓"
fi
printf '\033[32m[discovery] PASS — the fleet learns its models by observation, not config\033[0m\n'
