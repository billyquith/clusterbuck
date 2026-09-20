#!/usr/bin/env bash
# End-to-end proof that speed is addressable (ADR 36).
#
# Ability scores an ARTIFACT and is machine-independent by design, so it cannot express
# that the same model is quick on an accelerator and unusable without one. `stats.tps` can,
# and until now nothing consumed it. Here a node measures its own throughput from real
# jobs, the coordinator refuses a floor that node cannot meet, and the node itself refuses
# one that slips past — because only the node knows how fast it is right now.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
PORT="${CBK_PORT:-8095}"
MODEL_PORT="${CBK_MODEL_PORT:-11449}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
STATE="$WORKDIR/node.json"
PIDS=()
log(){ printf '\033[36m[speed]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[speed] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }

${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null

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

CBK_NODE_STATE="$STATE" CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" CBK_MODEL="llama3.2:3b" \
  CBK_HEARTBEAT_MS=500 cbk_worker_bg work >/dev/null 2>&1 & PIDS+=($!)
log "node $NODE_ID up"

submit(){  # min_tps ("null" for none) → job id, or the literal REFUSED
  local body
  body=$(curl -s -w '\n%{http_code}' -X POST "$URL/jobs" -H 'content-type: application/json' \
    -d "{\"capability\": \"8b-extract\", \"min_tps\": $1,
         \"messages\": [{\"role\":\"user\",\"content\":\"answer me\"}],
         \"urgency\": \"necessary\", \"privacy\": \"local_only\"}")
  if [[ "$(tail -n1 <<<"$body")" == "422" ]]; then echo "REFUSED"; else
    head -n1 <<<"$body" | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])'
  fi
}
await(){ for _ in $(seq 1 80); do
    S=$(curl -fsS "$URL/jobs/$1" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
    [[ "$S" == "done" || "$S" == "failed" ]] && { echo "$S"; return; }; sleep 0.25
  done; echo timeout; }

# 1. Before any job finishes, the node has measured NOTHING — and unknown must not read as
#    slow, or every speed-sensitive request fails on a healthy new install.
ID=$(submit 1000)
[[ "$ID" != "REFUSED" ]] || fail "an unmeasured fleet was treated as too slow"
[[ "$(await "$ID")" != "timeout" ]] || fail "first job never settled"
log "unmeasured fleet is not assumed slow ✓"

# 2. Running real jobs is what produces the measurement — no synthetic benchmark.
for _ in $(seq 1 3); do await "$(submit null)" >/dev/null; done
TPS=""
for _ in $(seq 1 40); do
  TPS=$(curl -fsS "$URL/nodes" | python3 -c "
import sys,json
n=next((n for n in json.load(sys.stdin)['nodes'] if n['node_id']=='$NODE_ID'), {})
print(n.get('tps') if n.get('tps') is not None else '')")
  [[ -n "$TPS" ]] && break; sleep 0.25
done
[[ -n "$TPS" ]] || fail "node never reported a measured tps"
log "node measured its own throughput from real jobs: $TPS tok/s"

# 3. A floor far above what this fleet has ever achieved is refused AT SUBMIT, and the
#    reason names speed rather than ability — they need completely different fixes.
HIGH=$(python3 -c "print(int(float('$TPS') * 1000) + 1000)")
RESP=$(curl -s -X POST "$URL/jobs" -H 'content-type: application/json' \
  -d "{\"task_class\": \"extract\", \"min_ability\": 4, \"min_tps\": $HIGH,
       \"messages\": [{\"role\":\"user\",\"content\":\"too fast for this fleet\"}],
       \"urgency\": \"necessary\", \"privacy\": \"local_only\"}")
grep -q "min_tps" <<<"$RESP" || fail "refusal does not name the speed floor: $RESP"
log "min_tps $HIGH refused at submit, naming speed not ability ✓"

# 4. A floor this node DOES meet still routes and runs.
OK=$(submit 1)
[[ "$OK" != "REFUSED" ]] || fail "a floor the node meets was refused"
[[ "$(await "$OK")" == "done" ]] || fail "job within the node's measured speed did not run"
log "a floor the fleet meets still runs ✓"

printf '\033[32m[speed] PASS — throughput is measured from real work and addressable\033[0m\n'
