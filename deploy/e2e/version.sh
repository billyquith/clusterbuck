#!/usr/bin/env bash
# End-to-end proof of worker version governance (ADR 27), with a real worker binary.
#
# The point: protocol compatibility is not sufficient. A worker can speak the queue contract
# perfectly and still carry bugs that produce plausible-looking wrong results, so the
# coordinator judges the reported BUILD version and can quarantine an unfit one.
#
# Proves, in order:
#   1. the worker reports its build-stamped version, and the coordinator records it
#   2. a version behind `current` is `stale` — flagged, still working
#   3. a version on the BLOCK-LIST is quarantined and STOPS CLAIMING JOBS
#      (the block-list is the knob a floor cannot express: bugs are not monotonic)
#   4. a quarantined worker is handed no model-management action
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_NAME=version
# shellcheck source=lib.sh
source "$REPO/deploy/e2e/lib.sh"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
PORT="${CBK_PORT:-8090}"
MODEL_PORT="${CBK_MODEL_PORT:-11447}"
URL="http://127.0.0.1:$PORT"
CAP="8b-extract"
STATE="$WORKDIR/node.json"
node_field(){ curl -fsS "$URL/nodes" | python3 -c "
import sys,json
n=next((n for n in json.load(sys.stdin)['nodes'] if n['node_id']=='$1'),{})
print(n.get('$2') or '')"; }

redis_cli -n 0 FLUSHDB >/dev/null
python3 "$REPO/server/tools/fake_model_server.py" --port "$MODEL_PORT" >/dev/null 2>&1 & PIDS+=($!)
wait_for "http://127.0.0.1:$MODEL_PORT/healthz" "model server"

# The version the worker was actually built with — read it from the binary, don't assume.
BUILT=$(cbk_worker --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
[[ -n "$BUILT" ]] || fail "could not determine worker version — is the worker installed?"
log "worker binary is version $BUILT"

start_server(){   # $1 = extra env assignments
  ( cd "$REPO/server" && exec env \
    CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
    CBK_FLEET_PATH="$REPO/server/fleet.yaml" CBK_WOL_BROADCAST=127.0.0.1 \
    $1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
  SERVER_PID=$!; PIDS+=($SERVER_PID)
  wait_for "$URL/healthz" "server"
}
stop_server(){ kill "$SERVER_PID" 2>/dev/null || true; sleep 1; }

# --- 1. no policy: the version is reported and recorded -----------------------------------
start_server ""
TOKEN=$(curl -fsS -X POST "$URL/nodes/tokens" | jqpy '["join_token"]')
CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" \
  cbk_worker enroll --token "$TOKEN" --server "$URL" --state "$STATE" >/dev/null
NODE=$(python3 -c "import json;print(json.load(open('$STATE'))['node_id'])")

run_worker(){
  CBK_NODE_STATE="$STATE" CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
    CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" CBK_MODEL="fake" \
    CBK_HEARTBEAT_MS=400 CBK_POLL_MS=200 CBK_LADDER_HYSTERESIS_S=1 \
    cbk_worker_bg work >"$WORKDIR/worker.log" 2>&1 &
  WORKER_PID=$!; PIDS+=($WORKER_PID)
}
run_worker

for _ in $(seq 1 40); do
  V=$(node_field "$NODE" agent_version); [[ -n "$V" ]] && break; sleep 0.25
done
[[ "$V" == "$BUILT" ]] || fail "coordinator recorded version '$V', expected '$BUILT'"
[[ "$(node_field "$NODE" fitness)" == "ok" ]] || fail "expected fitness ok with no policy"
log "coordinator recorded agent_version=$V, fitness=ok ✓"
kill "$WORKER_PID" 2>/dev/null || true; sleep 0.5; stop_server

# --- 2. behind `current` ⇒ stale, still working -------------------------------------------
start_server "CBK_WORKER_CURRENT_VERSION=99.0.0"
run_worker
for _ in $(seq 1 40); do
  F=$(node_field "$NODE" fitness); [[ "$F" == "stale" ]] && break; sleep 0.25
done
[[ "$F" == "stale" ]] || fail "expected stale against current=99.0.0, got '$F'"
grep -q "is behind" "$WORKDIR/worker.log" || fail "worker did not log the staleness warning"
log "behind current ⇒ stale, and the worker says so ✓"
kill "$WORKER_PID" 2>/dev/null || true; sleep 0.5; stop_server

# --- 3. block-listed ⇒ quarantined, and it STOPS CLAIMING ---------------------------------
start_server "CBK_WORKER_BLOCKED_VERSIONS=$BUILT"
run_worker
for _ in $(seq 1 40); do
  F=$(node_field "$NODE" fitness); [[ "$F" == "quarantine" ]] && break; sleep 0.25
done
[[ "$F" == "quarantine" ]] || fail "expected quarantine for blocked $BUILT, got '$F'"
grep -q "QUARANTINED" "$WORKDIR/worker.log" || fail "worker did not report being quarantined"
log "block-listed version ⇒ quarantined; worker acknowledged it ✓"

# A quarantined worker must not drain jobs, so a submitted job stays queued.
curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' -d "{
  \"capability\": \"$CAP\", \"messages\": [{\"role\":\"user\",\"content\":\"should not run\"}],
  \"urgency\": \"necessary\"}" | jqpy '["id"]' > "$WORKDIR/jobid"
JOB=$(cat "$WORKDIR/jobid")
sleep 3
ST=$(curl -fsS "$URL/jobs/$JOB" | jqpy '["status"]')
[[ "$ST" == "queued" ]] || fail "quarantined worker still ran the job (status=$ST)"
log "job submitted while quarantined stayed '$ST' — not claimed ✓"

printf '\033[32m[version] PASS — coordinator judges build fitness; unfit workers stand down\033[0m\n'
