#!/usr/bin/env bash
# End-to-end proof of the Performance page's load-test driver: a stream of randomized
# queries against a real coordinator + real worker + stub model server, for real, with a
# real mix of outcomes — not a rigged all-pass.
#
# The fleet here has exactly ONE local capability, seeded at ability 3-4 across task
# classes (model-evaluation.md's SEED_ABILITY for llama3.2:3b). That's enough to clear the
# extract/summarize categories' default floor (min_ability 3-4) but NOT the reason/code
# categories' floor (6-7) — so a default run genuinely produces both served and unassigned
# samples from real ability gaps, with no artificial override needed. This script also
# doubles as a template for scripted/CI-style runs against a real fleet: swap the URL for a
# real coordinator host and adjust the config.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
# Ports are per-script and must not collide: ci.sh runs these back to back, and a
# script that binds a port its predecessor has not released yet ends up talking to
# the WRONG coordinator. 8089/11447 were auth.sh's and version.sh's.
PORT="${CBK_PORT:-8091}"
MODEL_PORT="${CBK_MODEL_PORT:-11448}"
URL="http://127.0.0.1:$PORT"
ARTIFACT="llama3.2:3b"
CAP="8b-extract"
WORKDIR="$(mktemp -d)"
STATE="$WORKDIR/node.json"
PIDS=()
log(){ printf '\033[36m[perf]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[perf] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
jqpy(){ python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null

python3 "$REPO/server/tools/fake_model_server.py" --port "$MODEL_PORT" --models "$ARTIFACT" \
  >/dev/null 2>&1 & PIDS+=($!)
wait_for "http://127.0.0.1:$MODEL_PORT/healthz" "model server"

cat > "$WORKDIR/fleet.yaml" <<YAML
nodes: []
capabilities:
  $CAP:
    queue: "q:$CAP"
    model_server: "http://127.0.0.1:$MODEL_PORT/v1"
    model: "$ARTIFACT"
YAML

( cd "$REPO/server" && exec env \
  CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_FLEET_PATH="$WORKDIR/fleet.yaml" CBK_WOL_BROADCAST=127.0.0.1 \
  .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"

TOKEN=$(curl -fsS -X POST "$URL/nodes/tokens" | jqpy '["join_token"]')
CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" \
  cbk_worker enroll --token "$TOKEN" --server "$URL" --state "$STATE" >/dev/null
NODE=$(python3 -c "import json;print(json.load(open('$STATE'))['node_id'])")
log "node $NODE enrolled, serving $ARTIFACT via $CAP"

CBK_NODE_STATE="$STATE" CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" CBK_MODEL="$ARTIFACT" \
  CBK_HEARTBEAT_MS=500 CBK_POLL_MS=250 \
  cbk_worker_bg work >"$WORKDIR/worker.log" 2>&1 & PIDS+=($!)

RUN=$(curl -fsS -X POST "$URL/perf/runs" -H 'content-type: application/json' -d '{
  "label": "e2e", "n_jobs": 8, "duration_s": 20, "warmup_s": 0, "concurrency": 2
}')
RUN_ID=$(echo "$RUN" | jqpy '["id"]')
log "started run $RUN_ID"

STATUS="running"
for _ in $(seq 1 100); do
  GOT=$(curl -fsS "$URL/perf/runs/$RUN_ID")
  STATUS=$(echo "$GOT" | jqpy '["status"]')
  [[ "$STATUS" == "running" ]] || break
  sleep 0.3
done
[[ "$STATUS" == "done" ]] || fail "run ended as $STATUS, not done ($GOT)"

N_SERVED=$(echo "$GOT" | jqpy '["n_served"]')
N_UNASSIGNED=$(echo "$GOT" | jqpy '["n_unassigned"]')
log "served=$N_SERVED unassigned=$N_UNASSIGNED"
[[ "$N_SERVED" -gt 0 ]] || fail "expected at least one served sample (extract/summarize should clear ability 3-4)"
[[ "$N_UNASSIGNED" -gt 0 ]] || fail "expected at least one unassigned sample (reason/code need ability 6-7, only 3-4 is local)"

printf '\033[32m[perf] PASS — real mix of served/unassigned from real ability gaps, no worker crashes\033[0m\n'
