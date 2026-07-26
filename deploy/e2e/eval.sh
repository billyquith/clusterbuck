#!/usr/bin/env bash
# End-to-end proof of the eval harness (M7) — the loop M6 left open:
#
#   model appears on a node → discovered by observation → flagged unmeasured →
#   eval items dispatched as ORDINARY fleet jobs → a real worker drains them →
#   results scored → ability recorded → the artifact becomes ROUTABLE.
#
# The stub model server echoes its prompt, which genuinely fails the JSON items and passes
# the keyword items — so this asserts real discrimination (extract 1.0, summarize 10.0),
# not a rigged all-pass.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8088}"
MODEL_PORT="${CBK_MODEL_PORT:-11446}"
URL="http://127.0.0.1:$PORT"
ARTIFACT="newcomer:7b"
CAP="8b-extract"
WORKDIR="$(mktemp -d)"
STATE="$WORKDIR/node.json"
WORKER_DLL="$REPO/worker/src/Clusterbuck.Worker/bin/Debug/net10.0/cbk.dll"
PIDS=()
log(){ printf '\033[36m[eval]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[eval] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
jqpy(){ python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null

# A model server advertising an artifact nobody has ever scored.
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
    price_in_per_1k: 0.0002
    price_out_per_1k: 0.0006
YAML

( cd "$REPO/server" && exec env \
  CBK_REDIS_URL="redis://localhost:6379/0" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_FLEET_PATH="$WORKDIR/fleet.yaml" CBK_ESCALATION_INTERVAL_S=1 \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"

TOKEN=$(curl -fsS -X POST "$URL/nodes/tokens" | jqpy '["join_token"]')
CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" \
  dotnet "$WORKER_DLL" enroll --token "$TOKEN" --server "$URL" --state "$STATE" >/dev/null
NODE=$(python3 -c "import json;print(json.load(open('$STATE'))['node_id'])")
log "node $NODE enrolled"

# Worker runs for real: it discovers the artifact AND drains the eval jobs.
CBK_NODE_STATE="$STATE" CBK_REDIS_URL="redis://localhost:6379/0" \
  CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" CBK_MODEL="$ARTIFACT" \
  CBK_HEARTBEAT_MS=500 CBK_POLL_MS=250 \
  dotnet "$WORKER_DLL" work >"$WORKDIR/worker.log" 2>&1 & PIDS+=($!)

# 1. Discovery: the artifact reaches the registry with nobody having configured it.
for _ in $(seq 1 60); do
  INST=$(curl -fsS "$URL/nodes" | python3 -c "import sys,json;print(','.join(next((n['installed'] for n in json.load(sys.stdin)['nodes'] if n['node_id']=='$NODE'),[])))")
  [[ "$INST" == *"$ARTIFACT"* ]] && break; sleep 0.25
done
[[ "$INST" == *"$ARTIFACT"* ]] || fail "artifact never discovered (installed=$INST)"
log "discovered by observation: $INST"

# 2. It is recognised as unmeasured — no score is inherited from anyone.
NEEDS=$(curl -fsS "$URL/eval" | python3 -c "import sys,json;print(','.join(a['artifact'] for a in json.load(sys.stdin)['needs_eval']))")
[[ "$NEEDS" == *"$ARTIFACT"* ]] || fail "artifact not flagged unmeasured (needs_eval=$NEEDS)"
log "flagged unmeasured: $NEEDS"

# 3. Dispatch as ordinary jobs; the running worker drains them; collect scores.
DISPATCHED=$(curl -fsS -X POST "$URL/eval/run" | jqpy '["dispatched"]')
[[ "$DISPATCHED" -gt 0 ]] || fail "no eval jobs dispatched"
log "dispatched $DISPATCHED eval items as ordinary waitable jobs"

for _ in $(seq 1 80); do
  curl -fsS -X POST "$URL/eval/run" >/dev/null
  SCORE=$(curl -fsS "$URL/ability" | python3 -c "
import sys,json
m=json.load(sys.stdin)['matrix']
print(next((r['score'] for r in m if r['artifact']=='$ARTIFACT' and r['task_class']=='summarize'), ''))")
  [[ -n "$SCORE" ]] && break; sleep 0.25
done
[[ -n "$SCORE" ]] || fail "ability never recorded for $ARTIFACT"

# 4. The measurement discriminates: JSON items fail, keyword items pass.
EXTRACT=$(curl -fsS "$URL/ability" | python3 -c "
import sys,json
m=json.load(sys.stdin)['matrix']
print(next((r['score'] for r in m if r['artifact']=='$ARTIFACT' and r['task_class']=='extract'), ''))")
log "measured $ARTIFACT → summarize=$SCORE  extract=$EXTRACT"
python3 -c "import sys; sys.exit(0 if float('$SCORE') >= 9 else 1)" \
  || fail "summarize should score high (echo contains the keyword), got $SCORE"
python3 -c "import sys; sys.exit(0 if float('$EXTRACT') <= 2 else 1)" \
  || fail "extract should score low (echo is not valid JSON), got $EXTRACT"

# 5. Routable: a need only the freshly-measured artifact can meet now resolves to it.
curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' -d '{
  "task_class": "summarize", "min_ability": 9,
  "messages": [{"role":"user","content":"route me to the newly measured model"}],
  "urgency": "waitable"
}' >/dev/null
sleep 0.5
BEFORE_DEPTH=$(${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 XLEN "q:$CAP" | tr -d '\r')
[[ "$BEFORE_DEPTH" -gt 0 ]] || fail "need-shaped job did not reach q:$CAP"
log "min_ability 9 now routes to $CAP (backed by $ARTIFACT) ✓"

printf '\033[32m[eval] PASS — unmeasured model measured via ordinary jobs, then routable\033[0m\n'
