#!/usr/bin/env bash
# End-to-end proof of the eval harness (M7) — the loop M6 left open:
#
#   model appears on a node → discovered by observation → flagged unmeasured →
#   eval items dispatched as ORDINARY fleet jobs → a real worker drains them →
#   results scored → ability recorded → the artifact becomes ROUTABLE.
#
# The stub model server echoes its prompt, so it answers nothing. That makes it the ideal
# subject for the assertion this file now leads with: an echo bot lands at the FLOOR of
# every task class. Under the old suite it reached the ceiling — two "is this JSON?" items
# and a single "reply with the word PASS" were enough to record 10.0, which the anchor
# table calls a frontier cloud model, and which then beat a correctly configured cloud
# capability in routing because the sort prefers local.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_NAME=eval
# shellcheck source=lib.sh
source "$REPO/deploy/e2e/lib.sh"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
PORT="${CBK_PORT:-8088}"
MODEL_PORT="${CBK_MODEL_PORT:-11446}"
URL="http://127.0.0.1:$PORT"
ARTIFACT="newcomer:7b"
CAP="8b-extract"
STATE="$WORKDIR/node.json"

redis_cli -n 0 FLUSHDB >/dev/null

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
  CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_FLEET_PATH="$WORKDIR/fleet.yaml" CBK_ESCALATION_INTERVAL_S=1 \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"

TOKEN=$(curl -fsS -X POST "$URL/nodes/tokens" | jqpy '["join_token"]')
CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" \
  cbk_worker enroll --token "$TOKEN" --server "$URL" --state "$STATE" >/dev/null
NODE=$(python3 -c "import json;print(json.load(open('$STATE'))['node_id'])")
log "node $NODE enrolled"

# Worker runs for real: it discovers the artifact AND drains the eval jobs.
CBK_NODE_STATE="$STATE" CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" CBK_MODEL="$ARTIFACT" \
  CBK_HEARTBEAT_MS=500 CBK_POLL_MS=250 \
  cbk_worker_bg work >"$WORKDIR/worker.log" 2>&1 & PIDS+=($!)

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

# 4. A model that answers NOTHING scores at the floor of every class — and the score
#    carries the evidence behind it, so a reader can see what it rests on.
python3 - "$URL" "$ARTIFACT" <<'PY' || fail "echo bot was not scored at the floor"
import json, sys, urllib.request
url, artifact = sys.argv[1], sys.argv[2]
body = json.load(urllib.request.urlopen(f"{url}/ability"))
rows = [r for r in body["matrix"] if r["artifact"] == artifact]
assert rows, f"no ability rows for {artifact}"
ceiling = body["tier1_max_ability"]
for r in rows:
    assert r["provenance"] == "measured", r
    assert r["score"] <= 1.5, f"an echo bot scored {r['score']} at {r['task_class']}"
    assert r["score"] < ceiling, "echo reached the tier-1 ceiling"
    assert r["n_items"] and r["n_items"] >= 8, f"scored on {r['n_items']} item(s): {r}"
print("floor scores on", {r["task_class"]: r["n_items"] for r in rows}, "items each")
PY
log "echo bot scored at the floor, with its item counts recorded ✓"

# 5. Routable: the artifact has a real, measured score, so a need it can meet resolves to
#    its capability.
curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' -d '{
  "task_class": "summarize", "min_ability": 1,
  "messages": [{"role":"user","content":"route me to the newly measured model"}],
  "urgency": "waitable"
}' >/dev/null
sleep 0.5
BEFORE_DEPTH=$(redis_cli -n 0 XLEN "q:$CAP" | tr -d '\r')
[[ "$BEFORE_DEPTH" -gt 0 ]] || fail "need-shaped job did not reach q:$CAP"
log "measured artifact is routable via $CAP ✓"

# 6. …and a floor above what any instrument here can certify FAILS, rather than being
#    quietly served by the best thing to hand. 8-10 is the judged band; those tiers are
#    deferred, so nothing may claim it.
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$URL/jobs" \
  -H 'content-type: application/json' -d '{
  "task_class": "summarize", "min_ability": 9,
  "messages": [{"role":"user","content":"nothing here can certify this"}],
  "urgency": "waitable"
}')
[[ "$CODE" == "422" ]] || fail "min_ability 9 should fail explicitly, got HTTP $CODE"
log "min_ability 9 refused with 422 rather than silently under-served ✓"

printf '\033[32m[eval] PASS — measured via ordinary jobs; an echo bot cannot buy a high score\033[0m\n'
