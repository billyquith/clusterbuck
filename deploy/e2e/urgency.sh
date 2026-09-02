#!/usr/bin/env bash
# Prove urgency now orders the queue, not just wake rights (ADR 34).
#
# Before this, urgency decided only *whether capacity gets created*: on a worker that was
# already awake, an urgent job waited behind whatever `waitable` work was queued ahead of
# it — while the fleet docs promised `necessary` got "head of the async queues".
#
# Two halves asserted here. First the ordering: with a real worker, an urgent job submitted
# LAST completes before a waitable backlog submitted first. Then the compatibility gate: a
# node whose heartbeat reports only the base stream must keep urgent work on the base
# stream, because a worker that predates tiering would never see the urgent one.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
PORT="${CBK_PORT:-8096}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[urg]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[urg] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
field(){ python3 -c 'import sys,json;v=json.load(sys.stdin)[sys.argv[1]];print("null" if v is None else v)' "$1"; }
REDIS_CLI=(${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli})

"${REDIS_CLI[@]}" -n 0 FLUSHDB >/dev/null

python3 "$REPO/server/tools/fake_model_server.py" --port 11449 & PIDS+=($!)
wait_for "http://127.0.0.1:11449/healthz" "fake model"

# CBK_URGENT_STREAMS=on for the ordering half: this script drives the queue directly and
# enrolls no node, so the evidence-based `auto` gate would (correctly) refuse to tier.
( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" CBK_URGENT_STREAMS=on \
  .venv/bin/python -m clusterbuck ) & PIDS+=($!)
wait_for "$URL/healthz" "server"
log "server up with tiering forced on, NO worker running"

submit(){  # $1 = urgency
  curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' \
    -d "{\"capability\":\"8b-extract\",\"messages\":[{\"role\":\"user\",\"content\":\"$1 work\"}],\"urgency\":\"$1\",\"privacy\":\"local_only\"}" \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])'
}

# A patient backlog first, so FIFO alone would serve the urgent job last.
W1=$(submit waitable); W2=$(submit waitable); W3=$(submit waitable)
URGENT=$(submit urgent)
log "queued 3 waitable, THEN 1 urgent"

BASE=$("${REDIS_CLI[@]}" -n 0 XLEN q:8b-extract | tr -d '\r')
FAST=$("${REDIS_CLI[@]}" -n 0 XLEN q:8b-extract:urgent | tr -d '\r')
[[ "$BASE" == "3" ]] || fail "expected 3 on the base stream, got $BASE"
[[ "$FAST" == "1" ]] || fail "expected 1 on the urgent stream, got $FAST"
log "routed by urgency: base=3 urgent=1  ✓"

log "NOW starting one worker…"
CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" CBK_MODEL_SERVER_URL="http://127.0.0.1:11449/v1" \
  CBK_MODEL="fake" CBK_CAPABILITIES="8b-extract" CBK_WORKER_ID="node-tiered" \
  cbk_worker_bg work & PIDS+=($!)

# Wait for the lot, then compare the executor's own `finished_at` stamps. Asserted this
# way rather than by catching the urgent job mid-flight: against the fake model server a
# job completes in milliseconds, so "is the backlog still outstanding?" is a race the
# script would lose on a fast machine. The timestamps are exact (measured around the model
# call, ADR 33), so ordering is decidable after the fact.
for j in "$URGENT" "$W1" "$W2" "$W3"; do
  for _ in $(seq 1 200); do
    ST=$(curl -fsS "$URL/jobs/$j" | field status)
    [[ "$ST" == "done" || "$ST" == "failed" ]] && break; sleep 0.05
  done
  [[ "$ST" == "done" ]] || fail "job ${j:0:16} did not run, last status '$ST'"
done
log "all four served (so nothing was starved)  ✓"

fin(){ curl -fsS "$URL/jobs/$1" | field finished_at; }
U=$(fin "$URGENT")
for j in "$W1" "$W2" "$W3"; do
  W=$(fin "$j")
  [[ "$U" < "$W" || "$U" == "$W" ]] || fail "urgent finished at $U, AFTER waitable $W"
done
log "urgent finished before all 3 waitable jobs  ✓ (submitted last, served first)"

# --- the compatibility gate ---------------------------------------------------------
# A second coordinator on `auto`, with a node enrolled whose heartbeat reports only the
# base stream. Urgent work must stay on the base stream or that worker would never see it.
"${REDIS_CLI[@]}" -n 0 FLUSHDB >/dev/null
PORT2=$((PORT+1)); URL2="http://127.0.0.1:$PORT2"
( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_DB_PATH="$WORKDIR/auto.db" CBK_PORT="$PORT2" CBK_URGENT_STREAMS=auto \
  .venv/bin/python -m clusterbuck ) & PIDS+=($!)
wait_for "$URL2/healthz" "auto-gate server"

TOKEN=$(curl -fsS -X POST "$URL2/nodes/tokens" | field join_token)
ENROLLED=$(curl -fsS -X POST "$URL2/nodes/enroll" -H 'content-type: application/json' \
  -d "{\"join_token\":\"$TOKEN\",\"hostname\":\"old-worker\",\"os\":\"linux\",\"arch\":\"arm64\",\"hw\":{\"ram_gb\":16,\"accelerator\":\"cpu\",\"disk_free_gb\":50},\"profile\":\"shared\"}")
NODE_ID=$(printf '%s' "$ENROLLED" | field node_id)
NODE_KEY=$(printf '%s' "$ENROLLED" | field node_key)

# The heartbeat an OLD worker sends: base stream only, no `:urgent`.
curl -fsS -X POST "$URL2/nodes/$NODE_ID/heartbeat" -H 'content-type: application/json' \
  -H "X-CBK-Node-Key: $NODE_KEY" \
  -d '{"mode":"active","installed":[],"loaded":[],"queues":["q:8b-extract"],"stats":{}}' >/dev/null
log "enrolled a node reporting only q:8b-extract (an older worker)"

curl -fsS -X POST "$URL2/jobs" -H 'content-type: application/json' \
  -d '{"capability":"8b-extract","messages":[{"role":"user","content":"x"}],"urgency":"urgent","privacy":"local_only"}' >/dev/null

BASE=$("${REDIS_CLI[@]}" -n 0 XLEN q:8b-extract | tr -d '\r')
FAST=$("${REDIS_CLI[@]}" -n 0 EXISTS q:8b-extract:urgent | tr -d '\r')
[[ "$BASE" == "1" ]] || fail "urgent job should have stayed on the base stream, base=$BASE"
[[ "$FAST" == "0" ]] || fail "urgent stream was written to while an old node is enrolled"
log "urgent work stayed on the base stream  ✓ (no job stranded during rollout)"

printf '\033[32m[urg] PASS — urgency orders the queue, and the rollout strands nothing\033[0m\n'
