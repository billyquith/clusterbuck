#!/usr/bin/env bash
# Prove the async plane's defining behaviour: a job submitted with NO consumer parks in
# the queue as `queued`, then drains once a worker appears (DESIGN.md, ADR 3).
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8078}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[qw]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[qw] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }

# fresh queue state
docker exec cbk-redis redis-cli -n 0 FLUSHDB >/dev/null

python3 "$REPO/server/tools/fake_model_server.py" --port 11436 & PIDS+=($!)
wait_for "http://127.0.0.1:11436/healthz" "fake model"

( cd "$REPO/server" && exec env CBK_REDIS_URL="redis://localhost:6379/0" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" .venv/bin/python -m clusterbuck ) & PIDS+=($!)
wait_for "$URL/healthz" "server"
log "server up, NO worker running"

JOB=$(curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' \
  -d '{"capability":"8b-extract","messages":[{"role":"user","content":"wait for me"}],"urgency":"waitable","privacy":"local_only"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
log "submitted $JOB with no consumer"

sleep 1
ST=$(curl -fsS "$URL/jobs/$JOB" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
[[ "$ST" == "queued" ]] || fail "expected queued with no worker, got '$ST'"
log "parked as: $ST  ✓ (waited in queue, not lost)"

log "NOW starting the worker…"
CBK_REDIS_URL="redis://localhost:6379/0" CBK_MODEL_SERVER_URL="http://127.0.0.1:11436/v1" \
  CBK_MODEL="fake" CBK_CAPABILITIES="8b-extract" CBK_WORKER_ID="node-late" \
  dotnet "$REPO/worker/src/Clusterbuck.Worker/bin/Debug/net10.0/cbk.dll" work & PIDS+=($!)

for _ in $(seq 1 100); do
  ST=$(curl -fsS "$URL/jobs/$JOB" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  [[ "$ST" == "done" || "$ST" == "failed" ]] && break; sleep 0.2
done
[[ "$ST" == "done" ]] || fail "job did not drain after worker started, got '$ST'"
log "drained by late worker: $ST  ✓"
printf '\033[32m[qw] PASS — queue-and-wait: submit → queued → worker joins → done\033[0m\n'
