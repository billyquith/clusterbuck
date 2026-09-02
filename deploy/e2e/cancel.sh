#!/usr/bin/env bash
# Prove DELETE /jobs/{id} actually withdraws unclaimed work (protocols.md §1b).
#
# The gap: a client that gave up left its job on the fleet, which ran it to completion so
# that nobody collected the result — real wasted capacity on a small fleet. Asserted here:
# the queued entry is genuinely removed, the job reports `cancelled`, it is metered so the
# coordinator stops re-scanning it, and a worker that joins afterwards finds nothing to do.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
PORT="${CBK_PORT:-8095}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[cancel]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[cancel] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
field(){ python3 -c 'import sys,json;v=json.load(sys.stdin)[sys.argv[1]];print("null" if v is None else v)' "$1"; }
REDIS_CLI=(${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli})

"${REDIS_CLI[@]}" -n 0 FLUSHDB >/dev/null

python3 "$REPO/server/tools/fake_model_server.py" --port 11448 & PIDS+=($!)
wait_for "http://127.0.0.1:11448/healthz" "fake model"

( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" .venv/bin/python -m clusterbuck ) & PIDS+=($!)
wait_for "$URL/healthz" "server"
log "server up, NO worker running"

submit(){
  curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' \
    -d '{"capability":"8b-extract","messages":[{"role":"user","content":"abandon me"}],"urgency":"waitable","privacy":"local_only"}' \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])'
}

KEEP=$(submit); DROP=$(submit)
[[ "$("${REDIS_CLI[@]}" -n 0 XLEN q:8b-extract | tr -d '\r')" == "2" ]] || fail "expected 2 queued"
log "submitted 2 jobs with no consumer"

VERDICT=$(curl -fsS -X DELETE "$URL/jobs/$DROP" | field status)
[[ "$VERDICT" == "cancelled" ]] || fail "unclaimed job should be 'cancelled', got '$VERDICT'"
log "DELETE → cancelled  ✓ (nobody held it, so this is provable)"

DEPTH=$("${REDIS_CLI[@]}" -n 0 XLEN q:8b-extract | tr -d '\r')
[[ "$DEPTH" == "1" ]] || fail "the entry should be gone from the stream, XLEN=$DEPTH"
log "entry removed from the queue  ✓ (this is the reclaimed capacity)"

[[ "$(curl -fsS "$URL/jobs/$DROP" | field status)" == "cancelled" ]] || fail "status not durable"
[[ "$(curl -fsS -X DELETE "$URL/jobs/$DROP" | field status)" == "cancelled" ]] || fail "not idempotent"
log "durable and idempotent  ✓"

CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "$URL/jobs/job_nope")
[[ "$CODE" == "404" ]] || fail "unknown job should be 404, got $CODE"

# The cancelled job must be metered, or `jobs_awaiting_usage` re-selects it on every
# coordinator tick forever. No worker has run yet, so the cancelled job is the ONLY job
# that can have produced a usage row — hence exactly 1.
METERED=$(curl -fsS "$URL/usage" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["totals"]["jobs"])')
[[ "$METERED" == "1" ]] || fail "expected the cancelled job to be metered once, got $METERED"
log "metered once, cost 0  ✓ (this is what stops the endless re-scan)"

# A worker joining afterwards must find only the survivor.
log "NOW starting a worker…"
CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" CBK_MODEL_SERVER_URL="http://127.0.0.1:11448/v1" \
  CBK_MODEL="fake" CBK_CAPABILITIES="8b-extract" CBK_WORKER_ID="node-after-cancel" \
  cbk_worker_bg work & PIDS+=($!)

for _ in $(seq 1 100); do
  ST=$(curl -fsS "$URL/jobs/$KEEP" | field status)
  [[ "$ST" == "done" || "$ST" == "failed" ]] && break; sleep 0.2
done
[[ "$ST" == "done" ]] || fail "the surviving job should still run, got '$ST'"
[[ "$(curl -fsS "$URL/jobs/$DROP" | field status)" == "cancelled" ]] || fail "cancelled job ran anyway"
log "survivor ran; cancelled job never did  ✓"

printf '\033[32m[cancel] PASS — abandoned work is withdrawn, not silently executed\033[0m\n'
