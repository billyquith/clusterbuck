#!/usr/bin/env bash
# Prove a polling client can render an HONEST wait state (protocols.md §1b).
#
# This reproduces the situation a client reported as unreadable: jobs queued with nothing
# serving them. In that state the two numbers that existed — `depth` (XLEN, retained
# history) and `pending` (already claimed) — both report a healthy queue, and the job body
# carried no timestamps, no position, and a `worker` that stays null until the job is over.
#
# Asserted here, in order: real backlog and per-job position while nothing is running; the
# give-up fields (null when the client set no bounds — the honest answer); then a worker
# joins, the backlog actually drains, and the finished job carries a claiming node,
# `finished_at`, and no stale position.
#
# The mid-flight `running`/`started_at` window is NOT asserted here on purpose: against
# the fake model server a job completes in milliseconds, so catching it mid-run from a
# shell script is a race. That case is covered deterministically in
# server/tests/test_observe.py and test_api.py, which drive the observe tick directly.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
PORT="${CBK_PORT:-8097}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[wait]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[wait] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
field(){ python3 -c 'import sys,json;v=json.load(sys.stdin)[sys.argv[1]];print("null" if v is None else v)' "$1"; }

${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null

python3 "$REPO/server/tools/fake_model_server.py" --port 11447 & PIDS+=($!)
wait_for "http://127.0.0.1:11447/healthz" "fake model"

( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" .venv/bin/python -m clusterbuck ) & PIDS+=($!)
wait_for "$URL/healthz" "server"
log "server up, NO worker running"

submit(){
  curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' \
    -d "{\"capability\":\"8b-extract\",\"messages\":[{\"role\":\"user\",\"content\":\"$1\"}],\"urgency\":\"waitable\",\"privacy\":\"local_only\"}" \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])'
}

A=$(submit "first"); B=$(submit "second"); C=$(submit "third")
log "submitted 3 jobs with no consumer"

# --- the numbers a client needs, in the state where the old ones lie -----------------
for pair in "$A:0" "$B:1" "$C:2"; do
  job="${pair%%:*}"; want="${pair##*:}"
  got=$(curl -fsS "$URL/jobs/$job" | field queue_position)
  [[ "$got" == "$want" ]] || fail "queue_position for ${job:0:12}: want $want, got '$got'"
done
log "queue_position: 0 / 1 / 2  ✓ (each job knows its own place in line)"

QSTATS=$(curl -fsS "$URL/queues" | python3 -c '
import sys, json
q = next(x for x in json.load(sys.stdin)["queues"] if x["capability"] == "8b-extract")
print(q["backlog"], q["pending"], q["consumers"])')
read -r BACKLOG PENDING CONSUMERS <<<"$QSTATS"
[[ "$BACKLOG" == "3" ]] || fail "backlog: want 3, got '$BACKLOG'"
[[ "$PENDING" == "0" ]] || fail "pending should be 0 (nothing claimed), got '$PENDING'"
[[ "$CONSUMERS" == "0" ]] || fail "consumers should be 0 (no worker), got '$CONSUMERS'"
log "backlog=3 pending=0 consumers=0  ✓ (the state where depth/pending both read healthy)"

# --- when, if ever, clusterbuck gives up --------------------------------------------
BODY=$(curl -fsS "$URL/jobs/$A")
for f in deadline escalates_at expires_at started_at finished_at; do
  got=$(printf '%s' "$BODY" | field "$f")
  [[ "$got" == "null" ]] || fail "$f should be null on an unbounded queued job, got '$got'"
done
[[ -n "$(printf '%s' "$BODY" | field created_at)" ]] || fail "created_at missing"
log "expires_at=null  ✓ (honest: nothing will ever give up on this job)"

# A bounded job reports its bound instead of staying silent about it.
BOUNDED=$(curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' \
  -d '{"capability":"8b-extract","messages":[{"role":"user","content":"bounded"}],"urgency":"waitable","privacy":"local_only","deadline":"2030-01-01T00:00:00Z","escalate_after_min":10}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
BB=$(curl -fsS "$URL/jobs/$BOUNDED")
[[ "$(printf '%s' "$BB" | field expires_at)" == 2030-01-01* ]] || fail "expires_at not echoed"
[[ "$(printf '%s' "$BB" | field escalates_at)" != "null" ]] || fail "escalates_at not set"
log "bounded job echoes deadline + escalates_at  ✓"

# A deadline we cannot parse is refused rather than silently becoming "no expiry".
CODE=$(curl -fsS -o /dev/null -w '%{http_code}' -X POST "$URL/jobs" \
  -H 'content-type: application/json' \
  -d '{"capability":"8b-extract","prompt":"x","deadline":"next tuesday"}' || true)
[[ "$CODE" == "422" ]] || fail "malformed deadline should be 422, got '$CODE'"
log "malformed deadline → 422  ✓ (not silently unbounded)"

# --- a worker joins: claimed, and visibly so, before it finishes ---------------------
log "NOW starting the worker…"
CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" CBK_MODEL_SERVER_URL="http://127.0.0.1:11447/v1" \
  CBK_MODEL="fake" CBK_CAPABILITIES="8b-extract" CBK_WORKER_ID="node-observed" \
  cbk_worker_bg work & PIDS+=($!)

for _ in $(seq 1 100); do
  ST=$(curl -fsS "$URL/jobs/$C" | field status)
  [[ "$ST" == "done" || "$ST" == "failed" ]] && break; sleep 0.2
done
[[ "$ST" == "done" ]] || fail "jobs did not drain, last status '$ST'"

FINAL=$(curl -fsS "$URL/jobs/$C")
[[ "$(printf '%s' "$FINAL" | field worker)" == "node-observed" ]] || fail "worker not reported"
[[ "$(printf '%s' "$FINAL" | field finished_at)" != "null" ]] || fail "finished_at not stamped"
[[ "$(printf '%s' "$FINAL" | field queue_position)" == "null" ]] || fail "terminal job still has a position"
log "finished: worker=node-observed, finished_at set, position cleared  ✓"

# The backlog must actually have drained, not merely been re-labelled.
BACKLOG=$(curl -fsS "$URL/queues" | python3 -c '
import sys, json
q = next(x for x in json.load(sys.stdin)["queues"] if x["capability"] == "8b-extract")
print(q["backlog"])')
[[ "$BACKLOG" == "0" ]] || fail "backlog should be 0 after draining, got '$BACKLOG'"
log "backlog back to 0  ✓"

printf '\033[32m[wait] PASS — an honest wait state: backlog, position, limits, observed claim\033[0m\n'
