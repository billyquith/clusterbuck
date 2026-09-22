#!/usr/bin/env bash
# Prove a job survives the machine holding it being killed mid-generation (ADR 20).
#
# This is the property the whole queue design is chosen for — "a laptop closed its lid
# mid-job" (reaper.py's own header) — and until now nothing exercised it against real
# processes. Every other script here completes its jobs, so claim and ack happen between
# two coordinator ticks and there is no mid-run window to interrupt. Four separate fixes
# to the recovery path landed with only unit coverage behind them, which is exactly the
# kind of gap this suite exists to close.
#
# SIGKILL, not SIGTERM, and that distinction is the test. A worker asked politely to stop
# finishes the job in hand and exits clean, which proves the graceful path; a worker that
# is killed leaves a claimed entry in the pending list with no result and nobody coming
# back for it. Only the coordinator can recover that, and only through XAUTOCLAIM.
#
# Asserted here, in order: the job is genuinely claimed and running; killing its worker
# leaves it unanswered; the reaper requeues it with attempts incremented; a second worker
# picks it up and answers; and the client — which did nothing but poll throughout — ends
# up with a completion and never had to resubmit.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_NAME=abandon
# shellcheck source=lib.sh
source "$REPO/deploy/e2e/lib.sh"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"

PORT="${CBK_PORT:-8101}"
URL="http://127.0.0.1:$PORT"
STALL_PORT=11461     # the model server that hangs, for the worker we are going to kill
FAST_PORT=11462      # and an instant one, so the rescuer is not stalled in its turn

# Long enough that the job is still in flight while we kill its worker, and longer than
# the whole recovery, so a stall that somehow outlived its process could not be mistaken
# for the rescue.
STALL_S=120
# The reaper's own thresholds, compressed. In production these are minutes — the idle
# threshold MUST exceed the longest plausible inference or a merely-busy worker would be
# robbed (reaper.py) — but the mechanism is identical at any scale, and a 10-minute e2e
# run would simply never be run.
MIN_IDLE_MS=2000
TICK_S=1             # reaper fires every 6 ticks, so ~6s here rather than ~60s

flush_redis

python3 "$REPO/server/tools/fake_model_server.py" --port "$STALL_PORT" \
  --stall-s "$STALL_S" & PIDS+=($!)
python3 "$REPO/server/tools/fake_model_server.py" --port "$FAST_PORT" & PIDS+=($!)
wait_for "http://127.0.0.1:$STALL_PORT/healthz" "stalling model server"
wait_for "http://127.0.0.1:$FAST_PORT/healthz" "fast model server"

( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_REAPER_MIN_IDLE_MS="$MIN_IDLE_MS" CBK_ESCALATION_INTERVAL_S="$TICK_S" \
  .venv/bin/python -m clusterbuck ) & PIDS+=($!)
wait_for "$URL/healthz" "server"
log "coordinator up (reaper: idle>${MIN_IDLE_MS}ms, tick ${TICK_S}s)"

job_field() { curl -fsS "$URL/jobs/$1" | jqpy "$2"; }

# --- a worker takes a job and starts generating ---------------------------------------

CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_MODEL_SERVER_URL="http://127.0.0.1:$STALL_PORT/v1" \
  CBK_MODEL="fake" CBK_CAPABILITIES="8b-extract" CBK_WORKER_ID="node-doomed" \
  cbk_worker_bg work & DOOMED=$!; PIDS+=($DOOMED)
log "worker node-doomed started against the stalling model server (pid $DOOMED)"

JOB=$(curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' \
  -d '{"capability":"8b-extract","messages":[{"role":"user","content":"survive me"}],
       "urgency":"necessary","privacy":"local_only"}' | jqpy '["id"]')
log "submitted $JOB"

# Claimed means it is in the pending-entries list — the only place a job in flight
# exists, and precisely what XAUTOCLAIM later walks.
for _ in $(seq 1 60); do
  PENDING=$(redis_cli -n 0 XPENDING q:8b-extract cbk-workers | head -1 | tr -d '\r')
  [[ "$PENDING" == "1" ]] && break
  sleep 0.5
done
[[ "${PENDING:-0}" == "1" ]] || fail "job was never claimed (pending=${PENDING:-0})"
log "job is claimed and generating  ✓"

[[ "$(job_field "$JOB" '["status"]')" != "done" ]] || fail "finished before we could kill it"

# --- kill the machine under it ---------------------------------------------------------

kill -9 "$DOOMED" 2>/dev/null || fail "could not SIGKILL the worker"
wait "$DOOMED" 2>/dev/null || true
log "SIGKILLed node-doomed mid-generation — no result written, entry still claimed"

kill -0 "$DOOMED" 2>/dev/null && fail "worker survived a SIGKILL?"
[[ "$(job_field "$JOB" '["status"]')" != "done" ]] || fail "job answered by a dead worker"

# --- the coordinator recovers it --------------------------------------------------------

log "waiting for the reaper…"
for _ in $(seq 1 60); do
  ATTEMPTS=$(job_field "$JOB" '.get("attempts") or 0')
  [[ "$ATTEMPTS" -ge 1 ]] && break
  sleep 0.5
done
[[ "${ATTEMPTS:-0}" -ge 1 ]] || fail "the reaper never requeued it (attempts=${ATTEMPTS:-0})"
log "reaper requeued it, attempts=$ATTEMPTS  ✓"

# Back on the stream and undelivered: the requeue is a NEW entry, which is what makes it
# claimable by somebody else rather than stuck against the dead consumer.
STATUS=$(job_field "$JOB" '["status"]')
[[ "$STATUS" == "queued" ]] || fail "expected status queued after requeue, got $STATUS"
log "status back to queued, and its delivery re-recorded  ✓"

# --- and another node finishes it ---------------------------------------------------

CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_MODEL_SERVER_URL="http://127.0.0.1:$FAST_PORT/v1" \
  CBK_MODEL="fake" CBK_CAPABILITIES="8b-extract" CBK_WORKER_ID="node-rescuer" \
  cbk_worker_bg work & PIDS+=($!)
log "worker node-rescuer started"

for _ in $(seq 1 60); do
  STATUS=$(job_field "$JOB" '["status"]')
  [[ "$STATUS" == "done" ]] && break
  sleep 0.5
done
[[ "$STATUS" == "done" ]] || fail "job never completed after recovery (status=$STATUS)"

WORKER=$(job_field "$JOB" '.get("worker") or "?"')
[[ "$WORKER" == "node-rescuer" ]] || fail "expected node-rescuer to answer it, got $WORKER"

CONTENT=$(curl -fsS "$URL/jobs/$JOB" | jqpy '["result"]["choices"][0]["message"]["content"]')
[[ "$CONTENT" == *"survive me"* ]] || fail "the answer is not for this job: $CONTENT"
log "answered by $WORKER, and it is the right answer  ✓"

# The client's whole experience: it polled, and it never resubmitted. A job that needed
# resubmitting would have been a different id.
[[ "$(job_field "$JOB" '["id"]')" == "$JOB" ]] || fail "job id changed under the client"

pass "a job survives its worker being killed mid-generation and is served by another node"
