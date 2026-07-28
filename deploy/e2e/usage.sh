#!/usr/bin/env bash
# End-to-end proof of usage metering (M3a): a completed job is captured by the coordinator's
# usage scan and shows up in /usage with a non-zero avoided-cloud-spend headline (local
# tokens priced at the fleet.yaml cloud-equivalent rate).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
PORT="${CBK_PORT:-8083}"
URL="http://127.0.0.1:$PORT"
CAP="8b-extract"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[usage]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[usage] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
jqpy(){ python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null

python3 "$REPO/server/tools/fake_model_server.py" --port 11439 & PIDS+=($!)
wait_for "http://127.0.0.1:11439/healthz" "fake model server"

( cd "$REPO/server" && exec env \
  CBK_REDIS_URL="redis://localhost:6379/0" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_FLEET_PATH="$REPO/server/fleet.yaml" CBK_ESCALATION_INTERVAL_S=1 \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"

CBK_REDIS_URL="redis://localhost:6379/0" CBK_MODEL_SERVER_URL="http://127.0.0.1:11439/v1" \
  CBK_MODEL="fake" CBK_CAPABILITIES="$CAP" CBK_WORKER_ID="node-usage" \
  cbk_worker_bg work & PIDS+=($!)
sleep 1
log "server + worker up"

JOB=$(curl -fsS -X POST "$URL/jobs" -H 'content-type: application/json' -d "{
  \"capability\": \"$CAP\",
  \"messages\": [{\"role\": \"user\", \"content\": \"count these words for metering please and thank you\"}],
  \"urgency\": \"necessary\"
}" | jqpy '["id"]')
log "submitted $JOB"

# wait for completion
for _ in $(seq 1 100); do
  ST=$(curl -fsS "$URL/jobs/$JOB" | jqpy '["status"]'); [[ "$ST" == "done" ]] && break; sleep 0.2
done
[[ "$ST" == "done" ]] || fail "job never completed ($ST)"

# wait for the coordinator's usage scan to capture it
JOBS=0
for _ in $(seq 1 40); do
  JOBS=$(curl -fsS "$URL/usage" | jqpy '["totals"]["jobs"]'); [[ "$JOBS" -ge 1 ]] && break; sleep 0.25
done
[[ "$JOBS" -ge 1 ]] || fail "usage never captured the job"

SUMMARY=$(curl -fsS "$URL/usage")
AVOIDED=$(printf '%s' "$SUMMARY" | jqpy '["headline"]["avoided_cloud_spend"]')
TOK_IN=$(printf '%s' "$SUMMARY" | jqpy '["totals"]["tokens_in"]')
log "captured jobs=$JOBS tokens_in=$TOK_IN avoided_cloud_spend=\$$AVOIDED"
python3 -c "import sys; sys.exit(0 if float('$AVOIDED') > 0 else 1)" || fail "avoided spend not > 0"
printf '\033[32m[usage] PASS — metering captures the job; avoided-cloud-spend headline > 0\033[0m\n'
