#!/usr/bin/env bash
# End-to-end proof of the M0 loop (implementation.md → M0):
#   submit (Python API) → q:<cap> stream (Redis) → worker pulls (C#) →
#   model server → result written → poll returns done.
#
# Proves both the async plane and the cross-language contract on one node.
#
# Needs: Redis on localhost:6379, the server venv (server/.venv), the worker built.
# Default model server is the fake stub (zero weight). Set USE_OLLAMA=1 to drive a real
# Ollama instead (expects `ollama serve` + the model in $CBK_MODEL).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}"
SERVER_PORT="${CBK_PORT:-8077}"
SERVER_URL="http://127.0.0.1:${SERVER_PORT}"
CAP="8b-extract"
WORKDIR="$(mktemp -d)"
PIDS=()

log()  { printf '\033[36m[e2e]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[e2e] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

wait_for() {  # url, name
  for _ in $(seq 1 50); do
    if curl -fsS "$1" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  fail "$2 did not become ready at $1"
}

# --- Redis reachable? -------------------------------------------------------
redis-cli -u "$REDIS_URL" ping >/dev/null 2>&1 \
  || ${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} ping >/dev/null 2>&1 \
  || fail "no Redis reachable at $REDIS_URL"
log "redis ok"

# --- model server -----------------------------------------------------------
if [[ "${USE_OLLAMA:-0}" == "1" ]]; then
  MODEL_URL="http://localhost:11434/v1"
  MODEL_NAME="${CBK_MODEL:-llama3.2:3b}"
  log "using Ollama at $MODEL_URL model=$MODEL_NAME"
else
  MODEL_URL="http://127.0.0.1:11435/v1"
  MODEL_NAME="fake"
  python3 "$REPO/server/tools/fake_model_server.py" --port 11435 &
  PIDS+=($!)
  wait_for "http://127.0.0.1:11435/healthz" "fake model server"
  log "fake model server up"
fi

# --- server -----------------------------------------------------------------
( cd "$REPO/server" && exec env \
  CBK_REDIS_URL="$REDIS_URL" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$SERVER_PORT" \
  .venv/bin/python -m clusterbuck ) &
PIDS+=($!)
wait_for "$SERVER_URL/healthz" "server"
log "server up on $SERVER_URL"

# --- worker -----------------------------------------------------------------
CBK_REDIS_URL="$REDIS_URL" CBK_MODEL_SERVER_URL="$MODEL_URL" CBK_MODEL="$MODEL_NAME" \
  CBK_CAPABILITIES="$CAP" CBK_WORKER_ID="node-e2e" \
  cbk_worker_bg work &
PIDS+=($!)
sleep 1
log "worker up (serving $CAP → $MODEL_NAME)"

# --- submit a job -----------------------------------------------------------
SUBMIT=$(curl -fsS -X POST "$SERVER_URL/jobs" -H 'content-type: application/json' -d "{
  \"capability\": \"$CAP\",
  \"messages\": [{\"role\": \"user\", \"content\": \"ping from e2e\"}],
  \"urgency\": \"necessary\",
  \"privacy\": \"local_only\"
}")
JOB_ID=$(printf '%s' "$SUBMIT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')
log "submitted $JOB_ID"

# --- poll for the result ----------------------------------------------------
for _ in $(seq 1 100); do
  RES=$(curl -fsS "$SERVER_URL/jobs/$JOB_ID")
  STATUS=$(printf '%s' "$RES" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')
  [[ "$STATUS" == "done" || "$STATUS" == "failed" ]] && break
  sleep 0.2
done

[[ "$STATUS" == "done" ]] || fail "job ended '$STATUS', not done: $RES"
CONTENT=$(printf '%s' "$RES" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["choices"][0]["message"]["content"])')
WORKER=$(printf '%s' "$RES" | python3 -c 'import sys,json; print(json.load(sys.stdin)["worker"])')
log "done by $WORKER"
log "completion: $CONTENT"

# --- the same round-trip through the CLI verbs ------------------------------
# The curl checks above prove the SERVER. These prove the CLI reads what the server
# actually sends: `cbk submit` and `cbk status` had no coverage anywhere, and both were
# reading fields the coordinator has never returned — submit printed "submitted None",
# and status printed the state but silently dropped the completion.
CLI_SUBMIT=$(cbk_worker submit --server "$SERVER_URL" -p "ping from the cli" \
  --capability "$CAP" --urgency necessary 2>&1) || fail "cbk submit failed: $CLI_SUBMIT"
CLI_JOB=$(grep -oE 'job_[0-9a-f]+' <<<"$CLI_SUBMIT" | head -1)
[[ -n "$CLI_JOB" ]] || fail "cbk submit printed no job id: $CLI_SUBMIT"
log "cbk submit → $CLI_JOB"

CLI_STATUS=""
for _ in $(seq 1 100); do
  CLI_STATUS=$(cbk_worker status --server "$SERVER_URL" "$CLI_JOB" 2>&1) || true
  grep -q "$CLI_JOB: done" <<<"$CLI_STATUS" && break
  sleep 0.2
done
grep -q "$CLI_JOB: done" <<<"$CLI_STATUS" || fail "cbk status never reported done: $CLI_STATUS"
# The completion text itself has to reach the terminal. Asserting only on the status line
# is exactly what made the broken verb look like a pass — so strip the metadata lines and
# require that something is left. (Not matched against $CONTENT: a real model server under
# USE_OLLAMA=1 answers this second prompt differently.)
CLI_BODY=$(grep -vE "^${CLI_JOB}: |^worker: |^attempts: " <<<"$CLI_STATUS" || true)
[[ -n "${CLI_BODY//[[:space:]]/}" ]] || fail "cbk status printed no completion: $CLI_STATUS"
log "cbk status printed the completion: $(head -1 <<<"$CLI_BODY")"

printf '\033[32m[e2e] PASS — M0 loop end-to-end (API + CLI)\033[0m\n'
