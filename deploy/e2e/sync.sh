#!/usr/bin/env bash
# End-to-end proof of the SYNC plane (protocols.md §1a):
#   client → /v1/chat/completions (LiteLLM Router) → model server → OpenAI reply.
#
# The C# worker is deliberately NOT started — the sync path bypasses it (it calls the
# node's model_server directly). Also exercises `cbk fleet` against GET /fleet.
#
# Default model server is the fake stub. Set USE_OLLAMA=1 to drive real Ollama.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
PORT="${CBK_PORT:-8079}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()

log()  { printf '\033[36m[sync]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[sync] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }

# --- model server -----------------------------------------------------------
if [[ "${USE_OLLAMA:-0}" == "1" ]]; then
  MODEL_URL="http://localhost:11434/v1"; MODEL_NAME="${CBK_MODEL:-llama3.2:3b}"
  log "using Ollama at $MODEL_URL model=$MODEL_NAME"
else
  python3 "$REPO/server/tools/fake_model_server.py" --port 11438 & PIDS+=($!)
  wait_for "http://127.0.0.1:11438/healthz" "fake model server"
  MODEL_URL="http://127.0.0.1:11438/v1"; MODEL_NAME="fake"
  log "fake model server up"
fi

# --- fleet.yaml pointing 8b-extract at the model server ---------------------
cat > "$WORKDIR/fleet.yaml" <<YAML
nodes: []
capabilities:
  8b-extract:
    queue: "q:8b-extract"
    model_server: "$MODEL_URL"
    model: "$MODEL_NAME"
YAML

# --- server (serves both planes; sync needs the fleet) ----------------------
( cd "$REPO/server" && exec env \
  CBK_REDIS_URL="redis://localhost:6379/0" CBK_DB_PATH="$WORKDIR/cbk.db" \
  CBK_PORT="$PORT" CBK_FLEET_PATH="$WORKDIR/fleet.yaml" .venv/bin/python -m clusterbuck ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"
log "server up on $URL (sync plane from fleet.yaml)"

# --- cbk fleet --------------------------------------------------------------
cbk_worker fleet --server "$URL" 2>/dev/null || true

# --- OpenAI-compatible chat completion --------------------------------------
RESP=$(curl -fsS -X POST "$URL/v1/chat/completions" -H 'content-type: application/json' -d '{
  "model": "8b-extract",
  "messages": [{"role": "user", "content": "sync plane hello"}]
}') || fail "request failed"

CONTENT=$(printf '%s' "$RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["choices"][0]["message"]["content"])')
[[ -n "$CONTENT" ]] || fail "empty completion: $RESP"
log "completion: $CONTENT"
printf '\033[32m[sync] PASS — sync plane end-to-end via LiteLLM\033[0m\n'
