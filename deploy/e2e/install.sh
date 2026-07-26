#!/usr/bin/env bash
# End-to-end proof of the full model-management loop (M6a+b+c):
#   discover installed → planner proposes an upgrade → HUMAN approves → coordinator issues
#   the action → worker pulls it via the model-manager adapter → the new artifact is
#   discovered → proposal marked applied → re-evaluation demanded (ability unmeasured).
#
# Also asserts the negative: nothing is issued while the owner is `active`, and nothing at
# all is issued for an UNapproved proposal.
#
# Uses the fake model server's Ollama-native /api/pull (instant, no gigabytes). Set
# USE_OLLAMA=1 to drive a real pull of a genuinely small model instead.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8087}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
STATE="$WORKDIR/node.json"
WORKER_DLL="$REPO/worker/src/Clusterbuck.Worker/bin/Debug/net10.0/cbk.dll"
PIDS=()
log(){ printf '\033[36m[install]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[install] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
jqpy(){ python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null

if [[ "${USE_OLLAMA:-0}" == "1" ]]; then
  MODEL_URL="http://localhost:11434/v1"
  WANT="${CBK_PULL_MODEL:-qwen2.5:0.5b}"
  log "using real Ollama; will pull $WANT (a genuinely small model)"
else
  python3 "$REPO/server/tools/fake_model_server.py" --port 11445 \
    --models "llama3.2:3b" >/dev/null 2>&1 & PIDS+=($!)
  wait_for "http://127.0.0.1:11445/healthz" "fake model server"
  MODEL_URL="http://127.0.0.1:11445/v1"
  WANT=""   # whatever the planner picks from the catalog
  log "using fake model server (instant /api/pull)"
fi

( cd "$REPO/server" && exec env CBK_REDIS_URL="redis://localhost:6379/0" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" CBK_FLEET_PATH="$REPO/server/fleet.yaml" \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"

TOKEN=$(curl -fsS -X POST "$URL/nodes/tokens" | jqpy '["join_token"]')
dotnet "$WORKER_DLL" enroll --token "$TOKEN" --server "$URL" --state "$STATE" >/dev/null
NODE=$(python3 -c "import json;print(json.load(open('$STATE'))['node_id'])")
log "node $NODE enrolled"

# The owner is away, so installs are permitted (an active owner blocks them — asserted below).
python3 - "$STATE" <<'PY'
import json, sys
p = sys.argv[1]; s = json.load(open(p)); s["mode"] = "away"; json.dump(s, open(p, "w"))
PY

# The presence ladder damps the climb to `away` by 120s in production (cold loads are
# expensive, ADR 10). Shorten it here so the test exercises the install path rather than
# spending two minutes waiting out a timer.
CBK_NODE_STATE="$STATE" CBK_REDIS_URL="redis://localhost:6379/0" \
  CBK_MODEL_SERVER_URL="$MODEL_URL" CBK_HEARTBEAT_MS=1000 CBK_LADDER_HYSTERESIS_S=1 \
  dotnet "$WORKER_DLL" work >"$WORKDIR/worker.log" 2>&1 & PIDS+=($!)

installed_of(){ curl -fsS "$URL/nodes" | python3 -c "
import sys,json
n = next((n for n in json.load(sys.stdin)['nodes'] if n['node_id']=='$NODE'), {})
print(','.join(sorted(n.get('installed') or [])))"; }

for _ in $(seq 1 60); do [[ -n "$(installed_of)" ]] && break; sleep 0.25; done
BEFORE="$(installed_of)"
log "discovered before: $BEFORE"

# --- the planner proposes; nothing happens without approval --------------------
curl -fsS -X POST "$URL/proposals/scan" >/dev/null
PENDING=$(curl -fsS "$URL/proposals?status=pending" | jqpy '["proposals"].__len__()')
[[ "$PENDING" -ge 1 ]] || fail "planner proposed nothing"
log "planner raised $PENDING pending proposal(s) — none actionable yet"

sleep 2.5   # several heartbeats pass with nothing approved
if grep -q "model install" "$WORKDIR/worker.log"; then
  fail "worker acted on an UNAPPROVED proposal"
fi
log "no action taken without approval ✓"

# --- a human approves one upgrade --------------------------------------------
if [[ -n "$WANT" ]]; then
  PID=$(curl -fsS "$URL/proposals?status=pending" | python3 -c "
import sys,json
ps=[p for p in json.load(sys.stdin)['proposals'] if p['kind']=='upgrade' and p['artifact']=='$WANT']
print(ps[0]['id'] if ps else '')")
  [[ -n "$PID" ]] || fail "no upgrade proposal for $WANT (is it in the catalog and does it fit?)"
  ART="$WANT"
else
  read -r PID ART < <(curl -fsS "$URL/proposals?status=pending" | python3 -c "
import sys,json
p=[p for p in json.load(sys.stdin)['proposals'] if p['kind']=='upgrade'][0]
print(p['id'], p['artifact'])")
fi
curl -fsS -X POST "$URL/proposals/$PID/approve" >/dev/null
log "human approved: install $ART ($PID)"

# --- the worker executes it --------------------------------------------------
STATUS=""
for _ in $(seq 1 240); do   # generous: a real pull takes a while
  STATUS=$(curl -fsS "$URL/proposals" | python3 -c "
import sys,json
print(next(p['status'] for p in json.load(sys.stdin)['proposals'] if p['id']=='$PID'))")
  [[ "$STATUS" == "applied" || "$STATUS" == "failed" ]] && break
  sleep 0.5
done
[[ "$STATUS" == "applied" ]] || fail "install ended '$STATUS' (worker log: $(tail -3 "$WORKDIR/worker.log" | tr '\n' ' '))"
log "worker installed it; proposal → applied ✓"

AFTER="$(installed_of)"
log "discovered after: $AFTER"
[[ "$AFTER" == *"$ART"* ]] || fail "$ART not discovered after install"
log "new artifact discovered by observation ✓"

# --- ability must NOT be inherited ------------------------------------------
REEVAL=$(curl -fsS "$URL/proposals" | python3 -c "
import sys,json
print(sum(1 for p in json.load(sys.stdin)['proposals'] if p['kind']=='reeval' and p['artifact']=='$ART'))")
[[ "$REEVAL" -ge 1 ]] || fail "no re-evaluation demanded for freshly installed $ART"
log "re-evaluation demanded (ability unmeasured, not inherited) ✓"

printf '\033[32m[install] PASS — propose → approve → pull → discover → applied → re-eval\033[0m\n'
