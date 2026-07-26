#!/usr/bin/env bash
# End-to-end proof that the shared secret closes the audited privilege-escalation chain
# (ADR 26), against a real running server rather than a TestClient.
#
# The chain an audit walked anonymously was:
#   POST /nodes/tokens  ->  POST /nodes/enroll  ->  POST /nodes/{id}/policy?auto_approve=true
#   ->  POST /proposals/scan  ->  heartbeat returns an approved multi-GB install
# and its destructive variant: approving a `reclaim` proposal makes a worker DELETE an
# owner's model files. Both start at /nodes/tokens, so blocking step 1 collapses the chain.
#
# Also proves a node can still bootstrap without the operator secret (join token + node key),
# which is what keeps workers deployable without distributing the admin credential.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8089}"
URL="http://127.0.0.1:$PORT"
KEY="e2e-operator-secret"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[auth]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[auth] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
code(){ curl -s -o /dev/null -w '%{http_code}' "$@"; }

docker exec cbk-redis redis-cli -n 0 FLUSHDB >/dev/null

( cd "$REPO/server" && exec env \
  CBK_REDIS_URL="redis://localhost:6379/0" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_FLEET_PATH="$REPO/server/fleet.yaml" CBK_API_KEY="$KEY" \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"          # healthz is exempt, so this also proves the exemption
log "server up with CBK_API_KEY set"

# --- 1. the chain is refused at step 1, and at every later step independently -----------
for probe in \
  "POST /nodes/tokens" \
  "POST /proposals/scan" \
  "GET  /nodes" \
  "GET  /usage" \
  "GET  /"      ; do
  m=${probe%% *}; p=${probe##* }
  got=$(code -X "$m" "$URL$p")
  [[ "$got" == "401" ]] || fail "$m $p returned $got, expected 401 without a key"
done
log "anonymous: /nodes/tokens, /proposals/scan, /nodes, /usage, / all 401 ✓"

got=$(code -X POST "$URL/nodes/some-node/policy?auto_approve=true")
[[ "$got" == "401" ]] || fail "anonymous policy flip returned $got, expected 401"
log "anonymous auto_approve flip refused (401) — chain collapsed ✓"

# --- 2. the operator can still do all of it with the key --------------------------------
TOKEN=$(curl -fsS -X POST "$URL/nodes/tokens" -H "X-CBK-Api-Key: $KEY" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["join_token"])')
[[ -n "$TOKEN" ]] || fail "operator could not mint a join token"
log "operator with key: minted a join token ✓"

# Bearer form works too.
got=$(code "$URL/fleet" -H "Authorization: Bearer $KEY")
[[ "$got" == "200" ]] || fail "bearer form returned $got"
log "Authorization: Bearer accepted ✓"

# --- 3. a node bootstraps WITHOUT the operator secret ------------------------------------
NODE=$(curl -fsS -X POST "$URL/nodes/enroll" -H 'content-type: application/json' -d "{
  \"join_token\": \"$TOKEN\", \"hostname\": \"e2e\", \"os\": \"linux\", \"arch\": \"arm64\",
  \"hw\": {\"ram_gb\": 16, \"accelerator\": \"cpu\", \"disk_free_gb\": 50},
  \"profile\": \"shared\"}")
NODE_ID=$(printf '%s' "$NODE" | python3 -c 'import sys,json;print(json.load(sys.stdin)["node_id"])')
NODE_KEY=$(printf '%s' "$NODE" | python3 -c 'import sys,json;print(json.load(sys.stdin)["node_key"])')
log "enrolled $NODE_ID with only a join token (no operator key) ✓"

got=$(code -X POST "$URL/nodes/$NODE_ID/heartbeat" -H 'content-type: application/json' \
  -H "x-cbk-node-key: $NODE_KEY" -d '{"mode":"active"}')
[[ "$got" == "200" ]] || fail "heartbeat with node key returned $got"
got=$(code -X POST "$URL/nodes/$NODE_ID/heartbeat" -H 'content-type: application/json' \
  -H "x-cbk-node-key: wrong" -d '{"mode":"active"}')
[[ "$got" == "401" ]] || fail "heartbeat with a wrong node key returned $got, expected 401"
log "heartbeat: node key accepted, wrong key refused ✓"

# --- 4. the policy gate now needs a body, so it cannot be flipped by URL ------------------
got=$(code -X POST "$URL/nodes/$NODE_ID/policy?auto_approve=true" -H "X-CBK-Api-Key: $KEY")
[[ "$got" == "422" ]] || fail "query-param policy flip returned $got, expected 422"
AUTO=$(curl -fsS -X POST "$URL/nodes/$NODE_ID/policy" -H "X-CBK-Api-Key: $KEY" \
  -H 'content-type: application/json' -d '{"auto_approve": true}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["auto_approve"])')
[[ "$AUTO" == "True" ]] || fail "operator body form failed (got $AUTO)"
log "policy: URL form rejected (422), operator body form works ✓"

printf '\033[32m[auth] PASS — shared secret closes the escalation chain; nodes still bootstrap\033[0m\n'
