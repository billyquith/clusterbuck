#!/usr/bin/env bash
# Prove a new machine can join with ONE secret and no operator key (install/worker/join.py).
#
# Onboarding used to hand-carry two secrets onto every new box: the operator key (to mint a
# join token) and the Redis password (as a command-line argument, landing in shell history).
# This flow carries one join password, exchanged with the coordinator for a single-use token
# plus the broker URL — so the operator key never leaves the coordinator.
#
# Runs `join.py --dry-run`, which does everything except the privileged install step, so
# this needs no root.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8098}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()
PASSWORD="a-long-enough-join-password"
OPERATOR_KEY="operator-key-must-not-leak"
log(){ printf '\033[36m[join-e2e]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[join-e2e] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 60); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }

# The artifact the coordinator will hand out. Built here so the test proves the real file
# moves, not a placeholder.
if [[ ! -f "$REPO/worker/dist/cbk.pyz" ]]; then
  log "building the worker artifact…"
  ( cd "$REPO/worker" && uv run python build.py >/dev/null 2>&1 )
fi
ARTIFACT="$REPO/worker/dist/cbk.pyz"
[[ -f "$ARTIFACT" ]] || fail "no artifact at $ARTIFACT"

cat > "$WORKDIR/fleet.yaml" <<YAML
capabilities:
  8b-extract:
    queue: 'q:8b-extract'
    model_server: 'http://127.0.0.1:1/v1'
    model: 'm'
YAML

( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_API_KEY="$OPERATOR_KEY" CBK_JOIN_PASSWORD="$PASSWORD" \
  CBK_WORKER_ARTIFACT="$ARTIFACT" CBK_FLEET_PATH="$WORKDIR/fleet.yaml" \
  CBK_BROKER_ADVERTISE_URL="redis://192.168.50.146:6379/0" \
  .venv/bin/python -m clusterbuck >"$WORKDIR/server.log" 2>&1 ) & PIDS+=($!)
wait_for "$URL/healthz" "server"
log "coordinator up with bootstrap enabled"

# --- the gate -----------------------------------------------------------------------
CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$URL/nodes/bootstrap")
[[ "$CODE" == "401" ]] || fail "no password should be 401, got $CODE"
CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$URL/nodes/bootstrap" \
  -H "X-CBK-Join-Password: wrong")
[[ "$CODE" == "401" ]] || fail "wrong password should be 401, got $CODE"
log "bootstrap rejects a missing and a wrong password  ✓"

BODY=$(curl -fsS -X POST "$URL/nodes/bootstrap" -H "X-CBK-Join-Password: $PASSWORD")
printf '%s' "$BODY" | grep -q "$OPERATOR_KEY" && fail "the operator key leaked into the bootstrap response"
log "operator key is NOT in the response  ✓ (the whole point)"

# A remote worker cannot reach a loopback broker. This single-host test would otherwise
# never notice — here the coordinator and the "worker" share a machine, so a loopback
# address works by accident. Assert on the advertised value instead.
printf '%s' "$BODY" | python3 -c '
import sys, json
from urllib.parse import urlsplit
host = urlsplit(json.load(sys.stdin)["redis_url"]).hostname
if host in ("localhost", "127.0.0.1", "::1"):
    raise SystemExit(f"advertised a loopback broker ({host}) — a remote worker cannot use it")
print("  advertised broker host:", host)
' || fail "bootstrap advertised an unusable broker address"
log "advertised broker is routable, not loopback  ✓"

# The exemption must not have opened anything else.
CODE=$(curl -sS -o /dev/null -w '%{http_code}' "$URL/nodes")
[[ "$CODE" == "401" ]] || fail "/nodes should still need the operator key, got $CODE"
log "every other endpoint still needs the operator key  ✓"

# --- the client path ----------------------------------------------------------------
# A node.json shaped like the one a large machine gets: RAM-based capability proposal
# offers tiers this small fleet has never defined.
cat > "$WORKDIR/node.json" <<JSON
{"node_id":"node-e2e","node_key":"k","server":"$URL",
 "capabilities":["8b-extract","32b-reason"]}
JSON

OUT="$WORKDIR/join.out"
python3 "$REPO/install/worker/join.py" --coordinator "$URL" --model m \
  --password "$PASSWORD" --node-state "$WORKDIR/node.json" --dry-run >"$OUT" 2>&1 \
  || fail "join.py failed: $(tail -3 "$OUT")"

grep -q "single-use join token" "$OUT" || fail "no token reported: $(cat "$OUT")"
grep -q "artifact →" "$OUT" || fail "artifact was not downloaded: $(cat "$OUT")"
log "join.py bootstrapped and downloaded the real artifact  ✓"

# The secrets must never be echoed — this output is the thing a person pastes into chat.
grep -q '<password>' "$OUT" || fail "the redis password was not redacted in the handoff"
grep -q '<join-token>' "$OUT" || fail "the join token was not redacted in the handoff"
printf '%s' "$BODY" | python3 -c '
import sys, json
url = json.load(sys.stdin)["redis_url"]
secret = url.split("@")[0]
if len(secret) > 12 and secret in open(sys.argv[1]).read():
    raise SystemExit("the broker credential appeared verbatim in join.py output")
' "$OUT" || fail "broker credential leaked into join.py output"
log "redis password and token are redacted in the output  ✓"

# The silent-failure trap, now loud.
grep -q "NOT in the coordinator's registry" "$OUT" || fail "no capability warning: $(cat "$OUT")"
grep -q "32b-reason" "$OUT" || fail "the warning did not name the unroutable tier"
log "warns that 32b-reason would never route  ✓ (enrolment alone looks healthy)"

printf '\033[32m[join-e2e] PASS — one password joins a node; the operator key stays home\033[0m\n'
