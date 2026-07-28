#!/usr/bin/env bash
# End-to-end proof of worker SELF-UPDATE for the Python worker (protocols.md §7, ADR 13),
# using two REAL built zipapps — not a stub, not an editable install.
#
#   build 0.7.0 + 0.9.9 → worker runs the 0.7.0 zipapp → coordinator offers signed 0.9.9 →
#   worker verifies the signature, fetches, checks the digest, replaces its own .pyz,
#   re-execs → reports 0.9.9
#
# The .NET equivalent is selfupdate.sh. Both are needed because the artifact and the swap
# mechanism genuinely differ (a 73 MB single-file binary vs a 2.8 MB zipapp), so neither
# proves the other. This one also pins down the property the whole packaging change rests on:
# the artifact offered is `py3-none-any`, and the SAME release is not allowed to hand this
# worker a .NET binary (ADR 29).
#
# Also proves the refusals that make an update channel safe to switch on:
#   * a TAMPERED manifest (digest changed after signing) is rejected, artifact untouched
#   * without a pinned public key the worker refuses to self-update at all
#   * an opted-OUT node is never offered an update in the first place
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8093}"
FILE_PORT="${CBK_FILE_PORT:-8094}"
MODEL_PORT="${CBK_MODEL_PORT:-11449}"
URL="http://127.0.0.1:$PORT"
KEYDIR="$(mktemp -d)"; WORKDIR="$(mktemp -d)"; DIST="$WORKDIR/dist"; INSTALL="$WORKDIR/bin"
STATE="$WORKDIR/node.json"
RID="py3-none-any"          # one artifact, every platform — that is the point
PIDS=()
sha256(){ if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d" " -f1;
          else shasum -a 256 "$1" | cut -d" " -f1; fi; }
log(){ printf '\033[36m[selfupd-py]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[selfupd-py] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
           pkill -f "$INSTALL/cbk.pyz" 2>/dev/null || true; rm -rf "$WORKDIR" "$KEYDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 60); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.25; done; fail "$2 not ready"; }
jqpy(){ python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }
node_field(){ curl -fsS "$URL/nodes" | python3 -c "
import sys,json
n=next((n for n in json.load(sys.stdin)['nodes'] if n['node_id']=='$1'),{})
print(n.get('$2') or '')"; }

mkdir -p "$DIST" "$INSTALL"
PYWORKER="$REPO/worker/python"
[[ -x "$PYWORKER/.venv/bin/python" ]] \
  || fail "python worker not installed — run: (cd $PYWORKER && uv venv && uv pip install -e '.[dev]')"
# The zipapp deliberately does NOT vendor `cryptography` (it is the verifier, so it cannot be
# delivered through the channel it secures — see worker/python/README.md). A node therefore
# needs it in the interpreter that runs the artifact; use one that has it, as a real node must.
PY="$PYWORKER/.venv/bin/python"
"$PY" -c "import cryptography" 2>/dev/null \
  || fail "$PY lacks cryptography — self-update cannot be verified"

# --- operator signing key (never committed; generated per run) ----------------------------
"$REPO/server/.venv/bin/python" - <<PY
from clusterbuck import signing
k = signing.generate_keypair()
open("$KEYDIR/sign.key.pem","w").write(signing.private_pem(k))
open("$KEYDIR/sign.pub.pem","w").write(signing.public_pem(k))
PY
log "generated an ephemeral update-signing keypair"

# --- build two real zipapps ----------------------------------------------------------------
log "building 0.7.0 and 0.9.9 zipapps…"
( cd "$PYWORKER" && .venv/bin/python build.py --version 0.7.0 --name cbk.pyz \
    --out "$INSTALL" >/dev/null 2>&1 ) || fail "0.7.0 build failed"
( cd "$PYWORKER" && .venv/bin/python build.py --version 0.9.9 --name cbk-0.9.9.pyz \
    --out "$DIST" >/dev/null 2>&1 ) || fail "0.9.9 build failed"
[[ -f "$INSTALL/cbk.pyz" && -f "$DIST/cbk-0.9.9.pyz" ]] || fail "zipapps not produced"
SHA=$(sha256 "$DIST/cbk-0.9.9.pyz")
SIZE=$(( $(wc -c < "$DIST/cbk-0.9.9.pyz") / 1024 ))
log "built; 0.9.9 is ${SIZE} KB, digest ${SHA:0:16}…"

# Sanity: the installed artifact really reports 0.7.0, or the rest proves nothing.
INSTALLED_V=$("$PY" "$INSTALL/cbk.pyz" --version)
[[ "$INSTALLED_V" == "0.7.0" ]] || fail "installed zipapp reports '$INSTALLED_V', not 0.7.0"

# --- static file server for the artifact --------------------------------------------------
( cd "$DIST" && exec python3 -m http.server "$FILE_PORT" >/dev/null 2>&1 ) & PIDS+=($!)
wait_for "http://127.0.0.1:$FILE_PORT/cbk-0.9.9.pyz" "artifact host"

# A release carrying BOTH artifacts: the coordinator must pick by flavour, not platform.
cat > "$WORKDIR/release.json" <<JSON
{"version":"0.9.9","channel":"stable","protocol_version":1,
 "artifacts":{
   "$RID":{"url":"http://127.0.0.1:$FILE_PORT/cbk-0.9.9.pyz","sha256":"$SHA"},
   "osx-arm64":{"url":"http://127.0.0.1:$FILE_PORT/not-for-us","sha256":"$(printf 'f%.0s' {1..64})"},
   "linux-x64":{"url":"http://127.0.0.1:$FILE_PORT/not-for-us","sha256":"$(printf 'f%.0s' {1..64})"}
 }}
JSON

python3 "$REPO/server/tools/fake_model_server.py" --port "$MODEL_PORT" >/dev/null 2>&1 & PIDS+=($!)
wait_for "http://127.0.0.1:$MODEL_PORT/healthz" "model server"
${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null

( cd "$REPO/server" && exec env \
  CBK_REDIS_URL="redis://localhost:6379/0" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_FLEET_PATH="$REPO/server/fleet.yaml" CBK_WOL_BROADCAST=127.0.0.1 \
  CBK_WORKER_CURRENT_VERSION=0.9.9 \
  CBK_UPDATE_SIGNING_KEY="$KEYDIR/sign.key.pem" CBK_UPDATE_RELEASE="$WORKDIR/release.json" \
  .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) & PIDS+=($!)
wait_for "$URL/healthz" "server"

TOKEN=$(curl -fsS -X POST "$URL/nodes/tokens" | jqpy '["join_token"]')
CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" \
  "$PY" "$INSTALL/cbk.pyz" enroll --token "$TOKEN" --server "$URL" --state "$STATE" >/dev/null
NODE=$(python3 -c "import json;print(json.load(open('$STATE'))['node_id'])")

run_worker(){   # $1 = extra env
  env CBK_NODE_STATE="$STATE" CBK_REDIS_URL="redis://localhost:6379/0" \
    CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" CBK_MODEL="fake" \
    CBK_HEARTBEAT_MS=500 CBK_LADDER_HYSTERESIS_S=1 $1 \
    "$PY" "$INSTALL/cbk.pyz" work >>"$WORKDIR/worker.log" 2>&1 &
  WORKER_PID=$!; PIDS+=($WORKER_PID)
}
stop_worker(){ kill "$WORKER_PID" 2>/dev/null || true
               pkill -f "$INSTALL/cbk.pyz work" 2>/dev/null || true; sleep 1; }

# --- 0. the coordinator knows what this worker is -----------------------------------------
run_worker "CBK_UPDATE_PUBKEY=$KEYDIR/sign.pub.pem"
for _ in $(seq 1 40); do [[ -n "$(node_field "$NODE" agent_version)" ]] && break; sleep 0.25; done
FLAV=$(node_field "$NODE" agent_flavour)
[[ "$FLAV" == "python" ]] || fail "coordinator recorded agent_flavour='$FLAV', expected python"
[[ "$(node_field "$NODE" agent_version)" == "0.7.0" ]] || fail "expected 0.7.0 installed"
log "coordinator recorded agent_flavour=python, version=0.7.0 ✓"

# --- 1. opted OUT ⇒ never offered an update ----------------------------------------------
for _ in $(seq 1 40); do [[ "$(node_field "$NODE" fitness)" == "stale" ]] && break; sleep 0.25; done
sleep 2
grep -q "installed 0.9.9" "$WORKDIR/worker.log" && fail "updated without opting in!"
log "opted out: flagged stale, but NOT updated ✓"
stop_worker

# --- 2. tampered manifest ⇒ refused, artifact untouched -----------------------------------
python3 - "$WORKDIR/release.json" <<'PY'
import json,sys
p=sys.argv[1]; d=json.load(open(p))
d["artifacts"]["py3-none-any"]["sha256"]="0"*64      # lie about the digest
json.dump(d,open(p,"w"))
PY
curl -fsS -X POST "$URL/nodes/$NODE/policy" -H 'content-type: application/json' \
  -d '{"auto_update": true}' >/dev/null
BEFORE=$(sha256 "$INSTALL/cbk.pyz")
: > "$WORKDIR/worker.log"
run_worker "CBK_UPDATE_PUBKEY=$KEYDIR/sign.pub.pem"
sleep 4
grep -q "digest mismatch" "$WORKDIR/worker.log" \
  || fail "tampered digest was not refused BY THE DIGEST CHECK (log: $(tail -3 "$WORKDIR/worker.log"))"
[[ "$(sha256 "$INSTALL/cbk.pyz")" == "$BEFORE" ]] || fail "artifact changed despite tamper"
log "tampered digest refused; artifact untouched ✓"
stop_worker

# Restore the honest digest.
python3 - "$WORKDIR/release.json" "$SHA" <<'PY'
import json,sys
p,sha=sys.argv[1],sys.argv[2]; d=json.load(open(p))
d["artifacts"]["py3-none-any"]["sha256"]=sha
json.dump(d,open(p,"w"))
PY

# --- 3. no pinned key ⇒ refuses outright -------------------------------------------------
: > "$WORKDIR/worker.log"
run_worker ""
sleep 4
grep -q "signed-or-nothing" "$WORKDIR/worker.log" \
  || fail "worker without a pinned key did not refuse (log: $(tail -3 "$WORKDIR/worker.log"))"
log "no pinned public key ⇒ self-update refused ✓"
stop_worker

# --- 4. the real thing: verify → fetch → swap → re-exec ----------------------------------
: > "$WORKDIR/worker.log"
run_worker "CBK_UPDATE_PUBKEY=$KEYDIR/sign.pub.pem"
NEW=""
for _ in $(seq 1 80); do
  NEW=$(node_field "$NODE" agent_version)
  [[ "$NEW" == "0.9.9" ]] && break; sleep 0.5
done
[[ "$NEW" == "0.9.9" ]] || fail "worker never upgraded (still $NEW; log: $(tail -5 "$WORKDIR/worker.log"))"
grep -q "installed 0.9.9" "$WORKDIR/worker.log" || fail "no install line in the worker log"
[[ -f "$INSTALL/cbk.prev.pyz" ]] || fail "previous artifact was not retained for rollback"
[[ "$(sha256 "$INSTALL/cbk.pyz")" == "$SHA" ]] || fail "installed artifact is not the signed one"
log "verified → fetched → swapped → re-exec'd; now reporting $NEW ✓"
log "previous artifact retained at cbk.prev.pyz ($(sha256 "$INSTALL/cbk.prev.pyz" | cut -c1-16)…) ✓"
# The swapped-in artifact must be a working worker, not just the right bytes.
[[ "$("$PY" "$INSTALL/cbk.pyz" --version)" == "0.9.9" ]] || fail "swapped artifact does not run"
[[ "$(node_field "$NODE" fitness)" == "ok" ]] || fail "fitness not ok after updating"
log "coordinator now judges it fit ✓"

printf '\033[32m[selfupd-py] PASS — signed zipapp self-update applied; tamper and unsigned paths refused\033[0m\n'
