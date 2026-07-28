#!/usr/bin/env bash
# End-to-end proof of worker SELF-UPDATE (protocols.md §7, ADR 13) using two REAL published
# single-file binaries — not a stub, not `dotnet run`.
#
#   publish 0.7.0 → worker runs it → coordinator offers signed 0.9.9 → worker verifies the
#   signature, fetches, checks the digest, replaces its own binary, re-execs → reports 0.9.9
#
# Also proves the refusals that make an update channel safe to switch on:
#   * a TAMPERED manifest (digest changed after signing) is rejected, binary untouched
#   * without a pinned public key the worker refuses to self-update at all
#   * an opted-OUT node is never offered an update in the first place
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8091}"
FILE_PORT="${CBK_FILE_PORT:-8092}"
MODEL_PORT="${CBK_MODEL_PORT:-11448}"
URL="http://127.0.0.1:$PORT"
KEYDIR="$(mktemp -d)"; WORKDIR="$(mktemp -d)"; DIST="$WORKDIR/dist"; INSTALL="$WORKDIR/bin"
STATE="$WORKDIR/node.json"
RID="$(dotnet --list-runtimes >/dev/null 2>&1 && echo osx-arm64)"
case "$(uname -s)/$(uname -m)" in
  Darwin/arm64) RID=osx-arm64 ;; Darwin/x86_64) RID=osx-x64 ;;
  Linux/aarch64) RID=linux-arm64 ;; Linux/x86_64) RID=linux-x64 ;;
esac
PIDS=()
sha256(){ if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d" " -f1;
          else shasum -a 256 "$1" | cut -d" " -f1; fi; }
log(){ printf '\033[36m[selfupd]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[selfupd] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
           pkill -f "$INSTALL/cbk" 2>/dev/null || true; rm -rf "$WORKDIR" "$KEYDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 60); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.25; done; fail "$2 not ready"; }
jqpy(){ python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }
node_field(){ curl -fsS "$URL/nodes" | python3 -c "
import sys,json
n=next((n for n in json.load(sys.stdin)['nodes'] if n['node_id']=='$1'),{})
print(n.get('$2') or '')"; }

mkdir -p "$DIST" "$INSTALL"
log "target RID: $RID"

# --- operator signing key (never committed; generated per run) ----------------------------
"$REPO/server/.venv/bin/python" - <<PY
from clusterbuck import signing
k = signing.generate_keypair()
open("$KEYDIR/sign.key.pem","w").write(signing.private_pem(k))
open("$KEYDIR/sign.pub.pem","w").write(signing.public_pem(k))
PY
log "generated an ephemeral update-signing keypair"

# --- publish two real single-file binaries -------------------------------------------------
publish(){   # $1 = version, $2 = output dir
  ( cd "$REPO/worker/dotnet" && dotnet publish src/Clusterbuck.Worker -c Release -r "$RID" \
      -p:PublishSingleFile=true -p:SelfContained=true -p:PublishAot=false \
      -p:Version="$1" -o "$2" --nologo >/dev/null 2>&1 )
  [[ -f "$2/cbk" ]] || fail "publish of $1 produced no binary"
}
log "publishing 0.7.0 and 0.9.9 (this takes a minute)…"
publish 0.7.0 "$WORKDIR/v070"
publish 0.9.9 "$WORKDIR/v099"
cp "$WORKDIR/v070/cbk" "$INSTALL/cbk"           # the "installed" agent
cp "$WORKDIR/v099/cbk" "$DIST/cbk-0.9.9"        # what the update channel serves
SHA=$(sha256 "$DIST/cbk-0.9.9")
log "published; 0.9.9 digest ${SHA:0:16}…"

# --- static file server for the artifact --------------------------------------------------
( cd "$DIST" && exec python3 -m http.server "$FILE_PORT" >/dev/null 2>&1 ) & PIDS+=($!)
wait_for "http://127.0.0.1:$FILE_PORT/cbk-0.9.9" "artifact host"

cat > "$WORKDIR/release.json" <<JSON
{"version":"0.9.9","channel":"stable","protocol_version":1,
 "artifacts":{"$RID":{"url":"http://127.0.0.1:$FILE_PORT/cbk-0.9.9","sha256":"$SHA"}}}
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
  "$INSTALL/cbk" enroll --token "$TOKEN" --server "$URL" --state "$STATE" >/dev/null
NODE=$(python3 -c "import json;print(json.load(open('$STATE'))['node_id'])")

run_worker(){   # $1 = extra env
  env CBK_NODE_STATE="$STATE" CBK_REDIS_URL="redis://localhost:6379/0" \
    CBK_MODEL_SERVER_URL="http://127.0.0.1:$MODEL_PORT/v1" CBK_MODEL="fake" \
    CBK_HEARTBEAT_MS=500 CBK_LADDER_HYSTERESIS_S=1 $1 \
    "$INSTALL/cbk" work >>"$WORKDIR/worker.log" 2>&1 &
  WORKER_PID=$!; PIDS+=($WORKER_PID)
}
stop_worker(){ kill "$WORKER_PID" 2>/dev/null || true; pkill -f "$INSTALL/cbk work" 2>/dev/null || true; sleep 1; }

# --- 1. opted OUT ⇒ never offered an update ----------------------------------------------
run_worker "CBK_UPDATE_PUBKEY=$KEYDIR/sign.pub.pem"
for _ in $(seq 1 40); do [[ "$(node_field "$NODE" fitness)" == "stale" ]] && break; sleep 0.25; done
[[ "$(node_field "$NODE" agent_version)" == "0.7.0" ]] || fail "expected 0.7.0 installed"
sleep 2
grep -q "installed 0.9.9" "$WORKDIR/worker.log" && fail "updated without opting in!"
log "opted out: flagged stale, but NOT updated ✓"
stop_worker

# --- 2. tampered manifest ⇒ refused, binary untouched ------------------------------------
python3 - "$WORKDIR/release.json" <<'PY'
import json,sys
p=sys.argv[1]; d=json.load(open(p))
rid=next(iter(d["artifacts"])); d["artifacts"][rid]["sha256"]="0"*64   # lie about the digest
json.dump(d,open(p,"w"))
PY
curl -fsS -X POST "$URL/nodes/$NODE/policy" -H 'content-type: application/json' \
  -d '{"auto_update": true}' >/dev/null
: > "$WORKDIR/worker.log"
run_worker "CBK_UPDATE_PUBKEY=$KEYDIR/sign.pub.pem"
sleep 4
grep -qE "digest mismatch|update Refused" "$WORKDIR/worker.log" \
  || fail "tampered digest was not refused (log: $(tail -3 "$WORKDIR/worker.log"))"
[[ "$(node_field "$NODE" agent_version)" == "0.7.0" ]] || fail "binary changed despite tamper"
log "tampered digest refused; binary untouched ✓"
stop_worker

# Restore the honest digest.
python3 - "$WORKDIR/release.json" "$SHA" <<'PY'
import json,sys
p,sha=sys.argv[1],sys.argv[2]; d=json.load(open(p))
d["artifacts"][next(iter(d["artifacts"]))]["sha256"]=sha
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
[[ -f "$INSTALL/cbk.prev" ]] || fail "previous binary was not retained for rollback"
log "verified → fetched → swapped → re-exec'd; now reporting $NEW ✓"
log "previous binary retained at cbk.prev ($(sha256 "$INSTALL/cbk.prev" | cut -c1-16)…) ✓"
[[ "$(node_field "$NODE" fitness)" == "ok" ]] || fail "fitness not ok after updating"
log "coordinator now judges it fit ✓"

printf '\033[32m[selfupd] PASS — signed self-update applied; tamper and unsigned paths refused\033[0m\n'
