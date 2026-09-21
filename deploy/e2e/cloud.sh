#!/usr/bin/env bash
# End-to-end proof of the remit's SECOND clause: when the local fleet cannot serve a job,
# it is promoted to a cloud provider — executed by the coordinator itself (never by a
# worker, ADR 30), and metered as cloud spend.
#
# This had no end-to-end coverage at all. `cloud_ok` appeared in no e2e script, and the
# only test proving cloud selection injected a fictional ability by hand, so
# cloud_executor.py + budget.py + the provider entries in fleet.yaml were exercised by
# unit tests alone — half of what the system is for.
#
# The "provider" is the zero-weight fake model server reached through LiteLLM's
# OPENAI_BASE_URL, so this runs offline and spends nothing.
#
# Proves, in order:
#   1. privacy=local_only  -> refused, at any urgency (ADR 14: never leaves the LAN)
#   2. urgency=waitable    -> refused even with cloud_ok (ADR 18: no wake, no cloud)
#   3. cloud_ok + necessary-> executed by the coordinator, completed, metered as cloud
#   4. no worker was ever involved
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8101}"
FAKE_PORT="${CBK_FAKE_PORT:-8102}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[cloud]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[cloud] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
jqpy(){ python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null

# A cloud-ONLY fleet: no local capability exists, which is the situation the cloud tier is
# for. `model_server` is absent — that is what marks a capability as having no host node.
cat > "$WORKDIR/fleet.yaml" <<YAML
nodes: []
capabilities:
  frontier:
    cloud: true
    model: "openai/fake-frontier"
    api_key_env: CBK_FAKE_CLOUD_KEY
    price_in_per_1k: 0.003
    price_out_per_1k: 0.015
YAML

( exec python3 "$REPO/server/tools/fake_model_server.py" --port "$FAKE_PORT" >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "http://127.0.0.1:$FAKE_PORT/v1/models" "fake provider"

( cd "$REPO/server" && exec env \
  CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_FLEET_PATH="$WORKDIR/fleet.yaml" \
  CBK_CLOUD_BUDGET_MONTHLY=10 \
  CBK_FAKE_CLOUD_KEY=sk-e2e-not-a-real-key \
  OPENAI_BASE_URL="http://127.0.0.1:$FAKE_PORT/v1" \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"
log "coordinator up with a cloud-only fleet; provider stubbed on :$FAKE_PORT"

submit(){  # privacy, urgency  -> prints HTTP status and body to $WORKDIR/last.json
  curl -s -o "$WORKDIR/last.json" -w '%{http_code}' -X POST "$URL/jobs" \
    -H 'Content-Type: application/json' -d "{
      \"capability\": \"frontier\", \"prompt\": \"say hello\",
      \"privacy\": \"$1\", \"urgency\": \"$2\"
    }"
}

# --- 1. privacy=local_only is absolute, whatever the urgency (ADR 14) -------------------
got=$(submit local_only urgent)
[[ "$got" == "422" ]] || fail "local_only+urgent returned $got, expected 422"
grep -q "privacy" "$WORKDIR/last.json" || fail "the refusal must name privacy as the reason"
log "local_only refused even at urgent — privacy bounds cloud at every urgency ✓"

# --- 2. waitable never creates capacity for itself (ADR 18) -----------------------------
got=$(submit cloud_ok waitable)
[[ "$got" == "422" ]] || fail "cloud_ok+waitable returned $got, expected 422"
grep -q "waitable" "$WORKDIR/last.json" || fail "the refusal must name the urgency rule"
log "cloud_ok+waitable refused — no wake, no cloud, no demand ✓"

# --- 3. the job the cloud tier exists for ----------------------------------------------
got=$(submit cloud_ok necessary)
[[ "$got" == "202" ]] || fail "cloud_ok+necessary returned $got, expected 202: $(cat "$WORKDIR/last.json")"
JOB=$(jqpy "['id']" < "$WORKDIR/last.json")
log "accepted $JOB for the cloud tier"

for _ in $(seq 1 60); do
  STATUS=$(curl -fsS "$URL/jobs/$JOB" | jqpy "['status']")
  [[ "$STATUS" == "done" || "$STATUS" == "failed" ]] && break
  sleep 0.5
done
[[ "$STATUS" == "done" ]] || fail "job ended '$STATUS', expected done: $(curl -fsS "$URL/jobs/$JOB")"
TEXT=$(curl -fsS "$URL/jobs/$JOB" | python3 -c "
import sys, json
r = json.load(sys.stdin)['result']
print(r['choices'][0]['message']['content'] if isinstance(r, dict) else (r or ''))")
[[ -n "$TEXT" ]] || fail "completed with no completion text"
log "served by the coordinator's own executor: ${TEXT:0:48} ✓"

# --- 4. metered as cloud, and no worker was involved ------------------------------------
# The usage scan is a coordinator tick (~10s), so the figure lands after the job does.
for _ in $(seq 1 40); do
  CLOUD_SPEND=$(curl -fsS "$URL/usage" | jqpy "['headline']['cloud_spend']")
  python3 -c "import sys; sys.exit(0 if float('$CLOUD_SPEND') > 0 else 1)" && break
  sleep 0.5
done
BUDGET_USED=$(curl -fsS "$URL/usage" | jqpy "['budget']['cloud_spent_this_month']")
ENFORCED=$(curl -fsS "$URL/usage" | jqpy "['budget']['enforced']")
python3 -c "import sys; sys.exit(0 if float('$CLOUD_SPEND') > 0 else 1)" \
  || fail "usage shows no cloud spend; the job was not metered to the cloud venue"
python3 -c "import sys; sys.exit(0 if float('$BUDGET_USED') > 0 else 1)" \
  || fail "the cloud budget did not record the spend"
[[ "$ENFORCED" == "True" ]] || fail "budget reports enforced=$ENFORCED; ADR 30 says it gates routing"

# The executor names itself here rather than leaving it null, which is the stronger
# signal: the row positively records that the coordinator ran this, not some node that
# happened to be holding the provider key.
WORKER=$(curl -fsS "$URL/jobs/$JOB" | jqpy "['worker']")
[[ "$WORKER" == cloud:* ]] || fail "expected a cloud: executor, got '$WORKER' — ADR 30 says no worker ever holds the key"
log "metered \$$CLOUD_SPEND against the budget; executed by '$WORKER', never a worker ✓"

printf '\033[32m[cloud] PASS — local_only and waitable refused; cloud_ok+necessary served and metered\033[0m\n'
