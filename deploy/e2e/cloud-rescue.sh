#!/usr/bin/env bash
# End-to-end proof that a `cloud_ok` job addressed to a LOCAL tier still gets answered
# when no machine serves that tier (design.md §8) — by submit-time fallback when the fleet
# is dark, and by the rescue sweep when a machine was expected and never came.
#
# cloud.sh proves a job addressed to a cloud tier runs in the cloud. This proves the other
# half of the remit: local first, and the cloud only when local cannot. No worker is ever
# started, so the only way any job here completes is the coordinator's cloud executor.
#
# The "provider" is the zero-weight fake model server reached through LiteLLM's
# OPENAI_BASE_URL — operator environment, never job params — so this runs offline.
#
# Proves, in order:
#   1. dark tier (no node at all)       + cloud_ok  -> sent to the cloud at submit
#   2. wakeable tier that never wakes   + cloud_ok  -> queued locally, then rescued
#   3. the same tier                    + local_only -> never leaves; expires on its deadline
#   4. rescued work is metered as cloud spend, not as avoided spend
#   5. a rescue interrupted between its two steps is undone by the orphan sweep
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_NAME=cloud-rescue
# shellcheck source=lib.sh
source "$REPO/deploy/e2e/lib.sh"
PORT="${CBK_PORT:-8103}"
FAKE_PORT="${CBK_FAKE_PORT:-8104}"
URL="http://127.0.0.1:$PORT"

flush_redis

# `dark` has no node anywhere: nothing reads it and nothing can be woken, so it is
# unavailable at submit. `asleep` has a node with a MAC, so it counts as servable (a wake
# is owed) — its jobs queue locally and only the rescue sweep can move them. The magic
# packet goes to loopback (CBK_WOL_BROADCAST) and wakes nothing, which is the point.
cat > "$WORKDIR/fleet.yaml" <<YAML
nodes:
  - id: sleeper
    mac: "02:00:00:00:00:01"
    capabilities: [asleep]
capabilities:
  dark:
    model_server: "http://127.0.0.1:9/v1"
    model: "local-dark"
    price_in_per_1k: 0.0002
    price_out_per_1k: 0.0006
    cloud_fallback: frontier
  asleep:
    model_server: "http://127.0.0.1:9/v1"
    model: "local-asleep"
    price_in_per_1k: 0.0002
    price_out_per_1k: 0.0006
    cloud_fallback: frontier
  frontier:
    cloud: true
    model: "openai/fake-frontier"
    api_key_env: CBK_FAKE_CLOUD_KEY
    price_in_per_1k: 0.003
    price_out_per_1k: 0.015
YAML

wait_port_free "$FAKE_PORT"
( exec python3 "$REPO/server/tools/fake_model_server.py" --port "$FAKE_PORT" >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "http://127.0.0.1:$FAKE_PORT/v1/models" "fake provider"

wait_port_free "$PORT"
( cd "$REPO/server" && exec env \
  CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" \
  CBK_FLEET_PATH="$WORKDIR/fleet.yaml" \
  CBK_CLOUD_BUDGET_MONTHLY=10 \
  CBK_CLOUD_RESCUE_LEAD_S=120 \
  CBK_ESCALATION_INTERVAL_S=1 \
  CBK_REAPER_MIN_IDLE_MS=1000 CBK_ORPHAN_GRACE_S=2 \
  CBK_FAKE_CLOUD_KEY=sk-e2e-not-a-real-key \
  OPENAI_BASE_URL="http://127.0.0.1:$FAKE_PORT/v1" \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"
log "coordinator up; no worker will ever start; provider stubbed on :$FAKE_PORT"

submit() {  # capability, privacy, deadline-seconds -> job id
  local body
  body=$(curl -fsS -X POST "$URL/jobs" -H 'Content-Type: application/json' -d "{
    \"capability\": \"$1\", \"prompt\": \"say hello\", \"privacy\": \"$2\",
    \"urgency\": \"necessary\", \"deadline\": \"$(deadline_in "$3")\"}")
  jqpy "['id']" <<<"$body"
}

row() { db_row "$WORKDIR/cbk.db" "$1"; }

# --- 1. a dark tier falls back at submit -------------------------------------------------
J1=$(submit dark cloud_ok 300)
CAP=$(row "SELECT capability FROM jobs WHERE id='$J1'")
[[ "$CAP" == "frontier" ]] || fail "dark-tier job routed to '$CAP', expected the cloud fallback"
[[ "$(wait_terminal "$URL" "$J1")" == "done" ]] || fail "dark-tier job: $(job_verdict "$URL" "$J1")"
log "a tier no machine can serve sent its cloud_ok job to the cloud at submit ✓"

# --- 2. a tier expected to wake, that never does, is rescued -----------------------------
J2=$(submit asleep cloud_ok 60)
CAP=$(row "SELECT capability FROM jobs WHERE id='$J2'")
[[ "$CAP" == "asleep" ]] || fail "wakeable-tier job routed to '$CAP' at submit; it should wait for the wake"
[[ "$(wait_terminal "$URL" "$J2")" == "done" ]] || fail "rescue: $(job_verdict "$URL" "$J2")"
R=$(row "SELECT capability, rescued_from FROM jobs WHERE id='$J2'")
[[ "$R" == "frontier|asleep" ]] || fail "rescued row reads '$R', expected frontier|asleep"
log "queued locally, nothing came, rescued to the cloud inside its deadline ✓"

# --- 3. local_only never leaves, even with the same tier and the same deadline -----------
J3=$(submit asleep local_only 4)
[[ "$(wait_terminal "$URL" "$J3")" == "expired" ]] || fail "local_only: $(job_verdict "$URL" "$J3")"
R=$(row "SELECT capability, rescued_from FROM jobs WHERE id='$J3'")
[[ "$R" == "asleep|" ]] || fail "local_only row reads '$R' — it must never be moved"
log "local_only waited on its own tier and expired rather than leave the LAN ✓"

# --- 4. metered as cloud spend, at a price that says where it came from ------------------
for _ in $(seq 1 40); do
  U=$(row "SELECT venue, node, cost_source, cost > 0 FROM usage WHERE job_id='$J2'")
  [[ -n "$U" ]] && break
  sleep 0.5
done
# The fake model is not in LiteLLM's price map, so the fleet's rate prices it — and the
# row says so, rather than passing an estimate off as a quote.
[[ "$U" == "cloud|cloud:openai|fleet|1" ]] \
  || fail "rescued job metered as '$U', expected cloud|cloud:openai|fleet|1"
log "rescued work counted as cloud spend against the budget, not as avoided spend ✓"

# --- 5. a rescue that died half-way is reconciled, not lost ------------------------------
# rescue.py rewrites the row (capability -> the cloud tier, rescued_from, delivery cleared)
# BEFORE it moves the entry. A coordinator killed between the two leaves exactly the row
# planted below, with the entry still on the local stream. The orphan sweep must find it
# there and put the row back, or the job is failed as an orphan while its entry still sits
# on the stream — or worse, is later served locally and metered as cloud.
J5=$(submit asleep cloud_ok 3600)   # far deadline: the sweep itself never rescues this one
python3 -c "
import sqlite3, sys
with sqlite3.connect(sys.argv[1]) as c:  # commits on exit; db_row is read-only
    c.execute(\"UPDATE jobs SET capability='frontier', rescued_from='asleep', \"
              \"entry_id=NULL, stream=NULL, est_cost=1.0 WHERE id=?\", (sys.argv[2],))
" "$WORKDIR/cbk.db" "$J5"
# Proof the plant took — without it, the wait below passes on an untouched row.
R=$(row "SELECT capability, rescued_from FROM jobs WHERE id='$J5'")
[[ "$R" == "frontier|asleep" ]] || fail "could not plant the half-done rescue: '$R'"
for _ in $(seq 1 60); do
  R=$(row "SELECT capability, rescued_from, stream FROM jobs WHERE id='$J5'")
  [[ "$R" == "asleep||q:asleep"* ]] && break
  sleep 0.5
done
[[ "$R" == "asleep||q:asleep"* ]] || fail "half-done rescue not reconciled: row reads '$R'"
STATUS=$(curl -fsS "$URL/jobs/$J5" | jqpy "['status']")
[[ "$STATUS" == "queued" ]] || fail "the reconciled job should still be queued, is '$STATUS'"
log "an interrupted rescue was found on its old stream and undone, job still queued ✓"

pass "dark tier fell back at submit; an unwoken tier was rescued; local_only stayed home; an interrupted rescue was undone"
