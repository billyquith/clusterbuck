#!/usr/bin/env bash
# The client's view: examples/client/use_cases.py, run against real processes.
#
# Every other script here proves a coordinator property from the inside. This one asks the
# question a client developer asks: given only HTTP, can I tell what happened and what to
# do next? Each case is a generic shape of work, and asserts only what a client can see.
#
# Cases the coordinator does not satisfy YET are listed in PENDING. They must fail on an
# expectation (exit 2), never on a harness error — and a pending case that PASSES fails the
# run too, so a fix cannot land without the case being promoted with it.
#
# The fleet this sets up (use_cases.py documents it from the client side):
#   live-extract    fake model server                   + worker
#   dead-extract    a closed port                       + worker
#   broken-extract  fake model server, every chat 500s  + worker
#   idle-extract    fake model server                   no worker
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_NAME=client
# shellcheck source=lib.sh
source "$REPO/deploy/e2e/lib.sh"
# shellcheck source=worker.sh
source "$REPO/deploy/e2e/worker.sh"
worker_assert_ready
PORT="${CBK_PORT:-8111}"
URL="http://127.0.0.1:$PORT"
KEY="client-e2e-key"
REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}"
LIVE=11471 BROKEN=11472 DEAD=11473   # nothing listens on $DEAD

PENDING=(
  sync-structured discovery-health
  worker-server-dead worker-server-error
)

flush_redis

python3 "$REPO/server/tools/fake_model_server.py" --port "$LIVE" & PIDS+=($!)
python3 "$REPO/server/tools/fake_model_server.py" --port "$BROKEN" --fail-chat 500 & PIDS+=($!)
wait_for "http://127.0.0.1:$LIVE/healthz" "live model server"
wait_for "http://127.0.0.1:$BROKEN/healthz" "broken model server"

cat > "$WORKDIR/fleet.yaml" <<YAML
nodes: []
capabilities:
  live-extract:   {queue: "q:live-extract",   model: fake, model_server: "http://127.0.0.1:$LIVE/v1"}
  dead-extract:   {queue: "q:dead-extract",   model: fake, model_server: "http://127.0.0.1:$DEAD/v1"}
  broken-extract: {queue: "q:broken-extract", model: fake, model_server: "http://127.0.0.1:$BROKEN/v1"}
  idle-extract:   {queue: "q:idle-extract",   model: fake, model_server: "http://127.0.0.1:$LIVE/v1"}
YAML

# Fast ticks and a short orphan grace, so expiry and the orphan sweep answer within a
# case's patience. Grace must exceed the reaper's min-idle (config.validate()).
( cd "$REPO/server" && exec env CBK_REDIS_URL="$REDIS_URL" CBK_DB_PATH="$WORKDIR/cbk.db" \
  CBK_PORT="$PORT" CBK_FLEET_PATH="$WORKDIR/fleet.yaml" CBK_API_KEY="$KEY" \
  CBK_ESCALATION_INTERVAL_S=0.5 CBK_ORPHAN_GRACE_S=3 CBK_REAPER_MIN_IDLE_MS=2000 \
  .venv/bin/python -m clusterbuck >"$WORKDIR/server.log" 2>&1 ) & PIDS+=($!)
wait_for "$URL/healthz" "server"

# The fake model declares structured output, so `requires.json_schema` can route to it.
curl -fsS -X POST "$URL/catalog" -H "Authorization: Bearer $KEY" \
  -H 'content-type: application/json' \
  -d '{"artifact":"fake","registry_ref":"fake","size_gb":0,"min_ram_gb":0,
       "supports_json_schema":true}' >/dev/null

start_worker() {  # capability model-server-port
  CBK_REDIS_URL="$REDIS_URL" CBK_MODEL_SERVER_URL="http://127.0.0.1:$2/v1" CBK_MODEL=fake \
    CBK_CAPABILITIES="$1" CBK_WORKER_ID="node-$1" CBK_NODE_STATE="$WORKDIR/$1.json" \
    cbk_worker_bg work >"$WORKDIR/worker-$1.log" 2>&1 & PIDS+=($!)
}
start_worker live-extract "$LIVE"
start_worker dead-extract "$DEAD"
start_worker broken-extract "$BROKEN"
log "coordinator, model servers and three workers up"

is_pending() { local c; for c in "${PENDING[@]}"; do [[ "$c" == "$1" ]] && return 0; done; return 1; }

CASES=$(cd "$REPO/examples/client" && "$REPO/server/.venv/bin/python" - <<'PY'
from use_cases import CASES
print(" ".join(CASES))
PY
)

broken=() early=() held=0 promised=0
for case in $CASES; do
  set +e
  out=$(cd "$REPO/examples/client" && "$REPO/server/.venv/bin/python" use_cases.py \
    --server "$URL" --api-key "$KEY" --redis-url "$REDIS_URL" "$case" 2>&1)
  rc=$?
  set -e
  if is_pending "$case"; then
    case $rc in
      2) log "pending  $case — ${out##*$'\n'}"; promised=$((promised + 1)) ;;
      0) early+=("$case") ;;
      *) broken+=("$case: $out") ;;
    esac
  else
    if [[ $rc == 0 ]]; then log "ok       $case"; held=$((held + 1)); else broken+=("$case: $out"); fi
  fi
done

((${#early[@]} == 0)) || fail "pending case(s) now pass — promote them out of PENDING: ${early[*]}"
if ((${#broken[@]})); then
  printf '%s\n' "${broken[@]}" >&2
  fail "${#broken[@]} case(s) broke (see above; server log: $WORKDIR/server.log)"
fi
pass "$held case(s) hold for a client; $promised pending on server work"
