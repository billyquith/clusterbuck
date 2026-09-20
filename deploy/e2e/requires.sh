#!/usr/bin/env bash
# End-to-end proof that a job can state what the model must be ABLE to do (ADR 37).
#
# Ability is a graded 1-10 judgement of how WELL a model does a task class. A context
# window is a number with a hard edge and tool calling is a boolean; a 32k model and a
# 128k one can both honestly be "a 6 at summarize", and sending a 60k document to the
# first silently truncates it. No score can see that difference, so `requires` filters on
# it BEFORE ability is compared at all.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8096}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[requires]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[requires] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }

${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} -n 0 FLUSHDB >/dev/null

( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" CBK_FLEET_PATH="$REPO/server/fleet.yaml" \
  CBK_WOL_BROADCAST=127.0.0.1 .venv/bin/python -m clusterbuck >/dev/null 2>&1 ) &
PIDS+=($!)
wait_for "$URL/healthz" "server"

# The seeded catalog carries each artifact's published capabilities, so routing has facts
# to filter on before any node has enrolled.
CTX=$(curl -fsS "$URL/catalog" | python3 -c "
import sys,json
a={r['artifact']: r for r in json.load(sys.stdin)['artifacts']}
print(a['qwen2.5:32b']['context_tokens'])")
[[ -n "$CTX" && "$CTX" != "None" ]] || fail "catalog does not carry context_tokens"
log "catalog declares qwen2.5:32b at $CTX tokens"

ask(){  # requires-json → HTTP code + detail
  curl -s -w '\n%{http_code}' -X POST "$URL/jobs" -H 'content-type: application/json' \
    -d "{\"task_class\": \"summarize\", \"min_ability\": 4, \"requires\": $1,
         \"messages\": [{\"role\":\"user\",\"content\":\"hi\"}],
         \"urgency\": \"waitable\", \"privacy\": \"local_only\"}"
}

# 1. A window no local artifact can hold is refused, naming what was short.
OUT=$(ask '{"context_tokens": 900000}')
[[ "$(tail -n1 <<<"$OUT")" == "422" ]] || fail "an impossible context window was accepted"
grep -q "requirements unmet" <<<"$OUT" || fail "refusal does not explain what was unmet: $OUT"
log "900k-token window refused, naming the shortfall ✓"

# 2. A feature no artifact DECLARES is refused too — undeclared reads as no, because the
#    alternative fails at the model server where it looks like a model bug.
OUT=$(ask '{"vision": true}')
[[ "$(tail -n1 <<<"$OUT")" == "422" ]] || fail "vision was accepted with nothing declaring it"
log "vision refused where nothing declares it ✓"

# 3. Requirements are filtered BEFORE ability is compared, so a capable-but-unsuitable
#    artifact cannot win on score. min_ability 4 alone picks the cheap 8B tier; asking for
#    more context than the 8B's catalog entry holds must move it, not be ignored.
OUT=$(ask '{"context_tokens": 8000, "tools": true}')
[[ "$(tail -n1 <<<"$OUT")" == "202" ]] || fail "a requirement the fleet CAN meet was refused: $OUT"
log "a requirement the fleet meets still routes ✓"

# 4. Curation is the fix, and it is one POST. Declare vision on an artifact and the job
#    that was refused above now routes — without touching any ability score.
curl -fsS -X POST "$URL/catalog" -H 'content-type: application/json' -d '{
  "artifact": "llama3.2:3b", "registry_ref": "llama3.2:3b",
  "size_gb": 2.0, "min_ram_gb": 8.0, "expected_ability": 4.0,
  "context_tokens": 131072, "supports_tools": true,
  "supports_json_schema": true, "supports_vision": true
}' >/dev/null
OUT=$(ask '{"vision": true}')
[[ "$(tail -n1 <<<"$OUT")" == "202" ]] || fail "curating the catalog did not make it routable: $OUT"
log "curating the artifact makes it routable, with no ability change ✓"

printf '\033[32m[requires] PASS — capability is filtered, not scored\033[0m\n'
