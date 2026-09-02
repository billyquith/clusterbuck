#!/usr/bin/env bash
# Prove an opt-in Idempotency-Key makes POST /jobs safe to retry (protocols.md §1b).
#
# The gap: a client that loses the response to a submit cannot tell "submitted" from
# "not submitted", so it either risks a duplicate job or risks losing the work. Asserted
# here over real HTTP against real Redis: a repeat returns the SAME job, flagged as a
# replay, and the work is queued exactly once.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${CBK_PORT:-8094}"
URL="http://127.0.0.1:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()
log(){ printf '\033[36m[idem]\033[0m %s\n' "$*"; }
fail(){ printf '\033[31m[idem] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT
wait_for(){ for _ in $(seq 1 50); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; fail "$2 not ready"; }
REDIS_CLI=(${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli})

"${REDIS_CLI[@]}" -n 0 FLUSHDB >/dev/null

( cd "$REPO/server" && exec env CBK_REDIS_URL="${CBK_REDIS_URL:-redis://localhost:6379/0}" \
  CBK_DB_PATH="$WORKDIR/cbk.db" CBK_PORT="$PORT" .venv/bin/python -m clusterbuck ) & PIDS+=($!)
wait_for "$URL/healthz" "server"
log "server up (no worker needed — this is all submit-side)"

BODY='{"capability":"8b-extract","messages":[{"role":"user","content":"retry me"}],"urgency":"waitable","privacy":"local_only"}'

post(){  # $1 = key ("" for none); prints "<code> <id>"
  local hdr=()
  [[ -n "$1" ]] && hdr=(-H "Idempotency-Key: $1")
  curl -sS -X POST "$URL/jobs" -H 'content-type: application/json' "${hdr[@]}" \
    -d "$BODY" -o "$WORKDIR/out.json" -D "$WORKDIR/hdr.txt" -w '%{http_code}' \
    | tr -d '\n'
  printf ' '
  python3 -c 'import sys,json;print(json.load(open(sys.argv[1])).get("id",""))' "$WORKDIR/out.json"
}

read -r CODE1 ID1 <<<"$(post "e2e-key-1")"
[[ "$CODE1" == "202" ]] || fail "first submit should be 202, got $CODE1"
grep -qi '^idempotency-replayed:' "$WORKDIR/hdr.txt" && fail "first submit must not be flagged a replay"
log "first submit: 202 ${ID1:0:16}…  ✓"

read -r CODE2 ID2 <<<"$(post "e2e-key-1")"
[[ "$CODE2" == "200" ]] || fail "repeat should be 200 (nothing newly accepted), got $CODE2"
[[ "$ID2" == "$ID1" ]] || fail "repeat returned a different job: $ID2 != $ID1"
grep -qi '^idempotency-replayed: *true' "$WORKDIR/hdr.txt" || fail "repeat missing Idempotency-Replayed"
log "repeat: 200, same job, flagged as a replay  ✓"

DEPTH=$("${REDIS_CLI[@]}" -n 0 XLEN q:8b-extract | tr -d '\r')
[[ "$DEPTH" == "1" ]] || fail "the work must be queued exactly once, XLEN=$DEPTH"
log "queued exactly once (XLEN=1)  ✓ — the duplicate this prevents"

read -r CODE3 ID3 <<<"$(post "e2e-key-2")"
[[ "$CODE3" == "202" && "$ID3" != "$ID1" ]] || fail "a different key must make a new job"
log "different key → different job  ✓"

read -r CODE4 ID4 <<<"$(post "")"
[[ "$CODE4" == "202" ]] || fail "an unkeyed submit must keep working, got $CODE4"
read -r CODE5 ID5 <<<"$(post "")"
[[ "$CODE5" == "202" && "$ID5" != "$ID4" ]] || fail "unkeyed submits must not collide"
log "no key → unchanged behaviour (NULLs are distinct)  ✓"

# An oversized key rather than an empty one: curl DROPS a header whose value is only
# whitespace, so the empty case cannot be exercised from here at all (it is covered in
# server/tests/test_idempotency.py, which builds the request directly).
BIG=$(printf 'k%.0s' $(seq 1 300))
CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$URL/jobs" \
  -H 'content-type: application/json' -H "Idempotency-Key: $BIG" -d "$BODY")
[[ "$CODE" == "400" ]] || fail "an oversized key should be 400, got $CODE"
log "oversized key → 400  ✓"

printf '\033[32m[idem] PASS — a lost submit response is now safely retryable\033[0m\n'
