#!/usr/bin/env bash
# Shared harness for the e2e scripts. Source it after setting E2E_NAME.
#
# Every script here grew its own copy of the same eight lines: a REPO path, a temp
# WORKDIR, a PIDS array, log/fail, a cleanup trap, wait_for, jqpy, and the Redis flush.
# `${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli}` alone was written out 21 times in
# two different calling conventions. That is not just noise — it is 21 places to edit when
# CI needs to reach Redis differently, and commit 51f1790 had to fix exactly that class of
# bug (a leaked node identity) in worker.sh, noting it was "defaulted in worker.sh rather
# than in each script". This generalises that lesson to the rest of the preamble.
#
# Usage:
#   E2E_NAME=cloud
#   source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
#
# Provides: REPO WORKDIR PIDS  log fail pass  wait_for jqpy redis_cli flush_redis
# and installs the EXIT trap that kills PIDS and removes WORKDIR.

: "${E2E_NAME:?set E2E_NAME before sourcing lib.sh}"

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORKDIR="$(mktemp -d)"
PIDS=()

log()  { printf '\033[36m[%s]\033[0m %s\n' "$E2E_NAME" "$*"; }
fail() { printf '\033[31m[%s] FAIL:\033[0m %s\n' "$E2E_NAME" "$*" >&2; exit 1; }
pass() { printf '\033[32m[%s] PASS — %s\033[0m\n' "$E2E_NAME" "$*"; }

cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORKDIR"; }
trap cleanup EXIT

# url, human-name, [tries] — tries defaults to 50 (~10s at 0.2s apart).
wait_for() {
  local tries="${3:-50}"
  for _ in $(seq 1 "$tries"); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done
  fail "$2 not ready"
}

# Read JSON on stdin and print one expression off it, e.g. jqpy '["id"]'.
jqpy() { python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

# THE one definition. CI sets CBK_REDIS_CLI because a GitHub service container has no
# `docker exec` to reach; everything else uses the dev container.
redis_cli()   { ${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} "$@"; }
flush_redis() { redis_cli -n 0 FLUSHDB >/dev/null; }
