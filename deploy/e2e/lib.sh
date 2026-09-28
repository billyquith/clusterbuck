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
# Provides: REPO WORKDIR PIDS  log fail pass  wait_for wait_port_free stop_pid jqpy job_verdict
#           wait_terminal deadline_in db_row  redis_cli flush_redis
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

# port, [tries] — wait until nothing is listening on 127.0.0.1:port, or FAIL.
#
# Call it before starting a server on a fixed port. `wait_for` only asks whether SOMETHING
# answers on the port, so a server that lost the bind — to a previous run's coordinator
# still shutting down, or to another session running the same script — exits quietly and
# the whole test then runs against the stranger. version.sh failed that way at "expected
# fitness ok with no policy": its checks were answered by a coordinator with a different
# version policy. Refusing to start is the honest outcome, since nothing proven against
# someone else's server is proven about ours.
wait_port_free() {
  local tries="${2:-50}"
  for _ in $(seq 1 "$tries"); do
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null || return 0
    sleep 0.2
  done
  fail "port $1 is already in use — another e2e run, or a server that never stopped?"
}

# pid, [tries] — SIGTERM, wait until the process has actually gone, SIGKILL if it will not.
# A fixed sleep after `kill` is a guess about shutdown time; this is not.
stop_pid() {
  local tries="${2:-50}"
  kill "$1" 2>/dev/null || return 0
  for _ in $(seq 1 "$tries"); do kill -0 "$1" 2>/dev/null || return 0; sleep 0.2; done
  kill -9 "$1" 2>/dev/null || true
}

# Read JSON on stdin and print one expression off it, e.g. jqpy '["id"]'.
jqpy() { python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

# base-url, job-id — one line: `status [worker] [error]`. For a failure message, so a job
# that ended up somewhere it should not have says WHO took it and why it stopped, rather
# than just the status, which on its own cannot tell a leaked consumer from the one under test.
job_verdict() {
  curl -fsS "$1/jobs/$2" | python3 -c "
import sys,json
j=json.load(sys.stdin)
print(' '.join(str(x) for x in (j['status'], j.get('worker'), j.get('error')) if x))"
}

# base-url, job-id, [tries] — poll until the job is terminal; print its final status.
# tries defaults to 60 (~30s at 0.5s apart). Prints the last status seen if it never is.
wait_terminal() {
  local status="" tries="${3:-60}"
  for _ in $(seq 1 "$tries"); do
    status=$(curl -fsS "$1/jobs/$2" | jqpy "['status']")
    case "$status" in done|failed|expired|cancelled) break ;; esac
    sleep 0.5
  done
  echo "$status"
}

# seconds — an RFC 3339 UTC timestamp that far from now, for a job's `deadline`.
# timezone.utc rather than datetime.UTC: the system python3 on macOS is 3.9.
deadline_in() { python3 -c "
from datetime import datetime, timedelta, timezone
print((datetime.now(timezone.utc) + timedelta(seconds=$1)).isoformat().replace('+00:00', 'Z'))"; }

# db-path, sql — the first row of a query against the coordinator's SQLite, pipe-joined,
# NULL as empty. For asserting on columns the API does not expose.
db_row() {
  python3 -c "
import sqlite3, sys
r = sqlite3.connect(sys.argv[1]).execute(sys.argv[2]).fetchone()
print('|'.join('' if v is None else str(v) for v in (r or ())))" "$1" "$2"
}

# THE one definition. CI sets CBK_REDIS_CLI because a GitHub service container has no
# `docker exec` to reach; everything else uses the dev container.
redis_cli()   { ${CBK_REDIS_CLI:-docker exec cbk-redis redis-cli} "$@"; }
flush_redis() { redis_cli -n 0 FLUSHDB >/dev/null; }
