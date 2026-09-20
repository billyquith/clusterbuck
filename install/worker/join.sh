#!/usr/bin/env bash
# Join this machine to a clusterbuck fleet as a worker — Linux / macOS.
#
#   sudo bash join.sh --coordinator http://coordinator.local:8018 --model qwen2.5:7b
#
# Deliberately thin: the logic lives in join.py so it is written once rather than twice
# (see join.ps1, its Windows counterpart). Python 3.11+ is already a hard prerequisite for
# the worker itself, so relying on it here costs nothing.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
  printf '\033[31m[join] ERROR:\033[0m python3 not found — the worker needs Python 3.11+\n' >&2
  exit 1
fi

exec python3 "$HERE/join.py" "$@"
