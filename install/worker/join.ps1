# Join this machine to a clusterbuck fleet as a worker — Windows.
#
#   .\join.ps1 -Coordinator http://coordinator.local:8018 -Model qwen2.5:7b
#
# Deliberately thin: the logic lives in join.py so it is written once rather than twice
# (see join.sh, its Linux/macOS counterpart). Arguments are passed straight through, so
# anything join.py accepts works here.
#Requires -RunAsAdministrator

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path

$python = Get-Command python3 -ErrorAction SilentlyContinue
if (-not $python) { $python = Get-Command python -ErrorAction SilentlyContinue }
if (-not $python) {
    Write-Host '[join] ERROR: python not found — the worker needs Python 3.11+' -ForegroundColor Red
    exit 1
}

& $python.Source (Join-Path $here 'join.py') @args
exit $LASTEXITCODE
