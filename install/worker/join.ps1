# Join this machine to a clusterbuck fleet as a worker — Windows.
#
#   .\join.ps1 --coordinator http://coordinator.local:8018 --model qwen2.5:7b
#
# Deliberately thin: the logic lives in join.py so it is written once rather than twice
# (see join.sh, its Linux/macOS counterpart). Arguments are passed straight through, so
# anything join.py accepts works here.
#Requires -RunAsAdministrator

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path

# Finding an interpreter is not the same as finding Python. On a stock Windows,
# `python3` resolves to the Microsoft Store App Execution Alias - a stub that prints
# "Python was not found" and exits 9009 - and Get-Command reports it as a perfectly good
# command. Probing `python3` first therefore bound to the decoy and never fell back.
#
# So: try the launcher Windows actually ships first, and accept a candidate only when it
# RUNS and reports 3.11+. Presence on PATH is not evidence.
$candidates = @(
    @{ Exe = 'py';      Prefix = @('-3') },
    @{ Exe = 'python';  Prefix = @()     },
    @{ Exe = 'python3'; Prefix = @()     }
)

$python = $null
foreach ($c in $candidates) {
    $cmd = Get-Command $c.Exe -ErrorAction SilentlyContinue
    if (-not $cmd) { continue }
    # No double quotes in the -c program: PowerShell strips them when it builds a native
    # command line, which silently corrupts the snippet (see install.ps1's version check).
    $probe = @($c.Prefix) + @('-c', 'import sys; print(sys.version_info.major, sys.version_info.minor)')
    $out = & $cmd.Source @probe 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $out) { continue }
    $parts = ($out | Select-Object -First 1).Trim() -split '\s+'
    if ($parts.Count -lt 2) { continue }
    try { $ver = [version]"$($parts[0]).$($parts[1])" } catch { continue }
    if ($ver -lt [version]'3.11') { continue }
    $python = @{ Source = $cmd.Source; Prefix = $c.Prefix; Version = $ver }
    break
}

if (-not $python) {
    Write-Host '[join] ERROR: no working Python 3.11+ found — the worker needs one.' -ForegroundColor Red
    Write-Host '[join]        Install it for ALL USERS so the SYSTEM-run worker task can' -ForegroundColor Red
    Write-Host '[join]        see it too:  winget install Python.Python.3.13 --scope machine' -ForegroundColor Red
    exit 1
}

& $python.Source @($python.Prefix) (Join-Path $here 'join.py') @args
exit $LASTEXITCODE
