#Requires -RunAsAdministrator
<#
.SYNOPSIS
    clusterbuck worker installer — Windows

.DESCRIPTION
    Installs the clusterbuck worker agent (cbk.pyz) as a Windows Scheduled Task that
    pulls inference jobs from the coordinator's queues and runs them on Ollama.

    Idempotent: safe to re-run; updates config and binary without re-enrolling.
    Re-running without -Token skips enrollment (node keeps its existing identity).

.PARAMETER CoordinatorUrl
    Coordinator server URL. Required.  e.g. http://coordinator.local:8018

.PARAMETER RedisUrl
    Redis URL including password. Required.  e.g. redis://:pass@coordinator.local:6379/0

.PARAMETER Model
    Model this node advertises to the coordinator. Required.  e.g. qwen2.5:7b

.PARAMETER Artifact
    Path or https:// URL to cbk.pyz. If omitted, looks for an existing binary first.

.PARAMETER Token
    One-time join token for enrolling this node. Obtain from the coordinator with:
      Invoke-RestMethod http://coordinator:8018/nodes/tokens -Method Post -Headers @{...}

.PARAMETER ModelManager
    Model manager adapter: auto | ollama | none.  Default: auto

.PARAMETER ModelServerUrl
    Local model server URL.  Default: http://127.0.0.1:11434/v1

.PARAMETER DeployDir
    Directory where the binary lives.  Default: C:\clusterbuck

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File install.ps1 `
        -CoordinatorUrl http://coordinator.local:8018 `
        -RedisUrl 'redis://:secret@coordinator.local:6379/0' `
        -Model qwen2.5:7b `
        -Artifact C:\Downloads\cbk.pyz `
        -Token abc123xyz
#>
param(
    [Parameter(Mandatory)] [string]$CoordinatorUrl,
    [Parameter(Mandatory)] [string]$RedisUrl,
    [Parameter(Mandatory)] [string]$Model,
    [string]$Artifact       = '',
    [string]$Token          = '',
    [string]$ModelManager   = 'auto',
    [string]$ModelServerUrl = 'http://127.0.0.1:11434/v1',
    [string]$DeployDir      = 'C:\clusterbuck'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Step  { Write-Host "[cbk] $args" -ForegroundColor Cyan }
function Write-Ok    { Write-Host "[cbk] OK  $args" -ForegroundColor Green }
function Write-Fail  { Write-Host "[cbk] ERROR: $args" -ForegroundColor Red; exit 1 }
function Write-Warn  { Write-Host "[cbk] WARN: $args" -ForegroundColor Yellow }

# ── prerequisite checks ───────────────────────────────────────────────────────
Write-Step 'Checking prerequisites'

$py = Get-Command python -ErrorAction SilentlyContinue
if (-not $py) { Write-Fail 'Python not found. Install Python 3.11+ from https://python.org' }
$pyVer = & python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")'
if ([version]$pyVer -lt [version]'3.11') { Write-Fail "Python 3.11+ required (found $pyVer)" }
Write-Ok "Python $pyVer"

try {
    Invoke-RestMethod "$ModelServerUrl/models" -TimeoutSec 3 | Out-Null
    Write-Ok 'Model server reachable'
} catch {
    Write-Warn "Model server not reachable at $ModelServerUrl — ensure Ollama is running before starting the worker"
}

# ── directories ───────────────────────────────────────────────────────────────
Write-Step 'Directories'
$EtcDir = "$env:ProgramData\clusterbuck"
$VarDir = "$env:ProgramData\clusterbuck\data"
$LogDir = "$env:ProgramData\clusterbuck\logs"

foreach ($d in $DeployDir, $EtcDir, $VarDir, $LogDir) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}
Write-Ok 'Directories ready'

# ── binary ────────────────────────────────────────────────────────────────────
Write-Step 'Worker binary'
$CbkBin = "$DeployDir\cbk.pyz"

if ($Artifact) {
    if ($Artifact -match '^https?://') {
        Write-Step "Downloading $Artifact"
        Invoke-WebRequest -Uri $Artifact -OutFile $CbkBin -UseBasicParsing
    } else {
        if (-not (Test-Path $Artifact)) { Write-Fail "Artifact not found: $Artifact" }
        Copy-Item $Artifact $CbkBin -Force
    }
    Write-Ok "Binary installed -> $CbkBin"
} elseif (Test-Path $CbkBin) {
    Write-Ok "Existing binary kept -> $CbkBin"
} else {
    Write-Fail @"
No binary at $CbkBin — supply one with -Artifact PATH
  Build on the coordinator:
    cd C:\clusterbuck\worker && python build.py
  Copy here and re-run:
    install.ps1 -CoordinatorUrl ... -RedisUrl ... -Model ... -Artifact C:\path\to\cbk.pyz
"@
}

# ── worker config ─────────────────────────────────────────────────────────────
Write-Step 'Worker config'
$EnvFile   = "$EtcDir\worker.env"
$NodeState = "$VarDir\node.json"

if (-not (Test-Path $EnvFile)) {
    @"
CBK_REDIS_URL=$RedisUrl
CBK_SERVER_URL=$CoordinatorUrl
CBK_MODEL_SERVER_URL=$ModelServerUrl
CBK_MODEL=$Model
CBK_MODEL_MANAGER=$ModelManager
CBK_NODE_STATE=$NodeState
"@ | Set-Content -Path $EnvFile -Encoding UTF8
    # Restrict to Administrators
    $acl = Get-Acl $EnvFile
    $acl.SetAccessRuleProtection($true, $false)
    $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new(
        'Administrators', 'FullControl', 'Allow'))
    Set-Acl $EnvFile $acl
    Write-Ok "worker.env written -> $EnvFile"
} else {
    Write-Ok 'worker.env already exists (not overwritten — edit manually to change)'
}

# ── Scheduled Task ────────────────────────────────────────────────────────────
Write-Step 'Scheduled Task'
$taskName = 'cbk-worker'
$wrapper  = "$DeployDir\run-worker.cmd"

# Env vars baked into the wrapper so the task runs without reading the .env file at launch
$envLines = (Get-Content $EnvFile | Where-Object { $_ -match '^\w' } |
    ForEach-Object { "set $_" }) -join "`r`n"

@"
@echo off
$envLines
python "$CbkBin" work >> "$LogDir\worker.log" 2>&1
"@ | Set-Content $wrapper -Encoding ASCII

$action   = New-ScheduledTaskAction -Execute 'cmd.exe' `
                -Argument "/c `"$wrapper`"" -WorkingDirectory $DeployDir
$trigger  = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit 0 `
                -RestartCount 2147483647 `
                -RestartInterval (New-TimeSpan -Minutes 1) `
                -StartWhenAvailable
$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -RunLevel Highest

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $taskName `
    -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
Write-Ok "Scheduled Task '$taskName' registered"

# ── enroll ────────────────────────────────────────────────────────────────────
if ($Token) {
    Write-Step 'Enrolling with coordinator'
    if (Test-Path $NodeState) {
        Write-Ok 'node.json already exists — skipping enrollment (node is already registered)'
    } else {
        $env:CBK_SERVER_URL  = $CoordinatorUrl
        $env:CBK_NODE_STATE  = $NodeState
        & python $CbkBin enroll --token $Token
        Remove-Item Env:CBK_SERVER_URL, Env:CBK_NODE_STATE -ErrorAction SilentlyContinue
        Write-Ok "Enrolled -> $NodeState"
    }
} else {
    Write-Warn 'No -Token provided — skipping enrollment'
    Write-Warn 'Mint a token on the coordinator, then re-run with -Token <value>'
}

# ── start ─────────────────────────────────────────────────────────────────────
if (Test-Path $NodeState) {
    Write-Step 'Starting worker'
    Start-ScheduledTask -TaskName $taskName
    Start-Sleep -Seconds 2
    $state = (Get-ScheduledTask -TaskName $taskName).State
    Write-Ok "Worker task state: $state"
} else {
    Write-Warn "Worker not started — enroll first, then: Start-ScheduledTask '$taskName'"
}

# ── summary ───────────────────────────────────────────────────────────────────
Write-Host ''
Write-Host '━━━ clusterbuck worker installed ━━━' -ForegroundColor Green
Write-Host "  Binary:  $CbkBin"
Write-Host "  Config:  $EnvFile"
Write-Host "  State:   $NodeState"
Write-Host "  Model:   $Model"
Write-Host ''
Write-Host '  Manage:'
Write-Host "    Start:  Start-ScheduledTask  '$taskName'"
Write-Host "    Stop:   Stop-ScheduledTask   '$taskName'"
Write-Host "    Status: Get-ScheduledTask    '$taskName' | Select-Object State"
Write-Host "    Logs:   Get-Content '$LogDir\worker.log' -Wait"
Write-Host '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━' -ForegroundColor Green
