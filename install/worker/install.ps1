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

# worker.env and the generated wrapper both carry CBK_REDIS_URL, and SYSTEM executes the
# wrapper at boot. Left at the inherited ACL they are readable by Users and writable by
# Authenticated Users - which discloses the broker credential to every local account and
# hands one a way to run code as SYSTEM.
#
# Applied on EVERY run, not only the run that creates the file. An install that died
# between writing a secret and securing it used to leave it exposed, and the
# "already exists, not overwritten" guard meant no later run ever repaired it.
function Protect-SecretFile {
    param([Parameter(Mandatory)][string]$Path)
    $acl = Get-Acl $Path
    $acl.SetAccessRuleProtection($true, $false)          # stop inheriting, copy nothing
    foreach ($rule in @($acl.Access)) { [void]$acl.RemoveAccessRule($rule) }
    foreach ($id in @('Administrators', 'SYSTEM')) {
        $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new(
            $id, 'FullControl', 'Allow'))
    }
    Set-Acl -Path $Path -AclObject $acl
}

# ── prerequisite checks ───────────────────────────────────────────────────────
Write-Step 'Checking prerequisites'

$py = Get-Command python -ErrorAction SilentlyContinue
if (-not $py) { Write-Fail 'Python not found. Install Python 3.11+ from https://python.org' }
$pyVer = & python -c 'import platform; print(platform.python_version())'
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

# SYSTEM executes this binary at every boot, and at the inherited ACL it is WRITABLE by
# Authenticated Users - so any local account can replace the code the machine runs as
# SYSTEM. Same hole the wrapper had, on the file that actually is the worker. Applied
# whether the binary was just installed or was already there, since an older install left
# it wide open and no re-run would otherwise repair it.
#
# Self-update (ADR 13) is unaffected: the worker runs as SYSTEM, which keeps FullControl
# and can still swap the file and keep cbk.prev beside it.
if (Test-Path $CbkBin) { Protect-SecretFile $CbkBin }

# WRITING FILES THE WORKER PARSES: never `Set-Content -Encoding UTF8`.
#
# On PowerShell 5.1 that encoder emits a BOM, and nothing downstream expects one:
#   * the worker reads node.json and its own state as plain UTF-8 and fails outright with
#     "Unexpected UTF-8 BOM (decode using utf-8-sig)";
#   * cmd's `for /f` folds the BOM into the FIRST variable's NAME, so the leading setting
#     is silently never applied - which for worker.env is the broker URL.
# Both were observed on a live node. Use ASCII (for the .cmd wrapper) or
# [System.IO.File]::WriteAllText with UTF8Encoding($false).

# ── worker config ─────────────────────────────────────────────────────────────
Write-Step 'Worker config'
$EnvFile   = "$EtcDir\worker.env"
$NodeState = "$VarDir\node.json"

if (-not (Test-Path $EnvFile)) {
    $envBody = @"
CBK_REDIS_URL=$RedisUrl
CBK_SERVER_URL=$CoordinatorUrl
CBK_MODEL_SERVER_URL=$ModelServerUrl
CBK_MODEL=$Model
CBK_MODEL_MANAGER=$ModelManager
CBK_NODE_STATE=$NodeState
"@
    # NOT Set-Content -Encoding UTF8: on PowerShell 5.1 that writes a BOM, and this file is
    # parsed by two things that do not expect one. `cmd`'s for/f would fold the BOM into the
    # FIRST variable's name, so CBK_REDIS_URL would silently never be set; and the worker
    # reads its own state files as plain UTF-8 and fails outright with "Unexpected UTF-8
    # BOM". Both were observed on a live node.
    [System.IO.File]::WriteAllText($EnvFile, $envBody, (New-Object System.Text.UTF8Encoding($false)))
    Write-Ok "worker.env written -> $EnvFile"
} else {
    Write-Ok 'worker.env already exists (not overwritten — edit manually to change)'
}
Protect-SecretFile $EnvFile

# ── Scheduled Task ────────────────────────────────────────────────────────────
Write-Step 'Scheduled Task'
$taskName = 'cbk-worker'
$wrapper  = "$DeployDir\run-worker.cmd"

# The wrapper READS worker.env at launch; it does not bake a copy of it.
#
# It used to inline every variable here at install time, which made this file - not
# worker.env - the config the worker actually ran on. Editing the documented file then
# changed nothing, silently: a model swap applied to worker.env left the node still
# serving the old one, and the two copies drifted with no warning. It also put the broker
# credential on disk twice.
#
# `eol=#` skips comment lines; `tokens=1,* delims==` keeps everything after the FIRST `=`
# as the value, so a credential containing `=` survives intact.
@"
@echo off
rem SYSTEM runs this with stdout redirected to a file, so Python falls back to the legacy
rem ANSI codepage and dies encoding the arrows in the worker's own log lines. Force UTF-8.
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
for /f "usebackq eol=# tokens=1,* delims==" %%a in ("$EnvFile") do set "%%a=%%b"
python "$CbkBin" work >> "$LogDir\worker.log" 2>&1
"@ | Set-Content $wrapper -Encoding ASCII

# No longer holds the credential, but SYSTEM executes it at every boot - so an account
# that can write it can run code as SYSTEM. Lock it down for that reason, not secrecy.
Protect-SecretFile $wrapper

$action   = New-ScheduledTaskAction -Execute 'cmd.exe' `
                -Argument "/c `"$wrapper`"" -WorkingDirectory $DeployDir
$trigger  = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit 0 `
                -RestartCount 999 `
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

# node.json carries node_key, this node's credential for authenticating its heartbeats.
# `cbk enroll` creates it fresh, so it inherits the directory ACL and lands readable by
# every local account. Protected OUTSIDE the enrol branch so an already-enrolled node is
# repaired too, for the same reason worker.env is.
#
# Safe against the worker's own writes: save_state truncates the file in place rather than
# replacing it, so the ACL survives every mode change. The cost is that `cbk pause` now
# needs an elevated shell here — which matches Linux, where the file belongs to the
# service account and a human owner pauses through sudo.
if (Test-Path $NodeState) { Protect-SecretFile $NodeState }

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
