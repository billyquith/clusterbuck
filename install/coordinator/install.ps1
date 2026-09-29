#Requires -RunAsAdministrator
<#
.SYNOPSIS
    clusterbuck coordinator installer — Windows

.DESCRIPTION
    Installs the always-on coordinator (job API, queue broker, fleet manager) as a
    Windows Scheduled Task. Redis is run as a Docker container via Docker Desktop.

    Idempotent: safe to re-run; updates an existing installation in place.

.PARAMETER Repo
    Git repository URL. Default: https://github.com/billyquith/clusterbuck

.PARAMETER Branch
    Branch or tag to install. Default: main

.PARAMETER Port
    Coordinator API port. Default: 8018

.PARAMETER DeployDir
    Installation root directory. Default: C:\clusterbuck

.PARAMETER WorkerVersion
    Pins CBK_WORKER_CURRENT_VERSION in server config. Default: unset, so nodes are judged
    against whatever release.json names (the variable overrides it).

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File install.ps1
    powershell -ExecutionPolicy Bypass -File install.ps1 -Port 9000 -Branch v1.2.3
#>
param(
    [string]$Repo          = 'https://github.com/billyquith/clusterbuck',
    [string]$Branch        = 'main',
    [string]$Port          = '8018',
    [string]$DeployDir     = 'C:\clusterbuck',
    [string]$WorkerVersion = ''
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
if (-not $py) { Write-Fail 'Python not found. Install Python 3.12+ from https://python.org and ensure it is on PATH.' }
$pyVer = & python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")'
if ([version]$pyVer -lt [version]'3.12') { Write-Fail "Python 3.12+ required (found $pyVer)" }
Write-Ok "Python $pyVer"

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Fail 'git not found. Install Git for Windows from https://git-scm.com/'
}

# Docker Desktop provides the 'docker' CLI; required for Redis
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Fail 'Docker CLI not found. Install Docker Desktop from https://www.docker.com/products/docker-desktop/'
}
try { & docker info 2>$null | Out-Null } catch {
    Write-Fail 'Docker daemon is not running. Start Docker Desktop and try again.'
}
Write-Ok 'Docker available'

# ── directories ───────────────────────────────────────────────────────────────
Write-Step 'Creating directories'
$EtcDir = "$env:ProgramData\clusterbuck"
$VarDir = "$env:ProgramData\clusterbuck\data"
$LogDir = "$env:ProgramData\clusterbuck\logs"

foreach ($d in $DeployDir, $EtcDir, $VarDir, $LogDir) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}
Write-Ok "Directories ready under $DeployDir and $EtcDir"

# ── secrets (minted once, never overwritten) ──────────────────────────────────
Write-Step 'Secrets'
$SecretsFile = "$EtcDir\secrets.env"
if (-not (Test-Path $SecretsFile)) {
    $redisPw = -join ((48..57 + 97..102) * 10 | Get-Random -Count 48 | ForEach-Object { [char]$_ })
    $apiKey  = [Convert]::ToBase64String([System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
    @"
REDIS_PW=$redisPw
API_KEY=$apiKey
"@ | Set-Content -Path $SecretsFile -Encoding UTF8
    # Restrict to Administrators only
    $acl = Get-Acl $SecretsFile
    $acl.SetAccessRuleProtection($true, $false)
    $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new(
        'Administrators', 'FullControl', 'Allow'))
    Set-Acl $SecretsFile $acl
    Write-Ok "Secrets minted -> $SecretsFile  (keep this file safe)"
} else {
    Write-Ok "Reusing existing secrets from $SecretsFile"
}
# -Raw is required. Without it Get-Content emits a string[], the pipeline hands
# ConvertFrom-StringData one line at a time, and the result is an ARRAY of single-entry
# hashtables rather than one hashtable — at which point $secrets['REDIS_PW'] is a string
# index into Object[] and throws "Cannot convert value \"REDIS_PW\" to type Int32".
# Every run of this installer would have failed on the next line.
$secrets = Get-Content $SecretsFile -Raw | ConvertFrom-StringData
$redisPw = $secrets['REDIS_PW']
$apiKey  = $secrets['API_KEY']
if (-not $redisPw -or -not $apiKey) {
    throw "Could not read REDIS_PW/API_KEY from $SecretsFile"
}

# ── Redis (Docker container) ──────────────────────────────────────────────────
Write-Step 'Redis'
$containerName = 'cbk-redis'
$existing = & docker ps -a --filter "name=^$containerName$" --format '{{.Status}}' 2>$null
if ($existing -match '^Up') {
    Write-Ok 'Redis container already running'
} elseif ($existing) {
    & docker start $containerName | Out-Null
    Write-Ok 'Redis container started'
} else {
    & docker run -d --name $containerName --restart always -p 127.0.0.1:6379:6379 `
        redis:7-alpine redis-server --requirepass $redisPw | Out-Null
    Write-Ok 'Redis container created and started'
}

# ── repo ──────────────────────────────────────────────────────────────────────
Write-Step "Repo ($Branch)"
if (Test-Path "$DeployDir\.git") {
    Push-Location $DeployDir
    & git remote set-url origin $Repo
    & git fetch --depth 1 origin $Branch 2>&1 | Out-Null
    & git checkout -f -B $Branch FETCH_HEAD 2>&1 | Out-Null
    Pop-Location
    Write-Ok "Repo updated"
} else {
    & git clone --depth 1 --branch $Branch $Repo $DeployDir 2>&1 | Out-Null
    Write-Ok "Repo cloned -> $DeployDir"
}

# ── server Python venv ────────────────────────────────────────────────────────
Write-Step 'Server venv'
$VenvDir = "$DeployDir\server\.venv"
$uvCmd = Get-Command uv -ErrorAction SilentlyContinue
if (-not $uvCmd) {
    Write-Warn 'uv not found — installing via pip'
    & python -m pip install --quiet uv
}
& uv venv $VenvDir --python python 2>&1 | Out-Null
& uv pip install --quiet -e "$DeployDir\server" --python "$VenvDir\Scripts\python.exe"
Write-Ok 'Server venv ready'

# ── config ────────────────────────────────────────────────────────────────────
Write-Step 'Config'
$ServerEnv = "$EtcDir\server.env"
$FleetYaml = "$EtcDir\fleet.yaml"
$DbPath    = "$VarDir\cbk.db"

if (-not (Test-Path $ServerEnv)) {
    # NEVER `Set-Content -Encoding UTF8` here. On PowerShell 5.1 that encoder emits a BOM,
    # and the run-coordinator.cmd wrapper below parses this file with cmd's `for /f`, which
    # folds a BOM into the FIRST variable's NAME. That would leave CBK_API_KEY unset while
    # every other line worked — an unauthenticated coordinator that looks configured. Same
    # trap the worker installer hit (see install/worker/install.ps1 and commit 51f1790).
    $envBody = @"
CBK_API_KEY=$apiKey
CBK_REDIS_URL=redis://:$redisPw@127.0.0.1:6379/0
CBK_DB_PATH=$DbPath
CBK_FLEET_PATH=$FleetYaml
CBK_HOST=0.0.0.0
CBK_PORT=$Port
"@
    if ($WorkerVersion) { $envBody += "`r`nCBK_WORKER_CURRENT_VERSION=$WorkerVersion" }
    [System.IO.File]::WriteAllText($ServerEnv, $envBody, (New-Object System.Text.UTF8Encoding($false)))
    # Restrict to Administrators
    $acl = Get-Acl $ServerEnv
    $acl.SetAccessRuleProtection($true, $false)
    $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new(
        'Administrators', 'FullControl', 'Allow'))
    Set-Acl $ServerEnv $acl
    Write-Ok "server.env written"
} else {
    Write-Ok 'server.env already exists (not overwritten)'
}

if (-not (Test-Path $FleetYaml)) {
    # PyYAML tolerates a leading BOM, so this one is not currently load-bearing — but the
    # file is hand-edited afterwards and read by a Python that does not pass utf-8-sig, so
    # it is written BOM-less for the same reason as server.env rather than as an exception.
    [System.IO.File]::WriteAllText($FleetYaml, "nodes: []`r`ncapabilities: {}`r`n",
        (New-Object System.Text.UTF8Encoding($false)))
    Write-Ok 'fleet.yaml written'
}

# ── Scheduled Task ────────────────────────────────────────────────────────────
Write-Step 'Scheduled Task'
$taskName = 'cbk-coordinator'
$python   = "$VenvDir\Scripts\python.exe"
$wrapper  = "$DeployDir\run-coordinator.cmd"

# The wrapper READS server.env at launch. It used to bake the values in at install time,
# character-for-character the code deleted from install/worker/install.ps1 in 51f1790 —
# which meant editing the documented config file changed nothing until someone re-ran the
# installer, while `:169` cheerfully reported "server.env already exists (not
# overwritten)". The README and the summary below both tell the operator to hand-edit that
# file to add the three join settings, so the documented procedure was a silent no-op on a
# Windows coordinator. It also wrote the API key and the Redis password to a second file.
#
# Task Scheduler cannot source an env file, which is why a .cmd shim exists at all.
# PYTHONUTF8: SYSTEM redirects stdout, so Python falls back to the ANSI codepage and dies
# on the coordinator's own non-ASCII log output.
@"
@echo off
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
for /f "usebackq eol=# tokens=1,* delims==" %%a in ("$ServerEnv") do set "%%a=%%b"
"$python" -m clusterbuck >> "$LogDir\coordinator.log" 2>&1
"@ | Set-Content $wrapper -Encoding ASCII

$action   = New-ScheduledTaskAction -Execute 'cmd.exe' `
                -Argument "/c `"$wrapper`"" -WorkingDirectory "$DeployDir\server"
$trigger  = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit 0 `
                -RestartCount 2147483647 `
                -RestartInterval (New-TimeSpan -Minutes 1) `
                -StartWhenAvailable
$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -RunLevel Highest

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $taskName `
    -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null

Start-ScheduledTask -TaskName $taskName
Start-Sleep -Seconds 3

$state = (Get-ScheduledTask -TaskName $taskName).State
Write-Ok "Task '$taskName' state: $state"

# ── verify ────────────────────────────────────────────────────────────────────
Start-Sleep -Seconds 2
try {
    $resp = Invoke-RestMethod "http://127.0.0.1:$Port/healthz" -TimeoutSec 5
    if ($resp.status -eq 'ok') { Write-Ok "API healthy -> http://127.0.0.1:$Port/" }
} catch {
    Write-Warn "API not responding yet — check $LogDir\coordinator.log"
}

# ── summary ───────────────────────────────────────────────────────────────────
Write-Host ''
Write-Host '━━━ clusterbuck coordinator installed ━━━' -ForegroundColor Green
Write-Host "  API:      http://localhost:$Port/"
Write-Host "  Secrets:  $SecretsFile  (keep safe)"
Write-Host "  Config:   $ServerEnv"
Write-Host "  Logs:     $LogDir\coordinator.log"
Write-Host ''
Write-Host '  Next steps - turn on worker joining (three settings this script does not'
Write-Host '  write, and the server config is never overwritten, so add them by hand):'
Write-Host ''
Write-Host '    1. Generate a join password and keep it in a password manager:'
Write-Host "         [guid]::NewGuid().ToString('N')      # 32 hex chars"
Write-Host '    2. Build the worker artifact joiners will download:'
Write-Host "         cd $DeployDir\worker; python build.py"
Write-Host "    3. Add to $ServerEnv, then restart the coordinator:"
Write-Host '         CBK_JOIN_PASSWORD=<from step 1>            # 16 chars minimum'
Write-Host "         CBK_WORKER_ARTIFACT=$DeployDir\worker\dist\cbk.pyz"
Write-Host '         CBK_BROKER_ADVERTISE_URL=redis://:<redis-pw>@<THIS-HOST-LAN-IP>:6379/0'
Write-Host '       The last one is the Redis address OTHER machines use. Loopback is'
Write-Host '       refused (503): advertising it points every worker at its own localhost.'
Write-Host '    4. On each worker: clone the repo and run'
Write-Host "         .\install\worker\join.ps1 --coordinator http://<this-host>:$Port --model <model>"
Write-Host '       (join.sh on Linux/macOS). It asks for the join password and fetches the'
Write-Host '       token, broker URL and artifact itself; the operator API key stays here.'
Write-Host '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━' -ForegroundColor Green
