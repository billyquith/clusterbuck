#!/usr/bin/env bash
# clusterbuck worker installer — Linux / macOS
#
# Installs the worker agent (cbk) as a persistent service that pulls inference jobs
# from the coordinator's queues and runs them on the local model server.
#
# Idempotent: safe to re-run; updates config and binary in place.
# Re-running without --token skips enrollment (node keeps its existing identity).
#
# Usage:  sudo bash install.sh --coordinator URL --redis-url URL --model NAME [OPTIONS]
#
# Required:
#   --coordinator URL   Coordinator server URL  (e.g. http://coordinator.local:8000)
#   --redis-url URL     Redis URL with password (e.g. redis://:pass@coordinator.local:6379/0)
#   --model NAME        Model this node advertises (e.g. qwen2.5:7b)
#
# Optional:
#   --artifact PATH     Path to cbk.pyz, or an https:// URL to download it from
#                       (default: looks for an existing binary at /opt/clusterbuck/cbk)
#   --token TOKEN       One-time join token to enroll this node with the coordinator
#   --model-manager M   Model manager adapter: auto | ollama | none  (default: auto)
#   --model-server URL  Local model server URL (default: http://127.0.0.1:11434/v1)

set -euo pipefail

# ── defaults ──────────────────────────────────────────────────────────────────
COORDINATOR_URL=""
REDIS_URL=""
MODEL_NAME=""
ARTIFACT=""
JOIN_TOKEN=""
MODEL_MANAGER="auto"
MODEL_SERVER_URL="http://127.0.0.1:11434/v1"

DEPLOY_DIR="/opt/clusterbuck"
CBK_BIN="$DEPLOY_DIR/cbk"
ETC_DIR="/etc/clusterbuck"
VAR_DIR="/var/lib/clusterbuck"

# ── helpers ───────────────────────────────────────────────────────────────────
die()  { printf '\033[31m[cbk] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }
info() { printf '\033[36m[cbk]\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m[cbk] ✓\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[cbk] WARN:\033[0m %s\n' "$*"; }

os_type() {
  case "$OSTYPE" in darwin*) echo macos ;; *) echo linux ;; esac
}

# ── args ──────────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --coordinator)   COORDINATOR_URL="$2";  shift 2 ;;
    --redis-url)     REDIS_URL="$2";        shift 2 ;;
    --model)         MODEL_NAME="$2";       shift 2 ;;
    --artifact)      ARTIFACT="$2";         shift 2 ;;
    --token)         JOIN_TOKEN="$2";       shift 2 ;;
    --model-manager) MODEL_MANAGER="$2";   shift 2 ;;
    --model-server)  MODEL_SERVER_URL="$2"; shift 2 ;;
    *) die "unknown option: $1" ;;
  esac
done

[[ -n "$COORDINATOR_URL" ]] || die "--coordinator is required  (e.g. http://coordinator.local:8000)"
[[ -n "$REDIS_URL"       ]] || die "--redis-url is required    (e.g. redis://:pass@coordinator.local:6379/0)"
[[ -n "$MODEL_NAME"      ]] || die "--model is required        (e.g. qwen2.5:7b)"

# ── prerequisite checks ───────────────────────────────────────────────────────
[[ $EUID -eq 0 ]] || die "run as root: sudo bash $0 ..."

if ! python3 -c "import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)" 2>/dev/null; then
  PY_VER=$(python3 -c "import sys; print('.'.join(map(str,sys.version_info[:2])))" 2>/dev/null || echo "not found")
  die "Python 3.11+ required (found $PY_VER)"
fi
PY_VER=$(python3 -c "import sys; print('.'.join(map(str,sys.version_info[:2])))")
ok "Python $PY_VER"

if ! curl -fsS --connect-timeout 3 "$MODEL_SERVER_URL/models" >/dev/null 2>&1; then
  warn "model server not reachable at $MODEL_SERVER_URL — ensure Ollama is running before starting the worker"
fi

# ── system user ───────────────────────────────────────────────────────────────
info "system user"
if [[ "$(os_type)" == "linux" ]]; then
  if ! id clusterbuck &>/dev/null; then
    useradd -r -m -d "$DEPLOY_DIR" -s /usr/sbin/nologin clusterbuck
    ok "created user 'clusterbuck'"
  else
    ok "user 'clusterbuck' already exists"
  fi
else
  # macOS: run as the invoking user (sudo context); no system account needed
  ok "macOS — worker will run as the current user via launchd"
fi

# ── directories ───────────────────────────────────────────────────────────────
info "directories"
if [[ "$(os_type)" == "linux" ]]; then
  install -d -m 0755 -o clusterbuck -g clusterbuck "$DEPLOY_DIR"
  install -d -m 0755 -o root        -g root        "$ETC_DIR"
  install -d -m 0755 -o clusterbuck -g clusterbuck "$VAR_DIR"
else
  mkdir -p "$DEPLOY_DIR" "$ETC_DIR" "$VAR_DIR"
fi
ok "directories ready"

# ── binary ────────────────────────────────────────────────────────────────────
info "worker binary"
if [[ -n "$ARTIFACT" ]]; then
  if [[ "$ARTIFACT" == https://* || "$ARTIFACT" == http://* ]]; then
    info "downloading $ARTIFACT …"
    curl -fsSL "$ARTIFACT" -o "$CBK_BIN"
  else
    [[ -f "$ARTIFACT" ]] || die "artifact not found: $ARTIFACT"
    cp "$ARTIFACT" "$CBK_BIN"
  fi
  chmod 0755 "$CBK_BIN"
  if [[ "$(os_type)" == "linux" ]]; then
    chown clusterbuck:clusterbuck "$CBK_BIN"
  fi
  ok "binary installed → $CBK_BIN"
elif [[ -x "$CBK_BIN" ]]; then
  ok "existing binary kept → $CBK_BIN"
else
  die "no binary at $CBK_BIN — supply one with --artifact PATH
  Build on the coordinator:  cd /opt/clusterbuck/worker && python3 build.py
  Copy here:                 scp coordinator:/opt/clusterbuck/worker/dist/cbk.pyz /tmp/
  Then re-run:               sudo bash install.sh --artifact /tmp/cbk.pyz ..."
fi

# ── worker.env ────────────────────────────────────────────────────────────────
info "worker.env"
NODE_STATE="$VAR_DIR/node.json"
ENV_FILE="$ETC_DIR/worker.env"

if [[ ! -f "$ENV_FILE" ]]; then
  umask 077
  cat > "$ENV_FILE" <<EOF
CBK_REDIS_URL=${REDIS_URL}
CBK_SERVER_URL=${COORDINATOR_URL}
CBK_MODEL_SERVER_URL=${MODEL_SERVER_URL}
CBK_MODEL=${MODEL_NAME}
CBK_MODEL_MANAGER=${MODEL_MANAGER}
CBK_NODE_STATE=${NODE_STATE}
EOF
  chown root:root "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  ok "worker.env written → $ENV_FILE"
else
  ok "worker.env already exists (not overwritten — edit manually to change)"
fi

# ── service ───────────────────────────────────────────────────────────────────
info "service"
if [[ "$(os_type)" == "linux" ]]; then
  # Locate the service file — in repo if installed, or alongside this script
  SVC_SRC=""
  for candidate in \
    "$(dirname "$0")/../../deploy/systemd/cbk-worker.service" \
    "$DEPLOY_DIR/deploy/systemd/cbk-worker.service"; do
    [[ -f "$candidate" ]] && SVC_SRC="$candidate" && break
  done
  [[ -n "$SVC_SRC" ]] || die "cbk-worker.service not found — run from the repo or install the coordinator first"
  install -m 0644 "$SVC_SRC" /etc/systemd/system/cbk-worker.service
  systemctl daemon-reload
  ok "systemd unit installed"

else
  # macOS: launchd plist
  PLIST_DIR="/Library/LaunchDaemons"
  PLIST="$PLIST_DIR/com.clusterbuck.worker.plist"
  INVOKING_USER="${SUDO_USER:-$(logname 2>/dev/null || echo root)}"
  LOG_DIR="/var/log/clusterbuck"
  mkdir -p "$LOG_DIR"

  cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key>             <string>com.clusterbuck.worker</string>
  <key>ProgramArguments</key>  <array>
    <string>/usr/bin/python3</string>
    <string>${CBK_BIN}</string>
    <string>work</string>
  </array>
  <key>EnvironmentVariables</key> <dict>
    <key>CBK_REDIS_URL</key>       <string>${REDIS_URL}</string>
    <key>CBK_SERVER_URL</key>      <string>${COORDINATOR_URL}</string>
    <key>CBK_MODEL_SERVER_URL</key><string>${MODEL_SERVER_URL}</string>
    <key>CBK_MODEL</key>           <string>${MODEL_NAME}</string>
    <key>CBK_MODEL_MANAGER</key>   <string>${MODEL_MANAGER}</string>
    <key>CBK_NODE_STATE</key>      <string>${NODE_STATE}</string>
  </dict>
  <key>RunAtLoad</key>         <true/>
  <key>KeepAlive</key>         <true/>
  <key>StandardOutPath</key>   <string>${LOG_DIR}/worker.log</string>
  <key>StandardErrorPath</key> <string>${LOG_DIR}/worker.log</string>
  <key>UserName</key>          <string>${INVOKING_USER}</string>
</dict></plist>
EOF
  ok "launchd plist written → $PLIST"
fi

# ── enroll ────────────────────────────────────────────────────────────────────
if [[ -n "$JOIN_TOKEN" ]]; then
  info "enrolling with coordinator"
  if [[ -f "$NODE_STATE" ]]; then
    ok "node.json already exists — skipping enrollment (node is already registered)"
  else
    env CBK_SERVER_URL="$COORDINATOR_URL" CBK_NODE_STATE="$NODE_STATE" \
      python3 "$CBK_BIN" enroll --token "$JOIN_TOKEN"
    if [[ "$(os_type)" == "linux" ]]; then
      chown clusterbuck:clusterbuck "$NODE_STATE"
    fi
    ok "enrolled → $NODE_STATE"
  fi
else
  warn "no --token given — skipping enrollment"
  warn "mint a token on the coordinator and run:"
  warn "  sudo bash install.sh --coordinator $COORDINATOR_URL --redis-url '...' --model $MODEL_NAME --token TOKEN"
fi

# ── start ─────────────────────────────────────────────────────────────────────
if [[ -f "$NODE_STATE" ]]; then
  info "starting worker service"
  if [[ "$(os_type)" == "linux" ]]; then
    systemctl enable --now cbk-worker
    sleep 2
    systemctl is-active cbk-worker >/dev/null || {
      journalctl -u cbk-worker -n 20 --no-pager >&2
      die "cbk-worker failed to start — see journal above"
    }
    ok "cbk-worker active"
  else
    launchctl load -w "$PLIST" 2>/dev/null || launchctl enable "system/com.clusterbuck.worker"
    launchctl start com.clusterbuck.worker 2>/dev/null || true
    ok "worker started via launchd"
  fi
else
  warn "service not started — enroll first, then:"
  if [[ "$(os_type)" == "linux" ]]; then
    warn "  sudo systemctl enable --now cbk-worker"
  else
    warn "  sudo launchctl load -w $PLIST"
  fi
fi

# ── summary ───────────────────────────────────────────────────────────────────
echo
printf '\033[32m━━━ clusterbuck worker installed ━━━\033[0m\n'
printf '  Binary:  %s\n'    "$CBK_BIN"
printf '  Config:  %s\n'    "$ENV_FILE"
printf '  State:   %s\n'    "$NODE_STATE"
printf '  Model:   %s\n'    "$MODEL_NAME"
echo
if [[ "$(os_type)" == "linux" ]]; then
  printf '  Status:  sudo systemctl status cbk-worker\n'
  printf '  Logs:    sudo journalctl -u cbk-worker -f\n'
else
  printf '  Status:  sudo launchctl list | grep clusterbuck\n'
  printf '  Logs:    tail -f /var/log/clusterbuck/worker.log\n'
fi
printf '\033[32m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m\n'
