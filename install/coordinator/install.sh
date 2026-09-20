#!/usr/bin/env bash
# clusterbuck coordinator installer — Linux (Debian / Ubuntu / Raspberry Pi OS)
#
# Installs the always-on coordinator (job API, Redis broker, fleet manager) as a
# systemd service under a dedicated 'clusterbuck' system account.
#
# Requires: root, systemd, Python 3.12+, git, curl, apt-get
# Idempotent: safe to re-run; updates an existing installation in place.
#
# Usage:  sudo bash install.sh [OPTIONS]
# Quick:  git clone https://github.com/billyquith/clusterbuck && sudo bash clusterbuck/install/coordinator/install.sh
#         (clone rather than curl the raw URL: the repo is private, so raw.githubusercontent.com 404s)
#
# Options:
#   --repo URL       Git repository to clone  (default: https://github.com/billyquith/clusterbuck)
#   --branch BRANCH  Branch or tag to install (default: main)
#   --port PORT      Coordinator API port     (default: 8018)
#   --lan-redis      Bind Redis on all interfaces so remote workers can reach it
#   --worker-version VER  CBK_WORKER_CURRENT_VERSION to write to server.env (default: 0.7.0)

set -euo pipefail

# ── defaults ──────────────────────────────────────────────────────────────────
REPO_URL="https://github.com/billyquith/clusterbuck"
BRANCH="main"
PORT="8018"
LAN_REDIS=0
WORKER_VERSION="0.7.0"
DEPLOY_DIR="/opt/clusterbuck"
SECRETS_FILE="/root/.cbk-secrets"

# ── helpers ───────────────────────────────────────────────────────────────────
die()  { printf '\033[31m[cbk] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }
info() { printf '\033[36m[cbk]\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m[cbk] ✓\033[0m %s\n' "$*"; }

# ── args ──────────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --repo)           REPO_URL="$2";        shift 2 ;;
    --branch)         BRANCH="$2";          shift 2 ;;
    --port)           PORT="$2";            shift 2 ;;
    --lan-redis)      LAN_REDIS=1;          shift   ;;
    --worker-version) WORKER_VERSION="$2";  shift 2 ;;
    *) die "unknown option: $1  (try --help)" ;;
  esac
done

# ── prerequisite checks ───────────────────────────────────────────────────────
[[ $EUID -eq 0 ]]          || die "run as root: sudo bash $0"
command -v systemctl >/dev/null || die "systemd is required"
command -v git  >/dev/null || die "git is required  (apt-get install git)"
command -v curl >/dev/null || die "curl is required (apt-get install curl)"

if ! python3 -c "import sys; sys.exit(0 if sys.version_info >= (3,12) else 1)" 2>/dev/null; then
  PY_VER=$(python3 -c "import sys; print('.'.join(map(str,sys.version_info[:2])))" 2>/dev/null || echo "not found")
  die "Python 3.12+ required (found $PY_VER)"
fi
PY_VER=$(python3 -c "import sys; print('.'.join(map(str,sys.version_info[:2])))")
ok "Python $PY_VER"

# ── system user ───────────────────────────────────────────────────────────────
info "system user"
if ! id clusterbuck &>/dev/null; then
  useradd -r -m -d "$DEPLOY_DIR" -s /usr/sbin/nologin clusterbuck
  ok "created user 'clusterbuck' (home: $DEPLOY_DIR)"
else
  ok "user 'clusterbuck' already exists"
fi

# ── Redis ─────────────────────────────────────────────────────────────────────
info "Redis"
if ! command -v redis-server >/dev/null; then
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y redis-server >/dev/null
  ok "Redis installed"
else
  ok "Redis $(redis-server --version | awk '{print $3}' | tr -d 'v=,') already installed"
fi

# ── secrets (minted once, never overwritten) ──────────────────────────────────
info "secrets"
if [[ ! -f "$SECRETS_FILE" ]]; then
  REDIS_PW=$(openssl rand -hex 24)
  API_KEY=$(openssl rand -base64 32 | tr -d '\n')
  umask 077
  printf "REDIS_PW='%s'\nAPI_KEY='%s'\n" "$REDIS_PW" "$API_KEY" > "$SECRETS_FILE"
  chmod 600 "$SECRETS_FILE"
  ok "secrets minted → $SECRETS_FILE (keep this file safe)"
else
  ok "reusing existing secrets from $SECRETS_FILE"
fi
# shellcheck disable=SC1090
source "$SECRETS_FILE"

conf=/etc/redis/redis.conf
if grep -qE '^requirepass ' "$conf"; then
  sed -i "s|^requirepass .*|requirepass ${REDIS_PW}|" "$conf"
elif grep -qE '^# *requirepass ' "$conf"; then
  sed -i "s|^# *requirepass .*|requirepass ${REDIS_PW}|" "$conf"
else
  echo "requirepass ${REDIS_PW}" >> "$conf"
fi
systemctl enable --now redis-server >/dev/null 2>&1 || true
systemctl restart redis-server
redis-cli -a "$REDIS_PW" --no-auth-warning ping >/dev/null
ok "Redis secured and running"

# ── uv ────────────────────────────────────────────────────────────────────────
info "uv"
if ! command -v uv >/dev/null; then
  curl -LsSf https://astral.sh/uv/install.sh \
    | env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
  ok "uv installed"
else
  ok "uv $(uv --version | awk '{print $2}') already installed"
fi

# ── repo ──────────────────────────────────────────────────────────────────────
info "repo ($BRANCH)"
mkdir -p "$DEPLOY_DIR"
chown clusterbuck:clusterbuck "$DEPLOY_DIR"

run_as_cbk() { sudo -u clusterbuck env HOME="$DEPLOY_DIR" "$@"; }

if [[ -d "$DEPLOY_DIR/.git" ]]; then
  run_as_cbk git -C "$DEPLOY_DIR" remote set-url origin "$REPO_URL"
  run_as_cbk git -C "$DEPLOY_DIR" fetch --depth 1 origin "$BRANCH"
  run_as_cbk git -C "$DEPLOY_DIR" checkout -f -B "$BRANCH" FETCH_HEAD
  ok "repo updated to $(run_as_cbk git -C "$DEPLOY_DIR" rev-parse --short HEAD)"
else
  run_as_cbk git -C "$DEPLOY_DIR" init -q
  run_as_cbk git -C "$DEPLOY_DIR" symbolic-ref HEAD refs/heads/main 2>/dev/null || true
  run_as_cbk git -C "$DEPLOY_DIR" remote add origin "$REPO_URL"
  run_as_cbk git -C "$DEPLOY_DIR" fetch --depth 1 origin "$BRANCH"
  run_as_cbk git -C "$DEPLOY_DIR" checkout -f -B "$BRANCH" FETCH_HEAD
  ok "repo cloned → $DEPLOY_DIR"
fi

# Keep runtime paths out of git status noise
excl="$DEPLOY_DIR/.git/info/exclude"
for pat in '.ssh/' 'server/.venv/' 'worker/.venv/' 'worker/build/' 'worker/dist/'; do
  grep -qxF "$pat" "$excl" 2>/dev/null || echo "$pat" >> "$excl"
done

# ── server Python venv ────────────────────────────────────────────────────────
info "server venv"
run_as_cbk env UV_NO_CONFIG=1 uv venv "$DEPLOY_DIR/server/.venv" --python python3 -q
run_as_cbk env UV_NO_CONFIG=1 uv pip install -q \
  -e "$DEPLOY_DIR/server" --python "$DEPLOY_DIR/server/.venv/bin/python"
ok "server venv ready"

# ── runtime directories and config ───────────────────────────────────────────
info "config"
install -d -m 0755 -o root       -g root       /etc/clusterbuck
install -d -m 0755 -o clusterbuck -g clusterbuck \
  /var/lib/clusterbuck \
  /var/lib/clusterbuck/cache \
  /var/lib/clusterbuck/cache/huggingface \
  /var/lib/clusterbuck/cache/tiktoken

if [[ ! -f /etc/clusterbuck/server.env ]]; then
  umask 077
  cat > /etc/clusterbuck/server.env <<EOF
CBK_API_KEY=${API_KEY}
CBK_REDIS_URL=redis://:${REDIS_PW}@127.0.0.1:6379/0
CBK_DB_PATH=/var/lib/clusterbuck/cbk.db
CBK_FLEET_PATH=/etc/clusterbuck/fleet.yaml
CBK_HOST=0.0.0.0
CBK_PORT=${PORT}
CBK_WORKER_CURRENT_VERSION=${WORKER_VERSION}
HOME=/var/lib/clusterbuck
XDG_CACHE_HOME=/var/lib/clusterbuck/cache
HF_HOME=/var/lib/clusterbuck/cache/huggingface
TIKTOKEN_CACHE_DIR=/var/lib/clusterbuck/cache/tiktoken
EOF
  chown root:root /etc/clusterbuck/server.env
  chmod 600 /etc/clusterbuck/server.env
  ok "server.env written"
else
  ok "server.env already exists (not overwritten — edit manually to change)"
fi

if [[ ! -f /etc/clusterbuck/fleet.yaml ]]; then
  cat > /etc/clusterbuck/fleet.yaml <<'EOF'
# Dynamic fleet seed — workers register themselves via enrollment.
nodes: []
capabilities: {}
EOF
  chmod 644 /etc/clusterbuck/fleet.yaml
  ok "fleet.yaml written"
fi

# ── systemd unit ──────────────────────────────────────────────────────────────
info "systemd unit"
install -m 0644 "$DEPLOY_DIR/deploy/systemd/cbk-server.service" \
  /etc/systemd/system/cbk-server.service
systemctl daemon-reload
systemctl enable cbk-server >/dev/null 2>&1 || true
systemctl restart cbk-server
sleep 2
systemctl is-active cbk-server >/dev/null || {
  journalctl -u cbk-server -n 20 --no-pager >&2
  die "cbk-server failed to start — see journal above"
}
ok "cbk-server active"

# ── LAN Redis ─────────────────────────────────────────────────────────────────
if [[ $LAN_REDIS -eq 1 ]]; then
  info "Redis LAN binding"
  sed -i 's/^bind .*/bind * -::*/' /etc/redis/redis.conf
  grep -qE '^protected-mode' /etc/redis/redis.conf || echo 'protected-mode yes' >> /etc/redis/redis.conf
  systemctl restart redis-server
  ok "Redis listening on all interfaces (auth still required)"
else
  info "Redis is loopback-only — re-run with --lan-redis once workers are ready"
fi

# ── verify ────────────────────────────────────────────────────────────────────
sleep 1
curl -fsS "http://127.0.0.1:${PORT}/healthz" >/dev/null
ok "API healthy → http://127.0.0.1:${PORT}/"

# ── summary ───────────────────────────────────────────────────────────────────
echo
printf '\033[32m━━━ clusterbuck coordinator installed ━━━\033[0m\n'
printf '  API:      http://<this-host>:%s/\n' "$PORT"
printf '  Secrets:  %s  (keep safe — holds Redis password + API key)\n' "$SECRETS_FILE"
printf '  Config:   /etc/clusterbuck/server.env\n'
echo
printf '  Next steps:\n'
printf '    1. If workers are on other machines, re-run with --lan-redis\n'
printf '    2. Mint a join token for each worker:\n'
printf '       APIKEY=$(grep -Po '"'"'(?<=CBK_API_KEY=)\S+'"'"' /etc/clusterbuck/server.env)\n'
printf '       curl -fsS -X POST http://localhost:%s/nodes/tokens \\\n' "$PORT"
printf '            -H "X-CBK-Api-Key: $APIKEY"\n'
printf '    3. Run install/worker/install.sh on each worker node\n'
printf '\033[32m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m\n'
