# Installation

Automated scripts live in [`install/`](../install/):

| Script | Platform | Purpose |
|---|---|---|
| `install/coordinator/install.sh` | Linux (Debian/Ubuntu/Pi OS) | Coordinator — full unattended setup |
| `install/coordinator/install.ps1` | Windows (Docker Desktop for Redis) | Coordinator |
| `install/worker/install.sh` | Linux / macOS | Worker — binary, config, service, enrollment |
| `install/worker/install.ps1` | Windows | Worker |

**Coordinator quick-start (Linux):**
```bash
curl -fsSL https://raw.githubusercontent.com/billyquith/clusterbuck/main/install/coordinator/install.sh \
  | sudo bash -s -- --lan-redis
```

**Worker quick-start (Linux/macOS) — after the coordinator is up:**
```bash
# Build the zipapp on the coordinator first:
#   cd /opt/clusterbuck/worker && python3 build.py   → dist/cbk.pyz
#   scp dist/cbk.pyz worker-host:/tmp/

sudo bash install/worker/install.sh \
  --coordinator http://COORDINATOR_HOST:8018 \
  --redis-url   'redis://:REDIS_PW@COORDINATOR_HOST:6379/0' \
  --model       qwen2.5:7b \
  --artifact    /tmp/cbk.pyz \
  --token       JOIN_TOKEN
```

The rest of this document covers the same steps manually, which is useful when you need
more control or are troubleshooting. See [deployment.md](deployment.md) for background on
the split architecture and hardware sizing.

## What you're setting up

clusterbuck has two components on two kinds of machines:

- **Coordinator** — always-on, low-power (a Raspberry Pi is ideal). Runs the job API,
  queue broker (Redis), sync gateway, and fleet manager. Workers register with it and pull
  jobs through it. One per deployment.
- **Worker** — a machine with a capable model server (typically Ollama). Pulls jobs from
  the coordinator's queues and runs inference locally. Add as many as you have.

Both communicate over your LAN. No cloud dependency unless you opt in for overflow.

---

## Hardware requirements

### Coordinator

Any always-on machine works. The coordinator is pure I/O — queue ops, HTTP, Wake-on-LAN
packets — so it needs very little power or RAM.

- Raspberry Pi 4/5 (recommended): 64-bit, 4 GB+ RAM, a reliable SD card or USB SSD
- Any Linux box with Python 3.12+ and enough RAM to run Redis (~50 MB) and the server (~150 MB)

The coordinator **should not** be your inference machine. Keep it always-on and
unobtrusive; let the beefy machines sleep.

### Worker nodes

Workers do the actual inference, so sizing is driven by the models you want to run:

| Model size | Minimum RAM | Notes |
|---|---|---|
| 3–8 B | 8 GB | CPU-only is slow but works |
| 14–32 B | 24 GB | Metal/CUDA strongly recommended |
| 70 B+ | 48 GB+ | High-end GPU or Apple Silicon |

Rule of thumb: the model's weights in GB ≈ the RAM you need (quantised). A 14 B Q4 model
is ~8 GB; a 70 B Q4 is ~40 GB. Ollama will refuse to load a model that doesn't fit.

A **Mac with Apple Silicon** is an excellent worker: unified memory is fast, Metal
acceleration is automatic, and the machine can sleep between jobs (Wake-on-LAN works on
macOS). A Pi is not useful as an inference worker — too little RAM, no GPU.

---

## Software requirements

### Coordinator

- Linux (Debian/Ubuntu recommended; Pi OS works)
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (Python package/environment manager)
- Redis 7+ (`apt install redis-server`)
- Git (to clone the repo and receive updates)

### Worker nodes

- Python 3.11+ (already present on most systems)
- An OpenAI-compatible model server — [Ollama](https://ollama.com) is the default
- No build tools, no SDK — the worker ships as a self-contained Python zipapp (~2.8 MB)

---

## Coordinator setup

Run all of the following as root (or with `sudo`) on the coordinator machine.

### 1. Create the clusterbuck system user

```bash
useradd -r -m -d /opt/clusterbuck -s /usr/sbin/nologin clusterbuck
```

`/opt/clusterbuck` is the user's home and the deployment root. The service files and
install scripts assume this path.

### 2. Install Redis and generate secrets

```bash
apt-get update && apt-get install -y redis-server
```

Mint a Redis password and API key — do this once and keep the file safe:

```bash
umask 077
mkdir -p /etc/clusterbuck /root/.cbk-secrets-dir  # or wherever you keep root secrets
REDIS_PW=$(openssl rand -hex 24)
API_KEY=$(openssl rand -base64 32 | tr -d '\n')
cat > /root/.cbk-secrets <<EOF
REDIS_PW='${REDIS_PW}'
API_KEY='${API_KEY}'
EOF
chmod 600 /root/.cbk-secrets
```

Set the Redis password:

```bash
source /root/.cbk-secrets
# Find requirepass in /etc/redis/redis.conf and set it (or append if missing):
grep -qE '^requirepass ' /etc/redis/redis.conf \
  && sed -i "s|^requirepass .*|requirepass ${REDIS_PW}|" /etc/redis/redis.conf \
  || echo "requirepass ${REDIS_PW}" >> /etc/redis/redis.conf
systemctl enable --now redis-server && systemctl restart redis-server
# Verify:
redis-cli -a "$REDIS_PW" ping   # should return PONG
```

### 3. Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
uv --version
```

### 4. Clone the repo

The coordinator runs the server from a git checkout so updates are a `git pull`. Use a
read-only deploy key so the `clusterbuck` service account can fetch without your personal
credentials.

Generate the deploy key (as root, writing into the clusterbuck home):

```bash
sudo -u clusterbuck env HOME=/opt/clusterbuck \
  ssh-keygen -t ed25519 -N '' -C 'clusterbuck-deploy' \
  -f /opt/clusterbuck/.ssh/id_deploy_ed25519
cat /opt/clusterbuck/.ssh/id_deploy_ed25519.pub   # add this as a read-only deploy key in GitHub
```

Clone into `/opt/clusterbuck` (in-place, around the existing `.ssh`):

```bash
GSC="ssh -i /opt/clusterbuck/.ssh/id_deploy_ed25519 -o IdentitiesOnly=yes -o UserKnownHostsFile=/opt/clusterbuck/.ssh/known_hosts -o StrictHostKeyChecking=accept-new"
sudo -u clusterbuck env HOME=/opt/clusterbuck GIT_SSH_COMMAND="$GSC" \
  git -C /opt/clusterbuck init -q -b main
sudo -u clusterbuck env HOME=/opt/clusterbuck GIT_SSH_COMMAND="$GSC" \
  git -C /opt/clusterbuck remote add origin git@github.com:billyquith/clusterbuck.git
sudo -u clusterbuck env HOME=/opt/clusterbuck GIT_SSH_COMMAND="$GSC" \
  git -C /opt/clusterbuck fetch --depth 1 origin main
sudo -u clusterbuck env HOME=/opt/clusterbuck \
  git -C /opt/clusterbuck checkout -f -B main FETCH_HEAD
# Pin the deploy key so future git pulls use it automatically:
sudo -u clusterbuck env HOME=/opt/clusterbuck \
  git -C /opt/clusterbuck config core.sshCommand \
  'ssh -i /opt/clusterbuck/.ssh/id_deploy_ed25519 -o IdentitiesOnly=yes -o UserKnownHostsFile=/opt/clusterbuck/.ssh/known_hosts'
```

### 5. Set up the server Python environment

```bash
source /root/.cbk-secrets
sudo -u clusterbuck env HOME=/opt/clusterbuck UV_NO_CONFIG=1 \
  uv venv /opt/clusterbuck/server/.venv --python python3
sudo -u clusterbuck env HOME=/opt/clusterbuck UV_NO_CONFIG=1 \
  uv pip install -e "/opt/clusterbuck/server[dev]" \
  --python /opt/clusterbuck/server/.venv/bin/python
```

### 6. Create runtime directories and write config

```bash
source /root/.cbk-secrets
install -d -m 0755 -o root -g root /etc/clusterbuck
install -d -m 0755 -o clusterbuck -g clusterbuck \
  /var/lib/clusterbuck \
  /var/lib/clusterbuck/cache \
  /var/lib/clusterbuck/cache/huggingface \
  /var/lib/clusterbuck/cache/tiktoken
```

Write `/etc/clusterbuck/server.env` (copy from the template and fill in your secrets):

```bash
source /root/.cbk-secrets
umask 077
cat > /etc/clusterbuck/server.env <<EOF
CBK_API_KEY=${API_KEY}
CBK_REDIS_URL=redis://:${REDIS_PW}@127.0.0.1:6379/0
CBK_DB_PATH=/var/lib/clusterbuck/cbk.db
CBK_FLEET_PATH=/etc/clusterbuck/fleet.yaml
CBK_HOST=0.0.0.0
CBK_PORT=8018
CBK_WORKER_CURRENT_VERSION=0.7.0
HOME=/var/lib/clusterbuck
XDG_CACHE_HOME=/var/lib/clusterbuck/cache
HF_HOME=/var/lib/clusterbuck/cache/huggingface
TIKTOKEN_CACHE_DIR=/var/lib/clusterbuck/cache/tiktoken
EOF
chown root:root /etc/clusterbuck/server.env
chmod 600 /etc/clusterbuck/server.env
```

Write a minimal fleet seed (workers join dynamically via enrollment):

```bash
cat > /etc/clusterbuck/fleet.yaml <<'EOF'
nodes: []
capabilities: {}
EOF
chmod 644 /etc/clusterbuck/fleet.yaml
```

See `deploy/systemd/server.env.example` for the full set of optional settings (self-update
signing, Wake-on-LAN broadcast address, cloud fallback, etc.).

### 7. Install and start the coordinator service

```bash
install -m 0644 /opt/clusterbuck/deploy/systemd/cbk-server.service \
  /etc/systemd/system/cbk-server.service
systemctl daemon-reload
systemctl enable --now cbk-server
```

Verify:

```bash
systemctl is-active cbk-server           # should print "active"
curl http://127.0.0.1:8018/healthz       # should return {"status":"ok"}
journalctl -u cbk-server -n 20 --no-pager
```

### 8. Expose Redis on the LAN (required for remote workers)

By default Redis binds only to loopback. Workers on other machines need to reach it:

```bash
sed -i 's/^bind .*/bind * -::*/' /etc/redis/redis.conf
grep -qE '^protected-mode' /etc/redis/redis.conf || echo 'protected-mode yes' >> /etc/redis/redis.conf
systemctl restart redis-server
# Verify remote workers can reach it (auth still required):
ss -ltnp | grep ':6379'
```

The password (`requirepass`) and `protected-mode yes` stay on — this only opens the port,
not the data.

---

## Worker setup

Run the following on each worker node. Steps 1–5 require root; step 6 onward can be done
as any user with `sudo`.

### 1. Install Python and Ollama

Python 3.11+ is required. Check first:

```bash
python3 --version
```

If missing: `apt install python3` (Debian/Ubuntu) or use your OS package manager.

Install [Ollama](https://ollama.com/download) and pull at least one model:

```bash
curl -fsSL https://ollama.com/install.sh | sh
ollama pull qwen2.5:7b   # or whichever model this node will serve
```

Ollama must be running and reachable at `http://127.0.0.1:11434` before the worker starts.

### 2. Create the clusterbuck user and directories

```bash
useradd -r -m -d /opt/clusterbuck -s /usr/sbin/nologin clusterbuck
install -d -m 0755 -o root -g root /etc/clusterbuck
install -d -m 0755 -o clusterbuck -g clusterbuck /var/lib/clusterbuck
```

### 3. Deploy the worker binary

The worker is a single self-contained Python zipapp. Build it on the coordinator (or in CI)
and copy it to each worker node:

```bash
# On the coordinator:
cd /opt/clusterbuck/worker && python3 build.py   # → dist/cbk.pyz
scp dist/cbk.pyz WORKER_HOST:/tmp/cbk.pyz

# On the worker node:
install -o clusterbuck -g clusterbuck -m 0755 /tmp/cbk.pyz /opt/clusterbuck/cbk
rm /tmp/cbk.pyz
```

The service file's `ExecStart=/opt/clusterbuck/cbk work` expects the binary at exactly
this path, regardless of whether it was built from the zipapp or any future packaging.

For self-update to work, `cryptography` must be installed in the Python interpreter that
runs the zipapp:

```bash
pip install cryptography   # or: uv pip install cryptography --system
```

Without it the worker runs normally but refuses to apply signed updates.

### 4. Configure the worker

On the **coordinator**, generate a `worker.env` with the real Redis password:

```bash
source /root/.cbk-secrets
cat > /tmp/worker.env <<EOF
CBK_REDIS_URL=redis://:${REDIS_PW}@COORDINATOR_HOST:6379/0
CBK_SERVER_URL=http://COORDINATOR_HOST:8018
CBK_MODEL_SERVER_URL=http://127.0.0.1:11434/v1
CBK_MODEL=qwen2.5:7b
CBK_MODEL_MANAGER=ollama
CBK_NODE_STATE=/var/lib/clusterbuck/node.json
EOF
chmod 600 /tmp/worker.env
```

Replace `COORDINATOR_HOST` with the coordinator's hostname or IP (mDNS names like
`coordinator.local` work well on a LAN). Replace `CBK_MODEL` with the model you pulled
in step 1.

Transfer to the worker and install:

```bash
scp /tmp/worker.env WORKER_HOST:/tmp/worker.env && rm /tmp/worker.env
# On the worker node:
install -o root -g root -m 0600 /tmp/worker.env /etc/clusterbuck/worker.env
rm /tmp/worker.env
```

See `deploy/systemd/worker.env.example` for the full set of optional settings.

### 5. Install the worker service

```bash
# Copy the service file from the coordinator, or scp it from the repo checkout:
install -m 0644 /path/to/clusterbuck/deploy/systemd/cbk-worker.service \
  /etc/systemd/system/cbk-worker.service
systemctl daemon-reload
```

Do **not** start it yet — the node must enroll first.

### 6. Enroll the worker with the coordinator

Enrollment is a one-time handshake. The coordinator issues a single-use join token; the
worker exchanges it for a persistent node key it uses for all future heartbeats.

**On the coordinator** — mint a join token:

```bash
APIKEY=$(sudo grep -Po '(?<=CBK_API_KEY=)\S+' /etc/clusterbuck/server.env)
curl -fsS -X POST http://127.0.0.1:8018/nodes/tokens \
  -H "X-CBK-Api-Key: $APIKEY" | python3 -c "import sys,json; print(json.load(sys.stdin)['join_token'])"
```

**On the worker node** — enroll using that token:

```bash
export CBK_SERVER_URL=http://COORDINATOR_HOST:8018
/opt/clusterbuck/cbk enroll --token JOIN_TOKEN_HERE
```

The worker probes its hardware, registers with the coordinator, and writes its node
identity to `/var/lib/clusterbuck/node.json`. The token is consumed and cannot be reused.

### 7. Start the worker service

```bash
systemctl enable --now cbk-worker
```

Verify on the coordinator:

```bash
APIKEY=$(sudo grep -Po '(?<=CBK_API_KEY=)\S+' /etc/clusterbuck/server.env)
curl -fsS http://127.0.0.1:8018/nodes -H "X-CBK-Api-Key: $APIKEY" \
  | python3 -m json.tool | grep -E '"hostname|fitness|agent_version|mode"'
```

The node should appear with `"fitness": "ok"` within a few seconds of the first heartbeat.

---

## Verifying the deployment

### Health check

```bash
curl http://COORDINATOR_HOST:8018/healthz   # {"status":"ok"}
```

### Submit a test job

```bash
APIKEY=your-api-key
curl -X POST http://COORDINATOR_HOST:8018/jobs \
  -H "X-CBK-Api-Key: $APIKEY" \
  -H "Content-Type: application/json" \
  -d '{"capability":"8b-extract","messages":[{"role":"user","content":"say hello"}],"urgency":"waitable"}'
# Returns {"id":"job-...","status":"queued",...}
```

Poll for the result:

```bash
curl http://COORDINATOR_HOST:8018/jobs/JOB_ID -H "X-CBK-Api-Key: $APIKEY"
```

The dashboard at `http://COORDINATOR_HOST:8018/` shows live queue depth, fleet state, and
usage metrics.

---

## Updating

### Coordinator

```bash
sudo -u clusterbuck env HOME=/opt/clusterbuck git -C /opt/clusterbuck pull
# Rebuild the server venv only if server/pyproject.toml changed:
sudo -u clusterbuck env HOME=/opt/clusterbuck UV_NO_CONFIG=1 \
  uv pip install -e "/opt/clusterbuck/server[dev]" \
  --python /opt/clusterbuck/server/.venv/bin/python
sudo systemctl restart cbk-server
# The DB migration (if any) runs automatically on startup.
```

Update `CBK_WORKER_CURRENT_VERSION` in `/etc/clusterbuck/server.env` to match the new
worker release so the fleet governance flags nodes that need updating.

### Workers

Build a new zipapp on the coordinator, copy it to each worker, and restart:

```bash
# On coordinator:
cd /opt/clusterbuck/worker && python3 build.py
scp dist/cbk.pyz WORKER_HOST:/tmp/cbk.pyz

# On worker:
sudo install -o clusterbuck -g clusterbuck -m 0755 /tmp/cbk.pyz /opt/clusterbuck/cbk
sudo systemctl restart cbk-worker
```

If self-update is configured (signing key + release manifest set in `server.env`), the
coordinator can push updates to workers automatically on their next heartbeat.
