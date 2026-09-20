#!/usr/bin/env python3
"""Join this machine to a clusterbuck fleet as a worker.

One command, one secret. Everything else is fetched from the coordinator:

    sudo python3 join.py --coordinator http://coordinator.local:8018 --model qwen2.5:7b

**Why this exists.** Onboarding a worker by hand meant building the artifact somewhere,
copying it across, minting a join token with the operator key, and passing the Redis URL
*including its password* as a command-line argument — so two secrets were hand-carried
onto every new machine and one of them landed in shell history. This script carries one
password, which it exchanges with the coordinator for a single-use join token and the
broker URL.

**The operator key never leaves the coordinator.** A worker has no business holding it: it
mints join tokens, approves model installs and deletes models. A worker needs a join token
once, then authenticates with its own per-node key. See ADR 26.

**The artifact comes from the coordinator, not a local build.** Every node then runs the
same blessed build, and nobody is running whatever their own checkout happened to contain.

Platform-specific service installation is deliberately NOT reimplemented here — this hands
off to `install.sh` / `install.ps1`, which already create the systemd unit, launchd plist
or Scheduled Task and are proven in the field. New logic lives here; OS plumbing stays
where it works.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import platform
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

HEADER = "X-CBK-Join-Password"
TIMEOUT_S = 30

# Where the platform installers persist node identity. Read after install so the proposed
# capabilities can be checked against the coordinator's registry; kept in sync with
# install.sh's VAR_DIR and install.ps1's $VarDir.
NODE_STATE = {
    "windows": Path(os.environ.get("ProgramData", r"C:\ProgramData"))
    / "clusterbuck" / "data" / "node.json",
}
NODE_STATE_DEFAULT = Path("/var/lib/clusterbuck/node.json")


def info(msg: str) -> None:
    print(f"\033[36m[join]\033[0m {msg}")


def ok(msg: str) -> None:
    print(f"\033[32m[join] ✓\033[0m {msg}")


def warn(msg: str) -> None:
    print(f"\033[33m[join] WARN:\033[0m {msg}")


def die(msg: str) -> "NoReturn":  # noqa: F821
    print(f"\033[31m[join] ERROR:\033[0m {msg}", file=sys.stderr)
    raise SystemExit(1)


def _post_json(url: str, password: str) -> dict:
    req = urllib.request.Request(url, data=b"", method="POST",
                                 headers={HEADER: password})
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
        return json.loads(resp.read())


def bootstrap(coordinator: str, password: str) -> dict:
    """Exchange the join password for a one-time token and the broker URL."""
    url = f"{coordinator.rstrip('/')}/nodes/bootstrap"
    try:
        return _post_json(url, password)
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:200]
        if e.code == 404:
            die("this coordinator has worker bootstrap disabled.\n"
                "  Set CBK_JOIN_PASSWORD on the coordinator (16+ characters) and restart "
                "it, or fall back to install.sh with an explicit --token and --redis-url.")
        if e.code == 401:
            die("the coordinator rejected the join password.")
        die(f"bootstrap failed: HTTP {e.code} {body}")
    except urllib.error.URLError as e:
        die(f"cannot reach {url}: {e.reason}\n"
            "  Check the address, and that the coordinator is listening.")


def fetch_artifact(coordinator: str, password: str, dest: Path) -> Path:
    """Download the coordinator's blessed `cbk.pyz`."""
    url = f"{coordinator.rstrip('/')}/worker/artifact"
    req = urllib.request.Request(url, headers={HEADER: password})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
            dest.write_bytes(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            die("the coordinator has no worker artifact to serve.\n"
                "  Build it and point CBK_WORKER_ARTIFACT at it on the coordinator:\n"
                "    cd worker && uv run python build.py   # -> dist/cbk.pyz")
        die(f"artifact download failed: HTTP {e.code}")
    except urllib.error.URLError as e:
        die(f"artifact download failed: {e.reason}")
    if dest.stat().st_size == 0:
        die("the coordinator served an empty artifact")
    return dest


def installer_command(
    repo_root: Path, *, coordinator: str, redis_url: str, model: str,
    model_server: str, model_manager: str, artifact: Path, token: str,
    system: str | None = None,
) -> list[str]:
    """Full argv for the platform installer.

    The two installers do NOT share an argument style, and getting this wrong fails only
    on the platform you did not test on: `install.sh` takes GNU-style `--coordinator`,
    while `install.ps1` declares PowerShell parameters (`-CoordinatorUrl`, `-RedisUrl`,
    `-ModelServerUrl`) which will not bind a `--double-dash` name at all. So the flavour
    is built per platform rather than shared.

    `system` is injectable so both flavours are testable from one machine.
    """
    here = repo_root / "install" / "worker"
    on_windows = (system or platform.system()) == "Windows"
    script = here / ("install.ps1" if on_windows else "install.sh")
    if not script.is_file():
        die(f"installer not found: {script}")

    if on_windows:
        return [
            "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script),
            "-CoordinatorUrl", coordinator,
            "-RedisUrl", redis_url,
            "-Model", model,
            "-ModelServerUrl", model_server,
            "-ModelManager", model_manager,
            "-Artifact", str(artifact),
            "-Token", token,
        ]
    return [
        "bash", str(script),
        "--coordinator", coordinator,
        "--redis-url", redis_url,
        "--model", model,
        "--model-server", model_server,
        "--model-manager", model_manager,
        "--artifact", str(artifact),
        "--token", token,
    ]


def node_state_path() -> Path:
    return NODE_STATE.get(platform.system().lower(), NODE_STATE_DEFAULT)


def check_capabilities(registry: list[str], state: Path | None = None) -> None:
    """Warn if this node will serve a tier the coordinator does not know about.

    This is the failure worth catching at the one moment somebody is watching the output:
    enrolment succeeds, heartbeats look healthy, `fitness: ok` — and no job ever routes,
    because routing resolves against the coordinator's registry and a tier missing from it
    can never be selected.

    Capabilities are proposed from probed RAM, so a large machine will routinely be offered
    tiers a small fleet has never defined.
    """
    state = state or node_state_path()
    if not state.is_file():
        warn(f"no node state at {state} — skipping the capability check")
        return
    try:
        served = json.loads(state.read_text()).get("capabilities") or []
    except (OSError, ValueError) as e:
        warn(f"could not read {state} ({e}) — skipping the capability check")
        return

    missing = [c for c in served if c not in registry]
    if not missing:
        ok(f"capabilities {served} are all in the coordinator's registry")
        return

    warn(f"this node will serve {missing}, which is NOT in the coordinator's registry.")
    warn("Jobs will not route to those tiers until they are defined there. Enrolment and")
    warn("heartbeats will still look healthy, so this is easy to miss.")
    warn("")
    warn("  On the coordinator, either add them to fleet.yaml and restart, or narrow this")
    warn(f"  node to a known tier:  CBK_CAPABILITIES={','.join(registry) or '<tier>'}")
    warn(f"  Known to the coordinator: {registry or '(none defined)'}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="join.py",
        description="Join this machine to a clusterbuck fleet as a worker.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--coordinator", required=True,
                    help="coordinator URL, e.g. http://coordinator.local:8018")
    ap.add_argument("--model", required=True,
                    help="model this node advertises, e.g. qwen2.5:7b")
    # Prefer the prompt or the env var: a password passed as an argument lands in shell
    # history, which is the exact problem this whole flow removes for the Redis
    # credential. `--password` is the automation escape hatch, not the default path.
    ap.add_argument("--password", default=os.environ.get("CBK_JOIN_PASSWORD"),
                    help="join password. PREFER omitting this and letting it prompt, or "
                         "CBK_JOIN_PASSWORD — an argument lands in shell history")
    ap.add_argument("--model-server", default="http://127.0.0.1:11434/v1",
                    help="local model server (default: Ollama; LM Studio is :1234)")
    ap.add_argument("--model-manager", default="auto", choices=["auto", "ollama", "none"])
    ap.add_argument("--node-state",
                    help="path the installer persists node identity to "
                         "(default: the platform location; override for testing)")
    ap.add_argument("--dry-run", action="store_true",
                    help="fetch and report, but do not install anything")
    args = ap.parse_args(argv)

    if sys.version_info < (3, 11):
        die(f"Python 3.11+ required (running {platform.python_version()})")

    password = args.password or getpass.getpass("Join password: ")
    if not password:
        die("a join password is required")

    repo_root = Path(__file__).resolve().parents[2]
    state = Path(args.node_state) if args.node_state else None

    info(f"asking {args.coordinator} to bootstrap this node")
    cfg = bootstrap(args.coordinator, password)
    for field in ("join_token", "redis_url"):
        if not cfg.get(field):
            die(f"the coordinator's bootstrap response is missing {field!r}")
    registry = cfg.get("capabilities") or []
    ok("got a single-use join token and the broker URL "
       "(the operator key stayed on the coordinator)")

    staging = Path(tempfile.mkdtemp(prefix="cbk-join-")) / "cbk.pyz"
    info("downloading the coordinator's worker build")
    fetch_artifact(args.coordinator, password, staging)
    ok(f"artifact → {staging} ({staging.stat().st_size // 1024} KiB)")

    cmd = installer_command(
        repo_root,
        coordinator=args.coordinator,
        redis_url=cfg["redis_url"],
        model=args.model,
        model_server=args.model_server,
        model_manager=args.model_manager,
        artifact=staging,
        token=cfg["join_token"],
    )

    if args.dry_run:
        # The redis URL carries a password, so show the argv with it redacted rather than
        # printing a command a reader might paste into a shell (and into history).
        shown = list(cmd)
        shown[shown.index(cfg["redis_url"])] = "redis://:<password>@…"
        shown[shown.index(cfg["join_token"])] = "<join-token>"
        info("dry run — would hand off to:")
        print("   ", " ".join(shown))
        check_capabilities(registry, state)
        return 0

    info("handing off to the platform installer (service, config, enrolment)")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        die(f"the platform installer failed (exit {result.returncode})")

    check_capabilities(registry, state)
    ok("joined")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
