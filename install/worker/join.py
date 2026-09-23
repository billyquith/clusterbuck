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
import hashlib
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


def read_password(prompt: str = "Join password: ", attempts: int = 3) -> str:
    """Prompt for the join password, rejecting what a console paste actually produced.

    `getpass` reads raw keystrokes on Windows (msvcrt.getwch), so **Ctrl+V arrives as the
    literal control character 0x16 instead of pasting** — the prompt happily accepts a
    one-character "password" and moves on. That then travels as an HTTP header, where a
    control character makes the request unparseable, so the coordinator answers
    `400 Invalid HTTP request received.` — an error that implicates the network and says
    nothing about the cause. A wrong password, by contrast, is a clean 401.

    So catch it at the only point that can explain it, and re-prompt rather than exiting:
    the alternative is re-running a join that has already done real work.

    Surrounding whitespace is stripped because password managers routinely append a
    newline, and that lands as a genuine authentication failure — 401, hours of doubting
    the right password.
    """
    interactive = sys.stdin is not None and sys.stdin.isatty()
    for remaining in range(attempts - 1, -1, -1):
        password = getpass.getpass(prompt).strip()
        if not password:
            problem = "nothing was entered"
        elif not password.isprintable():
            problem = ("it contains a control character, so the coordinator would reject "
                       "the request as malformed")
        else:
            return password

        warn(f"that is not a usable password: {problem}.")
        # Non-interactive input has no second chance to give: stdin is already exhausted,
        # and looping would spin on EOF.
        if not interactive or not remaining:
            break
        warn("  On Windows, Ctrl+V does not paste into this prompt - it sends a keystroke.")
        warn("  Paste with RIGHT-CLICK or Ctrl+Shift+V, type it by hand, or set")
        warn("  CBK_JOIN_PASSWORD in the environment and re-run.")
    die("no usable join password")


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


def verify_artifact(path: Path, cfg: dict, pubkey_pem: str | None) -> None:
    """Check the downloaded artifact before anything installs it as a service.

    This file is about to be run as a long-lived service, usually as root. Until now the
    only check was `st_size != 0`, while the UPDATE channel that patches the very same
    binary afterwards verifies an ECDSA signature and a digest before writing a byte. The
    bootstrap was the soft underside of a hard shell: a LAN on-path attacker (ARP, DNS or
    mDNS spoofing of the coordinator's name, all easy against the plain-HTTP default) got
    root-installed code execution on every joining machine.

    Two levels, and they defend different things:

    * **Digest** — always, when the coordinator publishes one. Catches a corrupted or
      truncated download and a swapped file on the coordinator's disk. It does NOT stop a
      full on-path attacker, who controls the bootstrap response and the artifact alike.
    * **Signature** — only with `--pubkey-file`, and this is the real defence. The key
      travels out of band, exactly as the join password does, so it does not matter who
      controls the channel.

    A coordinator too old to publish either is accepted with a warning rather than
    refused: it is the status quo, and failing the join outright would strand fleets
    mid-upgrade. Say so loudly instead.
    """
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    published = cfg.get("artifact_sha256")

    if not published:
        warn("this coordinator published no artifact digest, so the download could not "
             "be verified.\n  Upgrade the coordinator to get integrity checking here.")
        return

    if digest != published:
        die("the downloaded artifact does not match the digest the coordinator "
            f"published.\n  expected {published}\n  got      {digest}\n"
            "  Refusing to install it. This is either a corrupted download or a "
            "tampered one.")
    ok("artifact digest matches the coordinator's")

    signature = cfg.get("artifact_signature")
    if pubkey_pem is None:
        if signature:
            warn("the coordinator signed this artifact, but no --pubkey-file was given, "
                 "so the signature was not checked.\n  The digest above came down the "
                 "same connection as the artifact, so it does not protect against an "
                 "on-path attacker. Pass --pubkey-file for a real guarantee.")
        return
    if not signature:
        die("--pubkey-file was given but the coordinator published no signature.\n"
            "  Configure CBK_UPDATE_SIGNING_KEY on the coordinator, or drop the flag to "
            "accept digest-only verification.")

    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec
    except ImportError:
        die("--pubkey-file needs the `cryptography` package on this machine:\n"
            "    python3 -m pip install cryptography")

    import base64
    payload = "\n".join(
        ["cbk-bootstrap-artifact-v1", digest, cfg.get("artifact_version") or ""]
    ).encode()
    try:
        serialization.load_pem_public_key(pubkey_pem.encode()).verify(
            base64.b64decode(signature), payload, ec.ECDSA(hashes.SHA256()))
    except Exception:
        die("the artifact's signature does NOT verify against the supplied public key.\n"
            "  Refusing to install it. Either the key is the wrong one, or this is not "
            "the coordinator you think it is.")
    ok("artifact signature verifies against the supplied key")


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
    model_server: str, model_manager: str, profile: str, artifact: Path, token: str,
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
            "-Profile", profile,
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
        "--profile", profile,
        "--artifact", str(artifact),
        "--token", token,
    ]


def installer_env(pubkey_pem: str | None = None) -> dict[str, str] | None:
    """Environment for the platform installer.

    Also how the update-signing public key reaches it, when one was supplied. In the
    environment rather than argv deliberately: argv is world-readable through `ps` for
    the lifetime of the call. A public key is not secret, but this is the hook the next
    secret would be bolted onto, so it starts in the right place.

    `install.ps1` runs under Windows PowerShell 5.1, which autoloads its core modules from
    PSModulePath. Launch this script from PowerShell 7 and that variable is inherited
    pointing at PS7's module directories — 5.1 then loads PS7's
    Microsoft.PowerShell.Security over its own already-registered types, fails with "The
    member Sddl is already present", and takes Get-Acl down with it. The installer needs
    Get-Acl to restrict worker.env, which holds the Redis password.

    Dropping the inherited value lets 5.1 compute its own default module path.
    """
    if platform.system() != "Windows":
        # Nothing to repair, so inherit — unless we have a key to hand down, in which
        # case the environment has to be materialised to carry it.
        if not pubkey_pem:
            return None
        return {**os.environ, "CBK_UPDATE_PUBKEY_PEM": pubkey_pem}
    # dict(os.environ) upper-cases its keys on Windows, so a plain pop("PSModulePath")
    # silently matches nothing. Drop every spelling.
    env = {k: v for k, v in os.environ.items() if k.lower() != "psmodulepath"}
    if pubkey_pem:
        env["CBK_UPDATE_PUBKEY_PEM"] = pubkey_pem
    return env


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
    ap.add_argument("--pubkey-file", default=os.environ.get("CBK_UPDATE_PUBKEY_FILE"),
                    help="PEM public key to verify the downloaded artifact against. "
                         "Carry it out of band, like the join password — a digest served "
                         "over the same connection as the artifact cannot protect against "
                         "an on-path attacker, and a signature can. Also installed on the "
                         "node so its self-update channel works.")
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
    ap.add_argument("--model-manager", default="auto",
                    choices=["auto", "ollama", "lmstudio", "none"])
    # Only settable at enrolment, and it decides whether `cbk pause` drains this node or
    # evicts its running job and its resident models — so a dedicated box joined without
    # it gets the shared default and gives back a machine nobody wanted back.
    ap.add_argument("--profile", default="shared",
                    choices=["dedicated", "shared", "background"],
                    help="who this machine is for (default: shared)")
    ap.add_argument("--node-state",
                    help="path the installer persists node identity to "
                         "(default: the platform location; override for testing)")
    ap.add_argument("--dry-run", action="store_true",
                    help="fetch and report, but do not install anything")
    args = ap.parse_args(argv)

    if sys.version_info < (3, 11):
        die(f"Python 3.11+ required (running {platform.python_version()})")

    # An explicit --password/CBK_JOIN_PASSWORD is taken as given: it came from a script
    # or a shell, not from a prompt that mangles pastes.
    password = (args.password or "").strip() or read_password()

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

    pubkey_pem = None
    if args.pubkey_file:
        pk = Path(args.pubkey_file)
        if not pk.is_file():
            die(f"no such public key file: {pk}")
        pubkey_pem = pk.read_text()

    staging = Path(tempfile.mkdtemp(prefix="cbk-join-")) / "cbk.pyz"
    info("downloading the coordinator's worker build")
    fetch_artifact(args.coordinator, password, staging)
    ok(f"artifact → {staging} ({staging.stat().st_size // 1024} KiB)")
    # Before the installer runs it as a service, not after.
    verify_artifact(staging, cfg, pubkey_pem)

    cmd = installer_command(
        repo_root,
        coordinator=args.coordinator,
        redis_url=cfg["redis_url"],
        model=args.model,
        model_server=args.model_server,
        model_manager=args.model_manager,
        profile=args.profile,
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
    result = subprocess.run(cmd, env=installer_env(pubkey_pem))
    if result.returncode != 0:
        die(f"the platform installer failed (exit {result.returncode})")

    check_capabilities(registry, state)
    ok("joined")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
