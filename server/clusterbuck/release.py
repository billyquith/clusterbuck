"""Publishing a worker release: one command, one record.

A release used to touch three settings that each named a version and never checked each
other (design.md → *Releasing a worker version*): `CBK_WORKER_CURRENT_VERSION` for fitness,
`CBK_WORKER_ARTIFACT` for joining nodes, and `release.json` for nodes already in the field.
Missing one was silent, and the worst combination told every node it was `stale` while
offering it nothing.

So `release.json` is now the record the other two are derived from, and publishing is a
copy plus one atomic rewrite of it:

    python -m clusterbuck.release publish dist/cbk.pyz
    python -m clusterbuck.release publish https://github.com/<owner>/<repo>/releases/download/v0.22.0/cbk.pyz

Run it on the coordinator as the service user. Deliberately a CLI, not an API endpoint:
publishing a build is code execution on every worker with auto-update on, and the API's
operator key is a credential clients hold.

The two environment variables still work, as overrides — a coordinator that sets them keeps
its behaviour. Leave them unset and they can never disagree with the manifest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

_log = logging.getLogger("clusterbuck.release")

# The one runtime id the Python worker ships as (ADR 29): a single zipapp for every OS.
RID = "py3-none-any"
_BAKED = re.compile(r'^AGENT_VERSION = "([^"]+)"', re.M)


class ReleaseError(RuntimeError):
    """A release that must not be published. The message says why."""


def read_release(release_path: str | Path | None) -> dict | None:
    """The manifest source, or None if there is none or it cannot be read."""
    if not release_path:
        return None
    try:
        return json.loads(Path(release_path).read_text())
    except (OSError, ValueError):
        return None


def released_version(release_path: str | Path | None) -> str | None:
    release = read_release(release_path)
    return str(release["version"]) if release and release.get("version") else None


def released_artifact(release_path: str | Path | None) -> Path | None:
    """The build the current manifest names, on disk — what a joining node should get.

    Resolved exactly as `GET /releases/<file>` resolves it (basename of the URL, inside the
    manifest's own directory), so the join download and the update download are the same
    bytes by construction rather than by an operator remembering to copy them twice.
    """
    release = read_release(release_path)
    art = ((release or {}).get("artifacts") or {}).get(RID)
    if not art or not art.get("url"):
        return None
    path = Path(release_path).parent / Path(urlsplit(art["url"]).path).name
    return path if path.is_file() else None


def baked_version(pyz: Path) -> str:
    """The AGENT_VERSION compiled into a built worker — what it will REPORT once running.

    Read from the artifact, never taken from a label. A manifest whose version disagrees
    with the build's own is a loop: nodes install it, report the old number, and are
    offered the same update on the next heartbeat — on Windows, a restart every beat.
    """
    try:
        with zipfile.ZipFile(pyz) as z:
            source = z.read("cbk_worker/config.py").decode()
    except (OSError, KeyError, zipfile.BadZipFile) as e:
        raise ReleaseError(f"{pyz} is not a worker build: {e}") from e
    m = _BAKED.search(source)
    if not m:
        raise ReleaseError(f"{pyz} has no AGENT_VERSION in cbk_worker/config.py")
    return m.group(1)


def _parse(v: str) -> tuple[int, ...]:
    try:
        return tuple(int(p) for p in v.split("."))
    except ValueError as e:
        raise ReleaseError(f"unparseable version {v!r}") from e


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _url_base(release: dict | None, override: str | None) -> str:
    """Where workers fetch from: the flag, else wherever the current manifest points.

    Never a default baked in here — the address a fleet reaches its coordinator at is the
    operator's to state, and guessing one is how a manifest ends up naming loopback.
    """
    if override:
        return override.rstrip("/")
    art = ((release or {}).get("artifacts") or {}).get(RID) or {}
    if art.get("url"):
        return art["url"].rsplit("/", 1)[0]
    raise ReleaseError("no existing manifest to take the download URL from: pass "
                       "--url-base, e.g. http://coordinator.local:8018/releases")


def publish(src: Path, release_path: Path, *, url_base: str | None = None,
            expected_sha256: str | None = None) -> dict:
    """Copy a build into the release directory and point the manifest at it.

    Returns the new manifest. Raises ReleaseError, having changed nothing, for anything
    that must not ship: not a worker build, a digest that does not match, or a version
    that is not strictly newer than the one released now.
    """
    release = read_release(release_path)
    version = baked_version(src)
    digest = _sha256(src)
    if expected_sha256 and digest.lower() != expected_sha256.strip().lower():
        raise ReleaseError(f"digest mismatch: expected {expected_sha256}, got {digest}")

    current = str(release["version"]) if release and release.get("version") else None
    # The coordinator and the worker both refuse to move a node backwards or sideways, so
    # a manifest that did would simply be ignored by the fleet — publishing it is always a
    # mistake, and the moment to say so is now rather than when nothing updates.
    if current is not None and _parse(version) <= _parse(current):
        raise ReleaseError(f"{src} is {version}, which is not newer than the released "
                           f"{current}")

    base = _url_base(release, url_base)
    release_dir = release_path.parent
    release_dir.mkdir(parents=True, exist_ok=True)
    name = f"cbk-{version}.pyz"
    dest = release_dir / name
    shutil.copyfile(src, dest)
    dest.chmod(0o755)
    if _sha256(dest) != digest:
        dest.unlink(missing_ok=True)
        raise ReleaseError(f"copy of {src} to {dest} does not match its source")

    new = {
        "version": version,
        "channel": (release or {}).get("channel", "stable"),
        "protocol_version": (release or {}).get("protocol_version", 1),
        "artifacts": {RID: {"url": f"{base}/{name}", "sha256": digest}},
    }
    # Previous manifest kept so a rollback is a rename, and rewritten atomically: this file
    # is read on every heartbeat, and a half-written one would stop every update.
    if release_path.exists() and current is not None:
        shutil.copy2(release_path, release_path.with_name(
            f"{release_path.name}.bak-{current}"))
    fd, tmp = tempfile.mkstemp(dir=release_dir, prefix=".release.", suffix=".json")
    with os.fdopen(fd, "w") as fh:
        json.dump(new, fh, indent=2)
        fh.write("\n")
    os.chmod(tmp, 0o664)
    os.replace(tmp, release_path)
    return new


def _fetch(url: str, into: Path) -> Path:
    import httpx

    out = into / Path(urlsplit(url).path).name
    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as resp:
        resp.raise_for_status()
        with out.open("wb") as fh:
            for chunk in resp.iter_bytes():
                fh.write(chunk)
    return out


def _published_sha256(url: str) -> str | None:
    """The `<url>.sha256` the release workflow attaches beside each build, if present."""
    import httpx

    try:
        resp = httpx.get(url + ".sha256", follow_redirects=True, timeout=30)
    except httpx.HTTPError:
        return None
    if resp.status_code != 200:
        return None
    first = resp.text.split()
    return first[0] if first else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m clusterbuck.release",
                                     description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="verb", required=True)
    pub = sub.add_parser("publish", help="publish a worker build to this coordinator")
    pub.add_argument("source", help="path to cbk.pyz, or an https:// URL to one")
    pub.add_argument("--release", default=os.environ.get("CBK_UPDATE_RELEASE"),
                     help="release.json to update (default: $CBK_UPDATE_RELEASE)")
    pub.add_argument("--url-base", help="where workers download from; default: the "
                     "current manifest's")
    pub.add_argument("--sha256", help="expected digest; for a URL it is otherwise read "
                     "from <url>.sha256, and publishing without one is refused")
    args = parser.parse_args(argv)

    if not args.release:
        print("error: no release.json — pass --release or set CBK_UPDATE_RELEASE",
              file=sys.stderr)
        return 2
    try:
        with tempfile.TemporaryDirectory() as tmp:
            expected = args.sha256
            if re.match(r"^https?://", args.source):
                expected = expected or _published_sha256(args.source)
                if not expected:
                    raise ReleaseError(f"no digest for {args.source}: none at "
                                       f"{args.source}.sha256 and no --sha256")
                src = _fetch(args.source, Path(tmp))
            else:
                src = Path(args.source)
                if not src.is_file():
                    raise ReleaseError(f"{src} not found")
            new = publish(src, Path(args.release), url_base=args.url_base,
                          expected_sha256=expected)
    except ReleaseError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    art = new["artifacts"][RID]
    print(f"published {new['version']}  sha256 {art['sha256']}\n  {art['url']}")
    print("Workers with auto-update take it on their next heartbeat; new joins get it "
          "now. No restart needed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
