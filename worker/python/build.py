#!/usr/bin/env python3
"""Build the shipped artifact: a single platform-independent zipapp, `dist/cbk.pyz`.

One artifact for every OS and architecture. The .NET worker needs six per-platform builds at
72–81 MB each and cannot use Native AOT on macOS at all (ADR 29); this is the same worker in
one file that runs anywhere Python 3.11+ does.

Vendors this package plus its pure-Python dependencies, so the signed artifact IS the code
that runs — the update signature covers all of it. `cryptography` is deliberately excluded:
it is needed only to VERIFY an update, so it cannot be delivered through the channel it
secures, and it is the one dependency with compiled wheels (which would make the artifact
platform-specific, defeating the point). Absent, self-update refuses; inference still works.

Usage:  python build.py [--out dist]
"""

from __future__ import annotations

import argparse
import compileall
import hashlib
import shutil
import subprocess
import sys
import zipapp
from pathlib import Path

HERE = Path(__file__).resolve().parent
# Vendored into the artifact. All pure-Python: a compiled wheel would pin the artifact to one
# platform and there would be nothing left to distinguish it from the .NET build's six.
VENDORED = ["redis", "httpx"]

ENTRY = '''\
"""zipapp entry point. Keeps `python cbk.pyz <verb>` identical to `cbk <verb>`."""
import sys

from cbk_worker.__main__ import main

sys.exit(main())
'''


def _install(staging: Path, packages: list[str]) -> None:
    """Vendor `packages` into `staging`, using whichever installer this environment has.

    uv-created venvs ship without pip, and CI may have either, so try both rather than
    assuming. Both are asked for wheels only: building from source could produce a compiled
    extension and quietly make the artifact platform-specific.
    """
    attempts = [
        ["uv", "pip", "install", "--quiet", "--target", str(staging), *packages],
        [sys.executable, "-m", "pip", "install", "--quiet", "--no-compile",
         "--only-binary", ":all:", "--target", str(staging), *packages],
    ]
    errors = []
    for cmd in attempts:
        if shutil.which(cmd[0]) is None and cmd[0] != sys.executable:
            continue
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return
        errors.append(f"{cmd[0]}: {result.stderr.strip()[:300]}")
    raise SystemExit("could not vendor dependencies:\n  " + "\n  ".join(errors))


def build(out_dir: Path) -> Path:
    staging = HERE / "build" / "pyz"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    print(f"vendoring {', '.join(VENDORED)} …")
    _install(staging, VENDORED)

    print("adding cbk_worker …")
    shutil.copytree(HERE / "src" / "cbk_worker", staging / "cbk_worker")
    (staging / "__main__.py").write_text(ENTRY)

    # Strip what only matters at install time. dist-info metadata is not read at runtime by
    # anything we ship, and the caches are rebuilt per interpreter anyway.
    for pattern in ("*.dist-info", "*.egg-info", "__pycache__", "bin"):
        for path in staging.rglob(pattern):
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
    for path in staging.rglob("*.pyc"):
        path.unlink(missing_ok=True)

    compileall.compile_dir(str(staging), quiet=2, force=True)

    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "cbk.pyz"
    zipapp.create_archive(staging, target=target,
                          interpreter="/usr/bin/env python3", compressed=True)
    target.chmod(0o755)

    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    (out_dir / "cbk.pyz.sha256").write_text(f"{digest}  cbk.pyz\n")
    size_mb = target.stat().st_size / 1_048_576
    print(f"\n{target}  {size_mb:.1f} MB  (py3-none-any)")
    print(f"sha256  {digest}")
    return target


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(HERE / "dist"), help="output directory")
    args = ap.parse_args()
    build(Path(args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
