"""Entry point: `cbk <verb>`, `python -m cbk_worker`, or `python cbk.pyz`."""

from __future__ import annotations

import asyncio
import contextlib
import sys

from . import commands
from .cli import Out, build_parser


def main(argv: list[str] | None = None) -> int:
    # Line-buffer stdout/stderr. Python block-buffers (~8 KB) whenever output is not a tty,
    # which for a long-running worker under systemd, Docker or a redirect means operational
    # messages — "QUARANTINED", "is behind", a failed install — sit in a buffer for hours or
    # are lost entirely if the process is killed. A worker that looks silent while it is
    # actually shouting is worse than no logging.
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(AttributeError, ValueError):
            stream.reconfigure(line_buffering=True)

    parser = build_parser()
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    if not args.verb:
        parser.print_help()
        return 1

    try:
        if args.verb == "work":
            return asyncio.run(commands.run_work(args))
        if args.verb == "submit":
            return asyncio.run(commands.run_submit(args))
        if args.verb == "status":
            return asyncio.run(commands.run_status(args))
        if args.verb == "fleet":
            return asyncio.run(commands.run_fleet(args))
        if args.verb == "enroll":
            return asyncio.run(commands.run_enroll(args))
        if args.verb in ("pause", "resume"):
            return commands.run_mode(args, "paused" if args.verb == "pause" else "active")
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        Out.error(f"error: {e}")
        return 1

    Out.error(f"unknown command '{args.verb}'")
    return 1


if __name__ == "__main__":
    sys.exit(main())
