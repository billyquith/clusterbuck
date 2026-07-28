"""Terminal output and argument parsing.

The .NET worker hand-rolls both to stay Native-AOT-clean (worker/dotnet Cli.cs). This side
has no such constraint, so argparse does the work — but the verb and flag surface is kept
identical, because an operator should not have to know which implementation is installed on
a node to drive it.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

from .config import AGENT_VERSION

# Colour is opt-out (NO_COLOR) and only used on a real terminal, so logs and pipes stay clean.
_COLOUR = sys.stdout.isatty() and not os.environ.get("NO_COLOR")


def _paint(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _COLOUR else text


class Out:
    @staticmethod
    def line(text: str = "") -> None:
        print(text)

    @staticmethod
    def dim(text: str) -> None:
        print(_paint("2", text))

    @staticmethod
    def info(text: str) -> None:
        print(_paint("36", text))

    @staticmethod
    def good(text: str) -> None:
        print(_paint("32", text))

    @staticmethod
    def warn(text: str) -> None:
        print(_paint("33", text))

    @staticmethod
    def error(text: str) -> None:
        print(_paint("31", text), file=sys.stderr)

    @staticmethod
    def table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
        """Column-aligned plain text — no box drawing, so it stays greppable."""
        cols = len(headers)
        widths = [len(h) for h in headers]
        for row in rows:
            for i in range(cols):
                widths[i] = max(widths[i], len(str(row[i]) if i < len(row) else ""))
        Out.dim("  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
        for row in rows:
            cells = [str(row[i]) if i < len(row) else "" for i in range(cols)]
            print("  ".join(c.ljust(widths[i]) for i, c in enumerate(cells)))


def build_parser() -> argparse.ArgumentParser:
    # --server is accepted BEFORE or AFTER the verb, because the .NET worker's hand-rolled
    # parser scans the whole argv and operators (and the e2e scripts) write it either way.
    # SUPPRESS is what makes that work: without it, a subparser that did not see --server
    # would reset the attribute to None and clobber a value given before the verb.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--server", default=argparse.SUPPRESS,
                        help="Coordinator URL (or CBK_SERVER_URL)")

    p = argparse.ArgumentParser(
        prog="cbk", description="cbk — clusterbuck worker agent (Python)",
        epilog="Configuration is by environment (CBK_*); see docs/deployment.md.")
    p.add_argument("--version", action="version", version=AGENT_VERSION)
    p.add_argument("--server", default=None, help="Coordinator URL (or CBK_SERVER_URL)")

    sub = p.add_subparsers(dest="verb", metavar="<command>")

    work = sub.add_parser("work", parents=[common],
                          help="Pull jobs for this node's capabilities and run them")
    work.add_argument("-c", "--capabilities", help="override served tiers (comma-separated)")
    work.add_argument("--model", help="override the model")
    work.add_argument("--state", help="path to the persisted node identity")

    submit = sub.add_parser("submit", parents=[common],
                            help="Submit an async job to the coordinator")
    submit.add_argument("-p", "--prompt", required=True)
    submit.add_argument("--capability", help="address a tier directly")
    submit.add_argument("--task-class", help="need-shaped addressing (with --min-ability)")
    submit.add_argument("--min-ability", type=int)
    submit.add_argument("--urgency", default="waitable",
                        choices=["urgent", "necessary", "waitable"])
    submit.add_argument("--privacy", default="local_only",
                        choices=["local_only", "cloud_ok"])

    status = sub.add_parser("status", parents=[common],
                            help="Poll a job's status and result")
    status.add_argument("job_id")

    sub.add_parser("fleet", parents=[common],
                   help="List the coordinator's capability/node registry")

    enroll = sub.add_parser("enroll", parents=[common],
                            help="Probe hardware and join the fleet")
    enroll.add_argument("-t", "--token", required=True, help="join token")
    enroll.add_argument("--profile", default="shared",
                        choices=["dedicated", "shared", "background"])
    enroll.add_argument("--state", help="path to the persisted node identity")

    for verb, helptext in (("pause", "Owner eviction: stop claiming jobs"),
                           ("resume", "Resume claiming jobs")):
        sp = sub.add_parser(verb, parents=[common], help=helptext)
        sp.add_argument("--state", help="path to the persisted node identity")

    return p
