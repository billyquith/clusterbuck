"""Programmatic Alembic entry points used by `Store`'s schema bootstrap.

`migrations/env.py` reads its target database from `CBK_DB_PATH` (so a plain
`alembic upgrade head` on the command line targets whatever `cbk-server` would open) —
so a caller here that wants a *different* path (a test's `tmp_path`, for instance) must
set that env var for the duration of the call. `Store` may be constructed more than once
per process (once per test), so the previous value is always restored afterwards rather
than leaked.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from alembic import command
from alembic.config import Config

_SERVER_DIR = Path(__file__).resolve().parents[1]


@contextmanager
def _db_path_env(db_path: str) -> Iterator[Config]:
    previous = os.environ.get("CBK_DB_PATH")
    os.environ["CBK_DB_PATH"] = db_path
    try:
        cfg = Config(str(_SERVER_DIR / "alembic.ini"))
        cfg.set_main_option("script_location", str(_SERVER_DIR / "migrations"))
        yield cfg
    finally:
        if previous is None:
            os.environ.pop("CBK_DB_PATH", None)
        else:
            os.environ["CBK_DB_PATH"] = previous


def upgrade_to_head(db_path: str) -> None:
    with _db_path_env(db_path) as cfg:
        command.upgrade(cfg, "head")


def stamp_head(db_path: str) -> None:
    """Mark a database as already at the baseline, without running its DDL.

    For a pre-Alembic database whose tables were just brought up to date the old way
    (`_SCHEMA` + `_MIGRATIONS` in store.py) — recording that it matches the baseline,
    not creating anything.
    """
    with _db_path_env(db_path) as cfg:
        command.stamp(cfg, "head")
