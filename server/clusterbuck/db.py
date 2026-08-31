"""Shared SQLAlchemy engine for tables migrated off raw `sqlite3` (`jobs` first).

One engine per `Store` instance, pointed at the same SQLite file `Store._conn()` still
uses for tables not yet migrated. Both apply the same WAL + busy_timeout pragmas
`Store._conn()` always has, so a migrated domain and an unmigrated one can keep sharing
the file safely — see the docstring on `Store._conn()` for why those pragmas exist.

Deliberately a synchronous engine, not `aiosqlite`: every `Store` call today is a
blocking call made inline from async route handlers and the coordinator loop (no
`asyncio.to_thread`), so a sync engine here changes nothing about that blocking
behaviour — it just replaces which library issues the same kind of blocking call.
Mixing a truly async engine in for one domain while the rest stay synchronous would be
a different, worse hazard: the async engine's commit callback needs the event loop, and
a concurrent synchronous call (still blocking that same loop) can starve it out from
under a pending write. That whole-codebase async conversion is a separate step.
"""

from __future__ import annotations

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlmodel import create_engine


def make_engine(db_path: str) -> Engine:
    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})

    @event.listens_for(engine, "connect")
    def _set_pragmas(dbapi_conn, _record) -> None:
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA busy_timeout=5000")
        cur.close()

    return engine
