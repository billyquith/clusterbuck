"""Store._ensure_schema has two paths onto the same baseline (see its docstring): a
fresh database goes straight through `alembic upgrade head`, while a database that
already has tables but no `alembic_version` (a pre-Alembic deployment) gets brought up
to date the old way and then *stamped* — never has the baseline's DDL run against it.

This file checks both paths land on the same schema, and does so every test run rather
than by one-time hand inspection: any future edit to `_SCHEMA`, `_MIGRATIONS`, or an ORM
table that isn't mirrored in migrations/versions/0001_baseline.py fails loudly here
instead of surfacing only when someone points Alembic at a real database.

Comparison is by SQLite type *affinity* (TEXT/INTEGER/REAL/NUMERIC), not exact declared
type strings — `sa.REAL` vs a hand-written `REAL` column may render identically, but
this is the right level of equivalence regardless: SQLite itself only enforces affinity,
so a column-name-for-column-name, affinity-for-affinity match is exactly the guarantee
that matters. `dflt_value` is compared only as "has a default or not" for the same
reason — cosmetic differences in how a literal default is quoted aren't schema drift.
"""

from __future__ import annotations

import sqlite3

from sqlmodel import SQLModel

from clusterbuck import migrate
from clusterbuck.db import make_engine
from clusterbuck.orm.job import Job
from clusterbuck.store import _MIGRATIONS, _SCHEMA, Store


def _affinity(decl_type: str) -> str:
    """SQLite's own type-affinity rule (see sqlite.org/datatype3.html §3.1)."""
    t = decl_type.upper()
    if "INT" in t:
        return "INTEGER"
    if "CHAR" in t or "CLOB" in t or "TEXT" in t:
        return "TEXT"
    if "REAL" in t or "FLOA" in t or "DOUB" in t:
        return "REAL"
    return "NUMERIC"


def _schema_signature(db_path) -> dict[str, set[tuple]]:
    """{table: {(column, affinity, notnull, has_default, pk) ...}}"""
    conn = sqlite3.connect(str(db_path))
    try:
        tables = [
            row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
                "AND name != 'alembic_version'"
            )
        ]
        sig: dict[str, set[tuple]] = {}
        for table in tables:
            cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
            sig[table] = {
                (name, _affinity(coltype), bool(notnull), dflt is not None, bool(pk))
                for _cid, name, coltype, notnull, dflt, pk in cols
            }
        return sig
    finally:
        conn.close()


def _alembic_version(db_path) -> tuple[str] | None:
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute("SELECT version_num FROM alembic_version").fetchone()
    finally:
        conn.close()


def _build_legacy_database(db_path) -> None:
    """Reproduce exactly what Store._ensure_schema's legacy branch bootstraps, bypassing
    its own alembic-vs-legacy dispatch — i.e. what an already-deployed, pre-Alembic
    coordinator database looks like just before this code ever ran against it."""
    engine = make_engine(str(db_path))
    conn = sqlite3.connect(str(db_path), timeout=5.0)
    try:
        conn.executescript(_SCHEMA)
        conn.commit()
    finally:
        conn.close()
    SQLModel.metadata.create_all(engine, tables=[Job.__table__])
    conn = sqlite3.connect(str(db_path), timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        for table, columns in _MIGRATIONS.items():
            existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
            for col, ddl in columns.items():
                if col not in existing:
                    conn.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def test_fresh_database_matches_alembic_baseline(tmp_path) -> None:
    """A brand-new database, built the way Store._ensure_schema builds one (no
    pre-existing tables), must match a plain `alembic upgrade head` exactly."""
    store_db = tmp_path / "store.db"
    Store(str(store_db))

    alembic_db = tmp_path / "alembic.db"
    migrate.upgrade_to_head(str(alembic_db))

    assert _schema_signature(store_db) == _schema_signature(alembic_db)
    assert _alembic_version(store_db) == _alembic_version(alembic_db) == ("0001",)


def test_legacy_database_is_stamped_not_migrated(tmp_path) -> None:
    """A pre-Alembic database (tables already exist, no `alembic_version`) must be
    recognised, brought up to date the old way, and *stamped* rather than having the
    baseline's `op.create_table` calls run against tables that already exist (which
    would raise 'table already exists')."""
    legacy_db = tmp_path / "legacy.db"
    _build_legacy_database(legacy_db)
    before = _schema_signature(legacy_db)

    Store(str(legacy_db))  # must not raise, and must not alter the schema

    assert _schema_signature(legacy_db) == before
    assert _alembic_version(legacy_db) == ("0001",)

    # A second Store() against the now-stamped database takes the "already stamped"
    # branch (plain `alembic upgrade head`, a no-op since it's already at head).
    Store(str(legacy_db))
    assert _schema_signature(legacy_db) == before
