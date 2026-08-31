"""The Alembic baseline (migrations/versions/0001_baseline.py) must describe exactly the
schema `Store.__init__` actually builds (`_SCHEMA` + `_MIGRATIONS` + the `jobs` SQLModel
table) — see that migration's own docstring for why this isn't optional. This is not a
one-time hand check: it runs every test invocation, so any future edit to `_SCHEMA`,
`_MIGRATIONS`, or an ORM table that isn't mirrored in a new migration fails loudly here
instead of silently drifting until someone points Alembic at a real database.

Comparison is by SQLite type *affinity* (TEXT/INTEGER/REAL/NUMERIC), not exact declared
type strings — `sa.REAL` vs a hand-written `REAL` column may render identically, but
this is the right level of equivalence regardless: SQLite itself only enforces affinity,
so a column-name-for-column-name, affinity-for-affinity match is exactly the guarantee
that matters. `dflt_value` is compared only as "has a default or not" for the same
reason — cosmetic differences in how a literal default is quoted aren't schema drift.
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

from clusterbuck.store import Store

SERVER_DIR = Path(__file__).resolve().parents[1]


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


def _schema_signature(db_path: Path) -> dict[str, set[tuple]]:
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


def test_alembic_baseline_matches_store_schema(tmp_path) -> None:
    store_db = tmp_path / "store.db"
    Store(str(store_db))  # runs _SCHEMA + _MIGRATIONS + the jobs SQLModel create_all

    alembic_db = tmp_path / "alembic.db"
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=SERVER_DIR,
        env={"CBK_DB_PATH": str(alembic_db), "PATH": __import__("os").environ["PATH"]},
        check=True,
        capture_output=True,
        text=True,
    )

    store_schema = _schema_signature(store_db)
    alembic_schema = _schema_signature(alembic_db)

    assert store_schema.keys() == alembic_schema.keys(), (
        f"table sets differ: only-in-store={store_schema.keys() - alembic_schema.keys()} "
        f"only-in-alembic={alembic_schema.keys() - store_schema.keys()}"
    )
    for table in store_schema:
        assert store_schema[table] == alembic_schema[table], (
            f"schema for '{table}' differs:\n"
            f"  store-only:   {store_schema[table] - alembic_schema[table]}\n"
            f"  alembic-only: {alembic_schema[table] - store_schema[table]}"
        )
