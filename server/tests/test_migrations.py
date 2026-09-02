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

import pytest
from alembic import command
from sqlmodel import SQLModel

from clusterbuck import migrate
from clusterbuck.db import make_engine
from clusterbuck.orm.ability import Ability
from clusterbuck.orm.attention_lease import AttentionLease
from clusterbuck.orm.catalog_entry import CatalogEntry
from clusterbuck.orm.eval_generation import EvalGeneration
from clusterbuck.orm.eval_run import EvalRun
from clusterbuck.orm.job import Job
from clusterbuck.orm.join_token import JoinToken
from clusterbuck.orm.node import Node
from clusterbuck.orm.node_model import NodeModel
from clusterbuck.orm.perf_run import PerfRun
from clusterbuck.orm.perf_sample import PerfSample
from clusterbuck.orm.proposal import Proposal
from clusterbuck.orm.reservation import Reservation
from clusterbuck.orm.usage import Usage
from clusterbuck.store import _MIGRATIONS, _SCHEMA, Store

# The current Alembic head. Pinned deliberately rather than derived from the migration
# scripts: deriving it from the machinery under test would let "nobody thought about the
# legacy path" pass silently. Bump this in the same commit as a new migration.
_HEAD = "0006"


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


def _index_signature(db_path) -> set[tuple[str, str, bool]]:
    """{(table, index_name, is_unique) ...} — indexes, which the column signature misses.

    Needed because `_schema_signature` compares columns only, so an index that exists on
    one bootstrap path and not the other would pass silently. That is exactly the failure
    mode for a UNIQUE index: `ALTER TABLE ADD COLUMN` cannot carry a constraint, and
    `create_all` skips a table that already exists, so the legacy path has to create it
    explicitly or the constraint quietly does not exist on an upgraded database.

    Auto-indexes SQLite creates for PRIMARY KEY / UNIQUE columns are excluded: they are
    named `sqlite_autoindex_*` and are an artefact of how the table was declared, not
    schema we author.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT tbl_name, name, sql FROM sqlite_master "
            "WHERE type = 'index' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return {
            (tbl, name, "UNIQUE" in (sql or "").upper()) for tbl, name, sql in rows
        }
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
    SQLModel.metadata.create_all(engine, tables=[
        Job.__table__, Reservation.__table__, Node.__table__,
        JoinToken.__table__, AttentionLease.__table__, CatalogEntry.__table__,
        NodeModel.__table__, Proposal.__table__, Ability.__table__,
        EvalRun.__table__, EvalGeneration.__table__, Usage.__table__,
        PerfRun.__table__, PerfSample.__table__,
    ])
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
    assert _index_signature(store_db) == _index_signature(alembic_db)
    assert ("jobs", "ix_jobs_idempotency_key", True) in _index_signature(store_db), \
        "the unique index must exist, not merely match a peer that also lacks it"
    assert _alembic_version(store_db) == _alembic_version(alembic_db) == (_HEAD,)


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
    assert _alembic_version(legacy_db) == (_HEAD,)

    # A second Store() against the now-stamped database takes the "already stamped"
    # branch (plain `alembic upgrade head`, a no-op since it's already at head).
    Store(str(legacy_db))
    assert _schema_signature(legacy_db) == before


def test_0004_adds_provenance_to_a_populated_jobs_table(tmp_path) -> None:
    """0004 is the first migration whose DDL runs against a live, populated `jobs` table
    (the deployed coordinator's database is past the legacy-stamp path), so upgrading
    must add the columns without disturbing the rows already there.

    Asserted explicitly rather than assumed from "ALTER TABLE ADD COLUMN is safe": the
    point of the check is that existing rows survive and read back as NULL provenance,
    which is the honest value for jobs submitted before provenance existed.
    """
    db = tmp_path / "populated.db"

    with migrate._db_path_env(str(db)) as cfg:
        command.upgrade(cfg, "0003")

    existing = [
        ("job_old_1", "res_old_1", "8b-extract", "done", "2026-08-01T10:00:00Z", "waitable"),
        ("job_old_2", "res_old_2", "8b-extract", "queued", "2026-08-02T11:00:00Z", "urgent"),
    ]
    conn = sqlite3.connect(str(db))
    try:
        conn.executemany(
            "INSERT INTO jobs (id, result_key, capability, status, created_at, urgency) "
            "VALUES (?, ?, ?, ?, ?, ?)", existing)
        conn.commit()
    finally:
        conn.close()

    migrate.upgrade_to_head(str(db))

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)")}
        assert {"submitter_app", "submitter_instance", "submitter_request_id",
                "submitted_at", "observed_ip"} <= columns

        rows = conn.execute(
            "SELECT id, result_key, capability, status, created_at, urgency, "
            "submitter_app, submitter_request_id, observed_ip FROM jobs ORDER BY id"
        ).fetchall()
        assert [tuple(r)[:6] for r in rows] == existing, "pre-existing rows must survive"
        for row in rows:
            assert row["submitter_app"] is None
            assert row["submitter_request_id"] is None
            assert row["observed_ip"] is None
    finally:
        conn.close()


def test_0005_adds_timing_to_a_populated_jobs_table(tmp_path) -> None:
    """Like 0004, 0005's DDL runs against a live, populated `jobs` table, so upgrading
    must add the timing/delivery columns without disturbing the rows already there.

    The NULL assertions are the point: a job that ran before this existed has no observed
    claim and no recorded delivery, and NULL is the honest value for both — a default
    would invent a timestamp for work nobody watched.
    """
    db = tmp_path / "populated.db"

    with migrate._db_path_env(str(db)) as cfg:
        command.upgrade(cfg, "0004")

    existing = [
        ("job_t1", "res_t1", "8b-extract", "done", "2026-08-01T10:00:00Z", "waitable"),
        ("job_t2", "res_t2", "8b-extract", "queued", "2026-08-02T11:00:00Z", "urgent"),
    ]
    conn = sqlite3.connect(str(db))
    try:
        conn.executemany(
            "INSERT INTO jobs (id, result_key, capability, status, created_at, urgency) "
            "VALUES (?, ?, ?, ?, ?, ?)", existing)
        conn.commit()
    finally:
        conn.close()

    migrate.upgrade_to_head(str(db))

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)")}
        assert {"started_at", "finished_at", "claimed_by", "entry_id",
                "stream"} <= columns

        rows = conn.execute(
            "SELECT id, result_key, capability, status, created_at, urgency, "
            "started_at, finished_at, claimed_by, entry_id, stream FROM jobs ORDER BY id"
        ).fetchall()
        assert [tuple(r)[:6] for r in rows] == existing, "pre-existing rows must survive"
        for row in rows:
            for column in ("started_at", "finished_at", "claimed_by", "entry_id", "stream"):
                assert row[column] is None
    finally:
        conn.close()


def test_a_preexisting_jobs_table_still_gets_the_unique_index(tmp_path) -> None:
    """The legacy path's sharp edge, and the reason `_INDEXES` exists.

    `_build_legacy_database` above starts from `create_all`, which creates the `jobs`
    table *including* its index — so it cannot see this failure. A real pre-Alembic
    deployment has a `jobs` table that already existed, and `create_all` skips an existing
    table wholesale, indexes included. `ALTER TABLE ADD COLUMN` cannot carry a constraint
    either. So without an explicit `CREATE UNIQUE INDEX`, such a database would be stamped
    at head with the idempotency constraint simply absent — and idempotent submit would
    silently degrade to "mint a new job every time", which is the bug it exists to prevent.
    """
    db = tmp_path / "old.db"
    conn = sqlite3.connect(str(db))
    try:
        # The shape the table had before any of this existed.
        conn.execute(
            "CREATE TABLE jobs ("
            " id TEXT PRIMARY KEY,"
            " result_key TEXT NOT NULL,"
            " capability TEXT NOT NULL,"
            " status TEXT NOT NULL,"
            " created_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO jobs (id, result_key, capability, status, created_at) "
            "VALUES ('job_ancient', 'res_ancient', '8b-extract', 'done', 't')"
        )
        conn.commit()
    finally:
        conn.close()

    Store(str(db))  # legacy branch: bring up to date the old way, then stamp

    assert _alembic_version(db) == (_HEAD,)
    assert ("jobs", "ix_jobs_idempotency_key", True) in _index_signature(db)

    conn = sqlite3.connect(str(db))
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}
        assert {"idempotency_key", "cancel_requested", "started_at",
                "entry_id"} <= columns, "the columns must be added, not just stamped"
        assert conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1

        # And the constraint must actually bite.
        conn.execute("UPDATE jobs SET idempotency_key = 'k' WHERE id = 'job_ancient'")
        conn.execute(
            "INSERT INTO jobs (id, result_key, capability, status, created_at, "
            "idempotency_key) VALUES ('job_new', 'res_new', 'c', 'queued', 't', 'other')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE jobs SET idempotency_key = 'k' WHERE id = 'job_new'")
    finally:
        conn.close()
