"""`Store._ensure_schema` has one path onto the schema: `alembic upgrade head`.

This file pins the invariant that survives that simplification and matters most — the
ORM and the migrations must agree. Every table lives as a SQLModel class under
`clusterbuck/orm/`, and every column must also exist in migrations/versions/. An ORM
edit with no accompanying migration fails loudly here, instead of surfacing when
someone points Alembic at a real database and finds a column the code expects missing.

It also exercises the two migrations whose DDL runs against a *populated* `jobs` table
(0004, 0005), because adding a column to live data is the operation with real risk.

Until 2026-09-21 there was a second bootstrap path — tables present but no
`alembic_version`, i.e. a coordinator deployed before Alembic — which built raw DDL and
then stamped. Every such database has long since been stamped (the live coordinator was
at `0009`), so that path and its tests are gone with `_SCHEMA` / `_MIGRATIONS`.

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
from clusterbuck.store import Store

# The current Alembic head. Pinned deliberately rather than derived from the migration
# scripts: deriving it from the machinery under test would let a migration that was
# never wired up pass silently. Bump this in the same commit as a new migration.
_HEAD = "0011"


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


def test_the_orm_and_the_migrations_agree(tmp_path) -> None:
    """The invariant this file exists for: every column the ORM declares must also be a
    column some migration creates, and vice versa.

    Previously this compared `Store()` against `alembic upgrade head` — meaningful while
    Store had a second, hand-written bootstrap path to disagree with. It no longer does,
    so that comparison would now be a tautology (both sides are the same call). The live
    risk that remains is an ORM edit landing without a migration, which this catches:
    build one database from `SQLModel.metadata` and one from the migrations, and demand
    they match.
    """
    orm_db = tmp_path / "orm.db"
    SQLModel.metadata.create_all(make_engine(str(orm_db)))

    alembic_db = tmp_path / "alembic.db"
    migrate.upgrade_to_head(str(alembic_db))

    orm_sig, alembic_sig = _schema_signature(orm_db), _schema_signature(alembic_db)
    # alembic_version is Alembic's own bookkeeping; the ORM has no opinion about it.
    alembic_sig.pop("alembic_version", None)

    assert orm_sig == alembic_sig, (
        "ORM and migrations disagree — add a migration for the ORM change, or an ORM "
        "field for the migration"
    )
    assert _index_signature(orm_db) == _index_signature(alembic_db)
    assert ("jobs", "ix_jobs_idempotency_key", True) in _index_signature(alembic_db), \
        "the unique index must exist, not merely match a peer that also lacks it"


def test_store_bootstraps_a_new_database_at_head(tmp_path) -> None:
    """A brand-new database gets the full schema and is recorded at head."""
    db = tmp_path / "store.db"
    Store(str(db))
    assert _alembic_version(db) == (_HEAD,)
    assert "jobs" in _schema_signature(db)


def test_attention_leases_is_gone(tmp_path) -> None:
    """0011 drops it. Guards against the table creeping back via an ORM class."""
    db = tmp_path / "head.db"
    migrate.upgrade_to_head(str(db))
    assert "attention_leases" not in _schema_signature(db)


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
