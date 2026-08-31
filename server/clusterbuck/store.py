"""SQLite job registry — the durable system of record (Redis stays purely the broker).

Tracks the id → result_key mapping, the last-known lifecycle state, and the urgency
trajectory (urgency + escalation deadline) so the escalation engine can promote patient
work without re-reading the queue payloads.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Iterator

from sqlmodel import Session, SQLModel, select

from . import migrate
from .db import make_engine
from .orm.job import Job

# `jobs` moved to orm/job.py (SQLModel) — the first domain off raw sqlite3. See that
# module's docstring, and store.py's own docstring history in git, for why. Its table is
# created via `SQLModel.metadata.create_all` in `Store.__init__`, not this DDL string.
_SCHEMA = """
-- Model catalog (fleet-management.md → Model catalog): curated known-good artifacts with
-- the metadata the "fits" gate needs. `expected_ability` is an admin-curated hint used only
-- to RANK proposals — the real gate is measured ability after install (ADR 15).
CREATE TABLE IF NOT EXISTS catalog (
    artifact         TEXT PRIMARY KEY,
    family           TEXT,
    params_b         REAL,
    quant            TEXT,
    size_gb          REAL NOT NULL,
    min_ram_gb       REAL NOT NULL,
    source           TEXT NOT NULL,     -- model-manager that can install it (e.g. ollama)
    registry_ref     TEXT NOT NULL,     -- what to ask the model manager to pull
    expected_ability REAL,
    added_at         TEXT NOT NULL
);

-- What each node's model server actually reports having, with digests, so an artifact
-- changing upstream is detectable (ADR 15: a new digest is a NEW artifact).
CREATE TABLE IF NOT EXISTS node_models (
    node_id    TEXT NOT NULL,
    artifact   TEXT NOT NULL,
    digest     TEXT,
    first_seen TEXT NOT NULL,
    last_seen  TEXT NOT NULL,
    PRIMARY KEY (node_id, artifact)
);

-- Planner proposals (fleet-management.md): SUGGESTIONS, never silent changes. Multi-GB
-- weights are never fetched without a human decision (per-node auto_approve is opt-in).
CREATE TABLE IF NOT EXISTS proposals (
    id         TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,           -- upgrade | reeval | reclaim
    node_id    TEXT NOT NULL,
    artifact   TEXT NOT NULL,
    incumbent  TEXT,
    task_class TEXT,
    rationale  TEXT NOT NULL,
    status     TEXT NOT NULL,           -- pending | approved | denied | applied | failed
    created_at TEXT NOT NULL,
    decided_at TEXT
);

-- Ability matrix (ADR 15 / model-evaluation.md): ability(artifact, task_class) on an
-- anchored 1-10 scale, versioned. artifact = model + quantisation. Stored data the router
-- reads; tier-1 programmatic eval writes it.
CREATE TABLE IF NOT EXISTS ability (
    artifact      TEXT NOT NULL,
    task_class    TEXT NOT NULL,
    score         REAL NOT NULL,
    scale_version TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    -- 'seed' = an anchored placeholder so routing works before anything is measured;
    -- 'measured' = earned from a real eval run. Seeds must never look measured, or the
    -- fleet's own models are exempted from evaluation forever.
    provenance    TEXT NOT NULL DEFAULT 'measured',
    PRIMARY KEY (artifact, task_class, scale_version)
);

-- Eval runs (M7): one row per dispatched tier-1 eval item. Evals are ordinary jobs on the
-- fleet (model-evaluation.md — "the harness is just another client"), so this table is what
-- correlates a job back to the item it was measuring.
CREATE TABLE IF NOT EXISTS eval_runs (
    job_id      TEXT PRIMARY KEY,
    artifact    TEXT NOT NULL,
    task_class  TEXT NOT NULL,
    item_index  INTEGER NOT NULL,
    result_key  TEXT NOT NULL,
    state       TEXT NOT NULL DEFAULT 'pending',   -- pending | scored | failed
    passed      INTEGER,                            -- 1/0 once scored
    created_at  TEXT NOT NULL
);

-- Client attention leases (ADR 18 / protocols §9): a client's active-user signal that
-- promotes its waitable backlog; expiry demotes the unstarted promotions gracefully.
CREATE TABLE IF NOT EXISTS attention_leases (
    client_key  TEXT PRIMARY KEY,
    scope       TEXT,             -- json array of task_class, or null (all)
    expires_at  REAL NOT NULL
);

-- Dynamic registry (ADR 9): nodes self-enroll and heartbeat; fleet.yaml is only the seed.
CREATE TABLE IF NOT EXISTS join_tokens (
    token       TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    used        INTEGER NOT NULL DEFAULT 0,
    used_by     TEXT
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id        TEXT PRIMARY KEY,
    node_key       TEXT NOT NULL,
    hostname       TEXT, os TEXT, arch TEXT,
    ram_gb         REAL, accelerator TEXT, vram_gb REAL, disk_free_gb REAL, bench_tps_small REAL,
    profile        TEXT,
    capabilities   TEXT,          -- json array
    disk_quota_gb  REAL,          -- owner's contract for model storage (profile-derived)
    auto_approve   INTEGER NOT NULL DEFAULT 0,  -- opt-in: install without asking a human
    auto_update    INTEGER NOT NULL DEFAULT 0,  -- opt-in: apply a signed worker update
    mode           TEXT,
    installed      TEXT, loaded TEXT, queues TEXT,  -- json arrays
    jobs_done      INTEGER DEFAULT 0,
    tps            REAL,
    last_heartbeat TEXT,
    agent_version  TEXT,          -- the worker's build-stamped release version
    agent_flavour  TEXT,          -- python (or dotnet for legacy nodes): which artifact it can execute
    protocol_version INTEGER,     -- queue-contract version it speaks
    fitness        TEXT,          -- ok | stale | quarantine (coordinator's last verdict)
    fitness_reason TEXT,
    enrolled_at    TEXT NOT NULL
);

-- Usage metering (fleet-management.md → Usage accounting): METADATA ONLY. No prompt or
-- completion text ever lands here; `outcome` is a status enum, not the error string
-- (errors can echo input). One row per job (PRIMARY KEY) ⇒ capture is idempotent.
CREATE TABLE IF NOT EXISTS usage (
    job_id       TEXT PRIMARY KEY,
    ts           TEXT NOT NULL,
    capability   TEXT,
    model        TEXT,
    node         TEXT,             -- worker id, or 'cloud:<provider>' (future)
    venue        TEXT NOT NULL,    -- local | cloud
    tokens_in    INTEGER NOT NULL DEFAULT 0,
    tokens_out   INTEGER NOT NULL DEFAULT 0,
    outcome      TEXT NOT NULL,    -- done | failed | expired  (enum, never error text)
    cost         REAL NOT NULL DEFAULT 0,  -- local: avoided (tokens×cloud rate); cloud: actual
    day          TEXT NOT NULL     -- YYYY-MM-DD, for rollups
);

CREATE TABLE IF NOT EXISTS reservations (
    id           TEXT PRIMARY KEY,
    status       TEXT NOT NULL,   -- confirmed | declined
    state        TEXT,            -- scheduled|warming|open|draining|closed|cancelled (null if declined)
    task_class   TEXT,
    min_ability  INTEGER,
    capability   TEXT,
    node         TEXT,
    artifact     TEXT,
    priority     TEXT,
    privacy      TEXT,
    load         TEXT,
    duration_min INTEGER,
    est_jobs     INTEGER,
    warm_by      REAL,
    starts       REAL,
    ends         REAL,
    created_at   TEXT NOT NULL
);
"""

# Columns added after a table's initial schema; applied to pre-existing dev DBs.
# {table: {column: ALTER statement}}
_MIGRATIONS = {
    "jobs": {
        "urgency": "ALTER TABLE jobs ADD COLUMN urgency TEXT NOT NULL DEFAULT 'waitable'",
        "escalate_at": "ALTER TABLE jobs ADD COLUMN escalate_at REAL",
        "escalated": "ALTER TABLE jobs ADD COLUMN escalated INTEGER NOT NULL DEFAULT 0",
        "reservation": "ALTER TABLE jobs ADD COLUMN reservation TEXT",
        "deadline_epoch": "ALTER TABLE jobs ADD COLUMN deadline_epoch REAL",
        "client_key": "ALTER TABLE jobs ADD COLUMN client_key TEXT",
        "task_class": "ALTER TABLE jobs ADD COLUMN task_class TEXT",
        "promoted_by": "ALTER TABLE jobs ADD COLUMN promoted_by TEXT",
        "attempts": "ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
    },
    "ability": {
        "provenance": "ALTER TABLE ability ADD COLUMN provenance TEXT NOT NULL DEFAULT 'measured'",
    },
    "nodes": {
        "disk_quota_gb": "ALTER TABLE nodes ADD COLUMN disk_quota_gb REAL",
        "auto_approve": "ALTER TABLE nodes ADD COLUMN auto_approve INTEGER NOT NULL DEFAULT 0",
        "agent_version": "ALTER TABLE nodes ADD COLUMN agent_version TEXT",
        "agent_flavour": "ALTER TABLE nodes ADD COLUMN agent_flavour TEXT",
        "protocol_version": "ALTER TABLE nodes ADD COLUMN protocol_version INTEGER",
        "fitness": "ALTER TABLE nodes ADD COLUMN fitness TEXT",
        "fitness_reason": "ALTER TABLE nodes ADD COLUMN fitness_reason TEXT",
        "auto_update": "ALTER TABLE nodes ADD COLUMN auto_update INTEGER NOT NULL DEFAULT 0",
    },
}


class Store:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._engine = make_engine(db_path)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Bring the database up to the current schema, then make sure Alembic knows it.

        Three cases:
        - Already has `alembic_version` (a database this method has stamped or migrated
          before): just `alembic upgrade head` — the normal case on every later boot.
        - No tables at all (a brand-new database, e.g. every test's `tmp_path`): also
          `alembic upgrade head` — migrations/versions/0001_baseline.py creates the full
          schema, so this is the only path exercised by new databases from here on.
        - Has tables but no `alembic_version` (a database from before Alembic existed —
          e.g. an already-deployed coordinator): bring it up to date the old way first
          (idempotent — CREATE TABLE IF NOT EXISTS + only-if-missing ALTER TABLE), then
          `alembic stamp head` to record it's at the baseline WITHOUT running that
          migration's DDL against tables that already exist.
        """
        with self._conn() as c:
            has_alembic_version = c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='alembic_version'"
            ).fetchone() is not None
            has_any_table = c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchone() is not None

        if has_alembic_version or not has_any_table:
            migrate.upgrade_to_head(self._db_path)
            return

        with self._conn() as c:
            c.executescript(_SCHEMA)
        SQLModel.metadata.create_all(self._engine, tables=[Job.__table__])
        with self._conn() as c:
            for table, columns in _MIGRATIONS.items():
                existing = {row["name"] for row in c.execute(f"PRAGMA table_info({table})")}
                for col, ddl in columns.items():
                    if col not in existing:
                        c.execute(ddl)
        migrate.stamp_head(self._db_path)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        # WAL + a busy timeout: the coordinator's background loop writes concurrently with
        # request handlers, so a plain rollback-journal DB can raise "database is locked".
        # WAL lets readers proceed during a write; busy_timeout makes writers wait instead
        # of erroring.
        conn = sqlite3.connect(self._db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _session(self) -> Session:
        # expire_on_commit=False: several methods below return rows after committing a
        # change to them (e.g. attention_promote), and callers read attributes off those
        # rows after this session has already closed. Expired attributes on a detached
        # instance raise DetachedInstanceError instead of lazily refreshing.
        return Session(self._engine, expire_on_commit=False)

    def insert(
        self,
        *,
        id: str,
        result_key: str,
        capability: str,
        created_at: str,
        urgency: str = "waitable",
        escalate_at: float | None = None,
        reservation: str | None = None,
        deadline_epoch: float | None = None,
        client_key: str | None = None,
        task_class: str | None = None,
    ) -> None:
        with self._session() as s:
            s.add(Job(
                id=id, result_key=result_key, capability=capability, status="queued",
                created_at=created_at, urgency=urgency, escalate_at=escalate_at,
                reservation=reservation, deadline_epoch=deadline_epoch,
                client_key=client_key, task_class=task_class,
            ))
            s.commit()

    def get(self, id: str) -> Job | None:
        with self._session() as s:
            return s.get(Job, id)

    def set_status(self, id: str, status: str) -> None:
        with self._session() as s:
            job = s.get(Job, id)
            if job is not None:
                job.status = status
                s.add(job)
                s.commit()

    def set_attempts(self, id: str, attempts: int) -> None:
        """Record a delivery attempt (the reaper's requeue count) so it is observable."""
        with self._session() as s:
            job = s.get(Job, id)
            if job is not None:
                job.attempts = attempts
                s.add(job)
                s.commit()

    def due_for_escalation(self, now: float) -> list[Job]:
        """waitable jobs whose patience bound has expired and haven't escalated yet.

        Doneness is NOT judged here (SQLite status is updated lazily on poll); the caller
        must confirm against the Redis result blob before treating a row as unserved.
        """
        with self._session() as s:
            stmt = select(Job).where(
                Job.urgency == "waitable",
                Job.escalated == 0,
                Job.escalate_at.is_not(None),
                Job.escalate_at <= now,
            )
            return list(s.exec(stmt))

    def mark_escalated(self, id: str) -> None:
        """Promote waitable → necessary on age (records the trajectory + provenance)."""
        with self._session() as s:
            job = s.get(Job, id)
            if job is not None:
                job.urgency = "necessary"
                job.escalated = 1
                job.promoted_by = "age"
                s.add(job)
                s.commit()

    # --- client attention (M4a) ---

    def attention_promote(self, client_key: str, scope: list[str] | None) -> list[Job]:
        """Promote a client's waitable backlog to necessary. Returns the affected rows."""
        with self._session() as s:
            stmt = select(Job).where(
                Job.client_key == client_key,
                Job.urgency == "waitable",
                Job.escalated == 0,
            )
            if scope:
                stmt = stmt.where(Job.task_class.in_(scope))
            rows = list(s.exec(stmt))
            for job in rows:
                job.urgency = "necessary"
                job.escalated = 1
                job.promoted_by = "attention"
                s.add(job)
            s.commit()
            return rows

    def attention_promoted_jobs(self, client_key: str) -> list[Job]:
        with self._session() as s:
            stmt = select(Job).where(
                Job.client_key == client_key, Job.promoted_by == "attention"
            )
            return list(s.exec(stmt))

    def demote_job(self, id: str) -> None:
        """Return an attention-promoted job to waitable (lease lapsed, still unstarted)."""
        with self._session() as s:
            job = s.get(Job, id)
            if job is not None:
                job.urgency = "waitable"
                job.escalated = 0
                job.promoted_by = None
                s.add(job)
                s.commit()

    def upsert_attention_lease(self, client_key: str, scope: str | None, expires_at: float) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO attention_leases (client_key, scope, expires_at) VALUES (?,?,?) "
                "ON CONFLICT(client_key) DO UPDATE SET scope = excluded.scope, "
                "expires_at = excluded.expires_at",
                (client_key, scope, expires_at),
            )

    def delete_attention_lease(self, client_key: str) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM attention_leases WHERE client_key = ?", (client_key,))

    def expired_attention_leases(self, now: float) -> list[sqlite3.Row]:
        with self._conn() as c:
            return c.execute(
                "SELECT client_key FROM attention_leases WHERE expires_at <= ?", (now,)
            ).fetchall()

    # --- ability matrix (M5) ---

    def set_ability(self, *, artifact: str, task_class: str, score: float,
                    scale_version: str, updated_at: str,
                    provenance: str = "measured") -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO ability "
                "(artifact, task_class, score, scale_version, updated_at, provenance) "
                "VALUES (?,?,?,?,?,?) ON CONFLICT(artifact, task_class, scale_version) "
                "DO UPDATE SET score = excluded.score, updated_at = excluded.updated_at, "
                "provenance = excluded.provenance",
                (artifact, task_class, score, scale_version, updated_at, provenance),
            )

    def ability_provenance(self, artifact: str, task_class: str,
                           scale_version: str) -> str | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT provenance FROM ability WHERE artifact = ? AND task_class = ? "
                "AND scale_version = ?", (artifact, task_class, scale_version),
            ).fetchone()
            return row["provenance"] if row else None

    def clear_ability(self, artifact: str, scale_version: str) -> int:
        """Drop an artifact's scores so it must be re-measured (ADR 15: a changed digest is
        a NEW artifact and inherits nothing). Returns rows removed."""
        with self._conn() as c:
            cur = c.execute(
                "DELETE FROM ability WHERE artifact = ? AND scale_version = ?",
                (artifact, scale_version),
            )
            return cur.rowcount

    def failed_eval_runs(self, artifact: str, task_class: str) -> int:
        with self._conn() as c:
            return c.execute(
                "SELECT COUNT(*) AS n FROM eval_runs "
                "WHERE artifact = ? AND task_class = ? AND state = 'failed'",
                (artifact, task_class),
            ).fetchone()["n"]

    def get_ability(self, artifact: str, task_class: str, scale_version: str) -> float | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT score FROM ability WHERE artifact = ? AND task_class = ? "
                "AND scale_version = ?",
                (artifact, task_class, scale_version),
            ).fetchone()
            return float(row["score"]) if row else None

    def ability_matrix(self, scale_version: str) -> list[sqlite3.Row]:
        with self._conn() as c:
            return c.execute(
                "SELECT artifact, task_class, score, provenance FROM ability "
                "WHERE scale_version = ? "
                "ORDER BY artifact, task_class",
                (scale_version,),
            ).fetchall()

    # --- catalog / observed models / proposals (M6b) ---

    def upsert_catalog(self, *, artifact: str, family: str | None, params_b: float | None,
                       quant: str | None, size_gb: float, min_ram_gb: float, source: str,
                       registry_ref: str, expected_ability: float | None,
                       added_at: str) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO catalog (artifact, family, params_b, quant, size_gb, "
                "min_ram_gb, source, registry_ref, expected_ability, added_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(artifact) DO UPDATE SET "
                "family=excluded.family, params_b=excluded.params_b, quant=excluded.quant, "
                "size_gb=excluded.size_gb, min_ram_gb=excluded.min_ram_gb, "
                "source=excluded.source, registry_ref=excluded.registry_ref, "
                "expected_ability=excluded.expected_ability",
                (artifact, family, params_b, quant, size_gb, min_ram_gb, source,
                 registry_ref, expected_ability, added_at),
            )

    def list_catalog(self) -> list[sqlite3.Row]:
        with self._conn() as c:
            return c.execute("SELECT * FROM catalog ORDER BY min_ram_gb, artifact").fetchall()

    def catalog_count(self) -> int:
        with self._conn() as c:
            return c.execute("SELECT COUNT(*) AS n FROM catalog").fetchone()["n"]

    def observe_node_models(self, node_id: str, artifacts: dict[str, str | None],
                            now: str) -> list[tuple[str, str | None, str | None]]:
        """Record what a node reports having. Returns (artifact, old_digest, new_digest)
        for artifacts whose digest CHANGED — i.e. the artifact was updated upstream."""
        changed: list[tuple[str, str | None, str | None]] = []
        with self._conn() as c:
            for artifact, digest in artifacts.items():
                row = c.execute(
                    "SELECT digest FROM node_models WHERE node_id = ? AND artifact = ?",
                    (node_id, artifact),
                ).fetchone()
                if row is None:
                    c.execute(
                        "INSERT INTO node_models (node_id, artifact, digest, first_seen, "
                        "last_seen) VALUES (?,?,?,?,?)",
                        (node_id, artifact, digest, now, now),
                    )
                else:
                    old = row["digest"]
                    if digest is not None and old is not None and digest != old:
                        changed.append((artifact, old, digest))
                    c.execute(
                        "UPDATE node_models SET digest = COALESCE(?, digest), last_seen = ? "
                        "WHERE node_id = ? AND artifact = ?",
                        (digest, now, node_id, artifact),
                    )
        return changed

    def node_models(self, node_id: str) -> list[sqlite3.Row]:
        with self._conn() as c:
            return c.execute(
                "SELECT * FROM node_models WHERE node_id = ? ORDER BY artifact", (node_id,)
            ).fetchall()

    def insert_proposal(self, *, id: str, kind: str, node_id: str, artifact: str,
                        incumbent: str | None, task_class: str | None, rationale: str,
                        status: str, created_at: str) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO proposals (id, kind, node_id, artifact, incumbent, task_class, "
                "rationale, status, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (id, kind, node_id, artifact, incumbent, task_class, rationale, status,
                 created_at),
            )

    def open_proposal_exists(self, *, kind: str, node_id: str, artifact: str) -> bool:
        """True if an undecided/approved proposal already covers this — keeps the scan
        idempotent so a repeating tick can't spam duplicates."""
        with self._conn() as c:
            row = c.execute(
                "SELECT 1 FROM proposals WHERE kind = ? AND node_id = ? AND artifact = ? "
                "AND status IN ('pending', 'approved') LIMIT 1",
                (kind, node_id, artifact),
            ).fetchone()
            return row is not None

    def list_proposals(self, status: str | None = None) -> list[sqlite3.Row]:
        with self._conn() as c:
            if status:
                return c.execute(
                    "SELECT * FROM proposals WHERE status = ? ORDER BY created_at DESC",
                    (status,),
                ).fetchall()
            return c.execute("SELECT * FROM proposals ORDER BY created_at DESC").fetchall()

    def get_proposal(self, id: str) -> sqlite3.Row | None:
        with self._conn() as c:
            return c.execute("SELECT * FROM proposals WHERE id = ?", (id,)).fetchone()

    def decide_proposal(self, id: str, status: str, decided_at: str) -> bool:
        """Approve/deny a pending proposal. False if it wasn't pending (already decided)."""
        with self._conn() as c:
            cur = c.execute(
                "UPDATE proposals SET status = ?, decided_at = ? "
                "WHERE id = ? AND status = 'pending'",
                (status, decided_at, id),
            )
            return cur.rowcount > 0

    def set_proposal_status(self, id: str, status: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE proposals SET status = ? WHERE id = ?", (status, id))

    def approved_proposals_for_node(self, node_id: str, kind: str) -> list[sqlite3.Row]:
        with self._conn() as c:
            return c.execute(
                "SELECT * FROM proposals WHERE node_id = ? AND kind = ? AND status = 'approved' "
                "ORDER BY created_at",
                (node_id, kind),
            ).fetchall()

    def set_node_flags(self, node_id: str, *, disk_quota_gb: float | None = None,
                       auto_approve: bool | None = None,
                       auto_update: bool | None = None) -> None:
        with self._conn() as c:
            if disk_quota_gb is not None:
                c.execute("UPDATE nodes SET disk_quota_gb = ? WHERE node_id = ?",
                          (disk_quota_gb, node_id))
            if auto_approve is not None:
                c.execute("UPDATE nodes SET auto_approve = ? WHERE node_id = ?",
                          (1 if auto_approve else 0, node_id))
            if auto_update is not None:
                c.execute("UPDATE nodes SET auto_update = ? WHERE node_id = ?",
                          (1 if auto_update else 0, node_id))

    def models_used_since(self, day: str) -> set[str]:
        """Models that served at least one job on/after `day` (for reclaim proposals)."""
        with self._conn() as c:
            rows = c.execute(
                "SELECT DISTINCT model FROM usage WHERE model IS NOT NULL AND day >= ?",
                (day,),
            ).fetchall()
            return {r["model"] for r in rows}

    # --- eval runs (M7) ---

    def add_eval_run(self, *, job_id: str, artifact: str, task_class: str,
                     item_index: int, result_key: str, created_at: str) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT OR IGNORE INTO eval_runs "
                "(job_id, artifact, task_class, item_index, result_key, created_at) "
                "VALUES (?,?,?,?,?,?)",
                (job_id, artifact, task_class, item_index, result_key, created_at),
            )

    def pending_eval_runs(self) -> list[sqlite3.Row]:
        with self._conn() as c:
            return c.execute(
                "SELECT * FROM eval_runs WHERE state = 'pending' ORDER BY created_at"
            ).fetchall()

    def score_eval_run(self, job_id: str, passed: bool) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE eval_runs SET state = 'scored', passed = ? WHERE job_id = ?",
                (1 if passed else 0, job_id),
            )

    def fail_eval_run(self, job_id: str) -> None:
        """The job itself failed/expired — the item yields no signal, so don't score it."""
        with self._conn() as c:
            c.execute("UPDATE eval_runs SET state = 'failed' WHERE job_id = ?", (job_id,))

    def eval_progress(self, artifact: str, task_class: str) -> tuple[int, int, int, int]:
        """(pending, scored, passed, failed) for one (artifact, task_class) batch.

        `failed` is reported separately and deliberately: a run that produced no signal is
        neither pending nor scored, so omitting it made a partly-broken batch look finished
        and recorded an ability from only the surviving items.
        """
        with self._conn() as c:
            r = c.execute(
                "SELECT "
                "SUM(state = 'pending') AS pending, "
                "SUM(state = 'scored') AS scored, "
                "SUM(passed = 1) AS passed, "
                "SUM(state = 'failed') AS failed "
                "FROM eval_runs WHERE artifact = ? AND task_class = ?",
                (artifact, task_class),
            ).fetchone()
            return (r["pending"] or 0, r["scored"] or 0, r["passed"] or 0, r["failed"] or 0)

    def artifacts_under_eval(self) -> set[str]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT DISTINCT artifact FROM eval_runs WHERE state = 'pending'"
            ).fetchall()
            return {r["artifact"] for r in rows}

    def eval_runs_summary(self, limit: int = 50) -> list[sqlite3.Row]:
        with self._conn() as c:
            return c.execute(
                "SELECT artifact, task_class, "
                "SUM(state = 'pending') AS pending, SUM(state = 'scored') AS scored, "
                "SUM(state = 'failed') AS failed, SUM(passed = 1) AS passed "
                "FROM eval_runs GROUP BY artifact, task_class "
                "ORDER BY artifact, task_class LIMIT ?",
                (limit,),
            ).fetchall()

    def installed_artifacts(self) -> list[tuple[str, str, list[str]]]:
        """(node_id, artifact, node_capabilities) for every artifact observed on a node."""
        out: list[tuple[str, str, list[str]]] = []
        for n in self.list_nodes():
            caps = json.loads(n["capabilities"] or "[]")
            for artifact in json.loads(n["installed"] or "[]"):
                out.append((n["node_id"], artifact, caps))
        return out

    def ability_count(self, scale_version: str) -> int:
        with self._conn() as c:
            return c.execute(
                "SELECT COUNT(*) AS n FROM ability WHERE scale_version = ?", (scale_version,)
            ).fetchone()["n"]

    # --- reservations (M2b) ---

    def insert_reservation(self, *, id: str, status: str, state: str | None,
                           task_class: str, min_ability: int, capability: str | None,
                           node: str | None, artifact: str | None, priority: str,
                           privacy: str, load: str, duration_min: int,
                           est_jobs: int | None, warm_by: float | None,
                           starts: float | None, ends: float | None,
                           created_at: str) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO reservations (id, status, state, task_class, min_ability, "
                "capability, node, artifact, priority, privacy, load, duration_min, "
                "est_jobs, warm_by, starts, ends, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (id, status, state, task_class, min_ability, capability, node, artifact,
                 priority, privacy, load, duration_min, est_jobs, warm_by, starts, ends,
                 created_at),
            )

    def get_reservation(self, id: str) -> sqlite3.Row | None:
        with self._conn() as c:
            return c.execute("SELECT * FROM reservations WHERE id = ?", (id,)).fetchone()

    def list_reservations(self, limit: int = 50) -> list[sqlite3.Row]:
        with self._conn() as c:
            return c.execute(
                "SELECT * FROM reservations ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()

    def active_reservations(self) -> list[sqlite3.Row]:
        """Confirmed reservations still in a live lifecycle state (for the reconciler)."""
        with self._conn() as c:
            return c.execute(
                "SELECT * FROM reservations WHERE status = 'confirmed' "
                "AND state IN ('scheduled', 'warming', 'open', 'draining')"
            ).fetchall()

    def set_reservation_state(self, id: str, state: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE reservations SET state = ? WHERE id = ?", (state, id))

    def cancel_reservation(self, id: str) -> bool:
        """Flip to cancelled unless already terminal. Returns True if it changed."""
        with self._conn() as c:
            cur = c.execute(
                "UPDATE reservations SET state = 'cancelled' "
                "WHERE id = ? AND state NOT IN ('closed', 'cancelled')",
                (id,),
            )
            return cur.rowcount > 0

    # --- usage metering (M3a) ---

    def jobs_awaiting_usage(self) -> list[sqlite3.Row]:
        """Jobs with no usage record yet. Capture is gated on this (not job status), so a
        client's GET flipping status to done can't make the scan miss a job. Rows drop out
        once a usage row exists, keeping the scan bounded to uncaptured work."""
        with self._conn() as c:
            return c.execute(
                "SELECT j.id, j.result_key, j.capability, j.deadline_epoch "
                "FROM jobs j LEFT JOIN usage u ON u.job_id = j.id "
                "WHERE u.job_id IS NULL"
            ).fetchall()

    def record_usage(self, *, job_id: str, ts: str, capability: str | None,
                     model: str | None, node: str | None, venue: str,
                     tokens_in: int, tokens_out: int, outcome: str, cost: float,
                     day: str) -> None:
        """Write one usage record. INSERT OR IGNORE ⇒ idempotent under tick/GET races."""
        with self._conn() as c:
            c.execute(
                "INSERT OR IGNORE INTO usage (job_id, ts, capability, model, node, venue, "
                "tokens_in, tokens_out, outcome, cost, day) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (job_id, ts, capability, model, node, venue, tokens_in, tokens_out,
                 outcome, cost, day),
            )

    def usage_headline(self) -> sqlite3.Row:
        """Totals + local/cloud cost split for the avoided-cloud-spend headline."""
        with self._conn() as c:
            return c.execute(
                "SELECT COUNT(*) AS jobs, "
                "COALESCE(SUM(tokens_in), 0) AS tokens_in, "
                "COALESCE(SUM(tokens_out), 0) AS tokens_out, "
                "COALESCE(SUM(CASE WHEN venue='local' THEN cost ELSE 0 END), 0) AS local_cost, "
                "COALESCE(SUM(CASE WHEN venue='cloud' THEN cost ELSE 0 END), 0) AS cloud_cost "
                "FROM usage"
            ).fetchone()

    def usage_rollup(self, group: str) -> list[sqlite3.Row]:
        """Aggregate usage by 'model', 'node', 'capability', or 'day'."""
        if group not in {"model", "node", "capability", "day"}:
            raise ValueError(f"invalid rollup group: {group!r}")
        with self._conn() as c:
            return c.execute(
                f"SELECT {group} AS key, COUNT(*) AS jobs, "
                "COALESCE(SUM(tokens_in), 0) AS tokens_in, "
                "COALESCE(SUM(tokens_out), 0) AS tokens_out, "
                "COALESCE(SUM(cost), 0) AS cost "
                f"FROM usage GROUP BY {group} ORDER BY cost DESC"
            ).fetchall()

    def cloud_spend_in_month(self, month_prefix: str) -> float:
        """Actual cloud spend for a 'YYYY-MM' prefix (budget burn)."""
        with self._conn() as c:
            row = c.execute(
                "SELECT COALESCE(SUM(cost), 0) AS spent FROM usage "
                "WHERE venue = 'cloud' AND day LIKE ?",
                (f"{month_prefix}%",),
            ).fetchone()
            return float(row["spent"])

    # --- dynamic registry (M4a) ---

    def mint_token(self, token: str, created_at: str) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO join_tokens (token, created_at) VALUES (?, ?)",
                (token, created_at),
            )

    def claim_token(self, token: str, used_by: str) -> bool:
        """Atomically burn a one-time join token. True only on the first claim."""
        with self._conn() as c:
            cur = c.execute(
                "UPDATE join_tokens SET used = 1, used_by = ? WHERE token = ? AND used = 0",
                (used_by, token),
            )
            return cur.rowcount > 0

    def enroll_node(self, *, node_id: str, node_key: str, req, capabilities: str,
                    enrolled_at: str) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO nodes (node_id, node_key, hostname, os, arch, ram_gb, "
                "accelerator, vram_gb, disk_free_gb, bench_tps_small, profile, "
                "capabilities, mode, enrolled_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (node_id, node_key, req.hostname, req.os, req.arch, req.hw.ram_gb,
                 req.hw.accelerator, req.hw.vram_gb, req.hw.disk_free_gb,
                 req.hw.bench_tps_small, req.profile, capabilities, "active", enrolled_at),
            )

    def get_node(self, node_id: str) -> sqlite3.Row | None:
        with self._conn() as c:
            return c.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,)).fetchone()

    def list_nodes(self) -> list[sqlite3.Row]:
        with self._conn() as c:
            return c.execute("SELECT * FROM nodes ORDER BY enrolled_at").fetchall()

    def record_heartbeat(self, *, node_id: str, mode: str, installed: str, loaded: str,
                         queues: str, jobs_done: int | None, tps: float | None,
                         last_heartbeat: str, agent_version: str | None = None,
                         agent_flavour: str | None = None,
                         protocol_version: int | None = None,
                         fitness: str | None = None,
                         fitness_reason: str | None = None) -> bool:
        with self._conn() as c:
            cur = c.execute(
                "UPDATE nodes SET mode = ?, installed = ?, loaded = ?, queues = ?, "
                "agent_version = COALESCE(?, agent_version), "
                "agent_flavour = COALESCE(?, agent_flavour), "
                "protocol_version = COALESCE(?, protocol_version), "
                "fitness = ?, fitness_reason = ?, "
                "jobs_done = COALESCE(?, jobs_done), tps = COALESCE(?, tps), "
                "last_heartbeat = ? WHERE node_id = ?",
                (mode, installed, loaded, queues, agent_version, agent_flavour,
                 protocol_version, fitness, fitness_reason, jobs_done, tps,
                 last_heartbeat, node_id),
            )
            return cur.rowcount > 0
