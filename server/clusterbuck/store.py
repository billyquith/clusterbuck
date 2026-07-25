"""SQLite job registry — the durable system of record (Redis stays purely the broker).

Tracks the id → result_key mapping, the last-known lifecycle state, and the urgency
trajectory (urgency + escalation deadline) so the escalation engine can promote patient
work without re-reading the queue payloads.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT PRIMARY KEY,
    result_key   TEXT NOT NULL,
    capability   TEXT NOT NULL,
    status       TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    urgency      TEXT NOT NULL DEFAULT 'waitable',
    escalate_at  REAL,            -- epoch seconds; only set for waitable(N)
    escalated    INTEGER NOT NULL DEFAULT 0,
    reservation  TEXT,            -- opt-in reservation id this job queues against
    deadline_epoch REAL           -- epoch seconds; for expiry sweep
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
    mode           TEXT,
    installed      TEXT, loaded TEXT, queues TEXT,  -- json arrays
    jobs_done      INTEGER DEFAULT 0,
    tps            REAL,
    last_heartbeat TEXT,
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

# Columns added after the initial M0 schema; applied to pre-existing dev DBs.
_MIGRATIONS = {
    "urgency": "ALTER TABLE jobs ADD COLUMN urgency TEXT NOT NULL DEFAULT 'waitable'",
    "escalate_at": "ALTER TABLE jobs ADD COLUMN escalate_at REAL",
    "escalated": "ALTER TABLE jobs ADD COLUMN escalated INTEGER NOT NULL DEFAULT 0",
    "reservation": "ALTER TABLE jobs ADD COLUMN reservation TEXT",
    "deadline_epoch": "ALTER TABLE jobs ADD COLUMN deadline_epoch REAL",
}


class Store:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        with self._conn() as c:
            c.executescript(_SCHEMA)
            existing = {row["name"] for row in c.execute("PRAGMA table_info(jobs)")}
            for col, ddl in _MIGRATIONS.items():
                if col not in existing:
                    c.execute(ddl)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

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
    ) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO jobs "
                "(id, result_key, capability, status, created_at, urgency, escalate_at, "
                "reservation, deadline_epoch) "
                "VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?)",
                (id, result_key, capability, created_at, urgency, escalate_at,
                 reservation, deadline_epoch),
            )

    def get(self, id: str) -> sqlite3.Row | None:
        with self._conn() as c:
            cur = c.execute("SELECT * FROM jobs WHERE id = ?", (id,))
            return cur.fetchone()

    def set_status(self, id: str, status: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, id))

    def due_for_escalation(self, now: float) -> list[sqlite3.Row]:
        """waitable jobs whose patience bound has expired and haven't escalated yet.

        Doneness is NOT judged here (SQLite status is updated lazily on poll); the caller
        must confirm against the Redis result blob before treating a row as unserved.
        """
        with self._conn() as c:
            cur = c.execute(
                "SELECT id, result_key, capability, escalate_at FROM jobs "
                "WHERE urgency = 'waitable' AND escalated = 0 "
                "AND escalate_at IS NOT NULL AND escalate_at <= ?",
                (now,),
            )
            return cur.fetchall()

    def mark_escalated(self, id: str) -> None:
        """Promote waitable → necessary (records the trajectory change)."""
        with self._conn() as c:
            c.execute(
                "UPDATE jobs SET urgency = 'necessary', escalated = 1 WHERE id = ?",
                (id,),
            )

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
                         last_heartbeat: str) -> bool:
        with self._conn() as c:
            cur = c.execute(
                "UPDATE nodes SET mode = ?, installed = ?, loaded = ?, queues = ?, "
                "jobs_done = COALESCE(?, jobs_done), tps = COALESCE(?, tps), "
                "last_heartbeat = ? WHERE node_id = ?",
                (mode, installed, loaded, queues, jobs_done, tps, last_heartbeat, node_id),
            )
            return cur.rowcount > 0
