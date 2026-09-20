"""`Store`: the SQLite-backed durable system of record (Redis stays purely the broker).

One class fronting every table the coordinator persists — jobs, reservations, the
dynamic fleet registry, the model catalog/proposals/ability/eval-runs cluster, and
usage metering. Each table is a SQLModel class under `orm/`; this module is the
repository layer (query/insert/update methods) plus schema bootstrap
(`Store._ensure_schema`, which hands off to Alembic — see `clusterbuck/migrate.py`).
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Iterator

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, SQLModel, select

from . import migrate
from .db import make_engine
from .models import TERMINAL_STATUSES, now_iso
from .orm.ability import Ability
from .orm.attention_lease import AttentionLease
from .orm.catalog_entry import CatalogEntry
from .orm.eval_generation import EvalGeneration
from .orm.eval_run import EvalRun
from .orm.job import Job
from .orm.join_token import JoinToken
from .orm.node import Node
from .orm.node_model import NodeModel
from .orm.perf_run import PerfRun
from .orm.perf_sample import PerfSample
from .orm.proposal import Proposal
from .orm.reservation import Reservation
from .orm.usage import Usage

# All tables have moved to orm/*.py (SQLModel) — see clusterbuck/orm/job.py's docstring
# for why. Nothing left to bootstrap here; kept as an empty string (rather than removed)
# so `Store._ensure_schema`'s legacy branch doesn't need a special case for "no DDL left".
_SCHEMA = ""

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
        # Caller provenance (protocols.md §1b). Nullable with no default: rows written
        # before this existed have no attribution, and NULL says so honestly.
        "submitter_app": "ALTER TABLE jobs ADD COLUMN submitter_app TEXT",
        "submitter_instance": "ALTER TABLE jobs ADD COLUMN submitter_instance TEXT",
        "submitter_request_id": "ALTER TABLE jobs ADD COLUMN submitter_request_id TEXT",
        "submitted_at": "ALTER TABLE jobs ADD COLUMN submitted_at TEXT",
        "observed_ip": "ALTER TABLE jobs ADD COLUMN observed_ip TEXT",
        # Lifecycle timing + delivery (protocols.md §1b). Nullable, no default: rows
        # written before this existed carry no observation, and NULL says so.
        "started_at": "ALTER TABLE jobs ADD COLUMN started_at TEXT",
        "finished_at": "ALTER TABLE jobs ADD COLUMN finished_at TEXT",
        "claimed_by": "ALTER TABLE jobs ADD COLUMN claimed_by TEXT",
        "entry_id": "ALTER TABLE jobs ADD COLUMN entry_id TEXT",
        "stream": "ALTER TABLE jobs ADD COLUMN stream TEXT",
        # Idempotency + cancellation (protocols.md §1b). The UNIQUE index that makes the
        # key binding is in _INDEXES below — `ADD COLUMN` cannot carry one.
        "idempotency_key": "ALTER TABLE jobs ADD COLUMN idempotency_key TEXT",
        "cancel_requested":
            "ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0",
    },
    "ability": {
        "provenance": "ALTER TABLE ability ADD COLUMN provenance TEXT NOT NULL DEFAULT 'measured'",
    },
    "eval_runs": {
        "generation": "ALTER TABLE eval_runs ADD COLUMN generation INTEGER NOT NULL DEFAULT 1",
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


# Indexes the legacy (pre-Alembic) bootstrap must create for itself. `create_all` skips a
# table that already exists — indexes included — and `ALTER TABLE ADD COLUMN` cannot carry
# a constraint, so without this an upgraded database would be stamped at head with the
# unique index simply absent. `IF NOT EXISTS` keeps it idempotent, matching _MIGRATIONS'
# only-if-missing character.
_INDEXES: dict[str, dict[str, str]] = {
    "jobs": {
        "ix_jobs_idempotency_key":
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_jobs_idempotency_key "
            "ON jobs(idempotency_key)",
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
        SQLModel.metadata.create_all(self._engine, tables=[
            Job.__table__, Reservation.__table__, Node.__table__,
            JoinToken.__table__, AttentionLease.__table__, CatalogEntry.__table__,
            NodeModel.__table__, Proposal.__table__, Ability.__table__,
            EvalRun.__table__, EvalGeneration.__table__, Usage.__table__,
            PerfRun.__table__, PerfSample.__table__,
        ])
        with self._conn() as c:
            for table, columns in _MIGRATIONS.items():
                existing = {row["name"] for row in c.execute(f"PRAGMA table_info({table})")}
                for col, ddl in columns.items():
                    if col not in existing:
                        c.execute(ddl)
            # After the columns exist, not before: an index cannot be created on a column
            # this loop is about to add.
            for indexes in _INDEXES.values():
                for ddl in indexes.values():
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
        submitter_app: str | None = None,
        submitter_instance: str | None = None,
        submitter_request_id: str | None = None,
        submitted_at: str | None = None,
        observed_ip: str | None = None,
        idempotency_key: str | None = None,
    ) -> Job | None:
        """Insert a queued job. Returns None normally — or, when `idempotency_key` is
        already taken, the job that took it, having written nothing.

        The insert IS the idempotency claim: the unique index on `idempotency_key`
        arbitrates, so two concurrent submits under one key cannot both win. The repo's
        other "idempotent" helpers (`record_usage`, `claim_token`) are check-then-write
        and safe only because a single tick writes them; a client-driven endpoint has as
        many concurrent writers as the retry storm it is there to absorb.

        The conflict lookup lives here so SQLAlchemy's exception type stays out of the
        API layer.
        """
        row = Job(
            id=id, result_key=result_key, capability=capability, status="queued",
            created_at=created_at, urgency=urgency, escalate_at=escalate_at,
            reservation=reservation, deadline_epoch=deadline_epoch,
            client_key=client_key, task_class=task_class,
            submitter_app=submitter_app, submitter_instance=submitter_instance,
            submitter_request_id=submitter_request_id, submitted_at=submitted_at,
            observed_ip=observed_ip, idempotency_key=idempotency_key,
        )
        try:
            with self._session() as s:
                s.add(row)
                s.commit()
        except IntegrityError as e:
            # Matched narrowly rather than catching IntegrityError wholesale: any future
            # constraint on this table would otherwise be converted into a silent
            # "here's your existing job" for a request that actually failed.
            #
            # SQLite names the offending *column*, not the index — the message reads
            # `UNIQUE constraint failed: jobs.idempotency_key` — so that is what to look
            # for. Matching the index name silently never fires.
            if "jobs.idempotency_key" not in str(getattr(e, "orig", e)):
                raise
            return self.job_by_idempotency_key(idempotency_key)
        return None

    def job_by_idempotency_key(self, key: str | None) -> Job | None:
        """The job holding this key, if any. Safe as a follow-up to a failed insert:
        SQLite raises the uniqueness violation only against committed data."""
        if key is None:
            return None
        with self._session() as s:
            return s.exec(select(Job).where(Job.idempotency_key == key)).first()

    def get(self, id: str) -> Job | None:
        with self._session() as s:
            return s.get(Job, id)

    def set_status(self, id: str, status: str) -> None:
        """Set a job's status, stamping `finished_at` the first time it goes terminal.

        The stamp lives here rather than at the call sites because there are four places
        the coordinator first writes a terminal status — the lazy write-back on poll, the
        usage tick's capture and its deadline-expiry branch, and the reaper's dead-letter
        — and hooking only one of them would leave a `done` job with no `finished_at`,
        which is precisely the ambiguity the column exists to remove.

        The stamp is a guarded UPDATE (`WHERE finished_at IS NULL`) rather than
        read-then-write, so two ticks racing the same job cannot overwrite an earlier,
        more accurate observation with a later one.
        """
        with self._session() as s:
            job = s.get(Job, id)
            if job is None:
                return
            job.status = status
            s.add(job)
            if status in TERMINAL_STATUSES:
                s.exec(
                    update(Job)
                    .where(Job.id == id, Job.finished_at.is_(None))
                    .values(finished_at=now_iso())
                )
            s.commit()

    def orphaned_jobs(self, before_iso: str) -> list[Job]:
        """Non-terminal jobs older than `before_iso` that were never enqueued.

        `entry_id IS NULL` is the whole signal: the row was committed and the XADD never
        happened, so no stream entry exists and nothing will ever deliver it. Compared as
        text because `created_at` is ISO-8601 with a `Z`, whose lexical order is
        chronological — the same assumption `recent_usage` already relies on.
        """
        with self._session() as s:
            stmt = select(Job).where(
                Job.status.not_in(TERMINAL_STATUSES),
                Job.entry_id.is_(None),
                Job.created_at < before_iso,
            )
            return list(s.exec(stmt))

    def stale_queued_jobs(self, before_iso: str) -> list[Job]:
        """Non-terminal jobs older than `before_iso`, enqueued or not — the opt-in
        maximum-queue-age backstop's population."""
        with self._session() as s:
            stmt = select(Job).where(
                Job.status.not_in(TERMINAL_STATUSES),
                Job.created_at < before_iso,
            )
            return list(s.exec(stmt))

    def jobs_by_entry_ids(self, entry_ids: list[str]) -> dict[str, Job]:
        """entry_id → job, for the ids currently held in a stream's pending list.

        One query per tick rather than one per claimed entry. Only rows whose recorded
        delivery still matches are returned, so a stale entry id (the reaper requeued the
        job under a new one) simply does not match and is skipped.
        """
        if not entry_ids:
            return {}
        with self._session() as s:
            rows = s.exec(select(Job).where(Job.entry_id.in_(entry_ids))).all()
            return {r.entry_id: r for r in rows if r.entry_id is not None}

    def mark_started(self, id: str, *, at: str, claimed_by: str) -> bool:
        """Record an observed claim: `started_at` + `claimed_by`, and queued → running.

        Guarded twice, because this runs from a periodic tick against jobs a client may
        be polling concurrently: `started_at IS NULL` keeps the first (true) observation,
        and the status only advances from `queued`, so it can never clobber a terminal
        status written by a poll or the usage tick in between.

        Returns True if this call was the one that recorded the claim.
        """
        with self._session() as s:
            result = s.exec(
                update(Job)
                .where(Job.id == id, Job.started_at.is_(None))
                .values(started_at=at, claimed_by=claimed_by)
            )
            changed = bool(result.rowcount)
            s.exec(
                update(Job)
                .where(Job.id == id, Job.status == "queued")
                .values(status="running")
            )
            s.commit()
            return changed

    def request_cancel(self, id: str) -> bool:
        """Mark a cancellation we could not prove, and floor the job's deadline to now.

        The floor is the load-bearing half. Since Redis 7, `XAUTOCLAIM` *drops* pending
        entries whose stream entry no longer exists — so withdrawing an entry a worker
        already holds and then losing that worker leaves the job with no result blob, no
        terminal status, and nothing able to recover it: the reaper cannot see it, and
        `jobs_awaiting_usage` would re-select it on every tick forever. Flooring the
        deadline hands it to the expiry sweep, which already writes both halves.

        Returns False if the job is already over, in which case there is nothing to do.
        """
        with self._session() as s:
            job = s.get(Job, id)
            if job is None or job.status in TERMINAL_STATUSES:
                return False
            job.cancel_requested = 1
            now = datetime.now(UTC).timestamp()
            job.deadline_epoch = min(job.deadline_epoch or now, now)
            s.add(job)
            s.commit()
            return True

    def record_delivery(self, id: str, *, stream: str, entry_id: str) -> None:
        """Remember the stream entry now representing this job.

        Cannot ride `insert`: the row is written before the XADD, so the entry id does
        not exist yet. Every enqueue must call this — including the reaper's requeue,
        which mints a *new* entry id, and would otherwise leave queue position and
        withdrawal pointing at an entry that no longer exists.
        """
        with self._session() as s:
            job = s.get(Job, id)
            if job is not None:
                job.entry_id = entry_id
                job.stream = stream
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
                # Filtered on status as well as urgency: without this a cancelled job
                # with a due escalation still promotes and may WAKE A MACHINE, which is
                # the worst outcome available in a cold-by-default fleet.
                Job.status.not_in(TERMINAL_STATUSES),
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
                # Same guard as due_for_escalation, and more exposed: this promotes in
                # bulk with no per-row result check at all.
                Job.status.not_in(TERMINAL_STATUSES),
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
        with self._session() as s:
            lease = s.get(AttentionLease, client_key)
            if lease is None:
                lease = AttentionLease(client_key=client_key, scope=scope, expires_at=expires_at)
            else:
                lease.scope = scope
                lease.expires_at = expires_at
            s.add(lease)
            s.commit()

    def delete_attention_lease(self, client_key: str) -> None:
        with self._session() as s:
            lease = s.get(AttentionLease, client_key)
            if lease is not None:
                s.delete(lease)
                s.commit()

    def expired_attention_leases(self, now: float) -> list[AttentionLease]:
        with self._session() as s:
            stmt = select(AttentionLease).where(AttentionLease.expires_at <= now)
            return list(s.exec(stmt))

    # --- ability matrix (M5) ---

    def set_ability(self, *, artifact: str, task_class: str, score: float,
                    scale_version: str, updated_at: str,
                    provenance: str = "measured",
                    n_items: int | None = None, n_passed: int | None = None) -> None:
        with self._session() as s:
            key = (artifact, task_class, scale_version)
            row = s.get(Ability, key)
            if row is None:
                row = Ability(artifact=artifact, task_class=task_class, score=score,
                              scale_version=scale_version, updated_at=updated_at,
                              provenance=provenance, n_items=n_items, n_passed=n_passed)
            else:
                row.score = score
                row.updated_at = updated_at
                row.provenance = provenance
                # Overwritten, not merged: these describe THIS measurement. Keeping a
                # previous round's counts beside a new score would misreport the evidence.
                row.n_items = n_items
                row.n_passed = n_passed
            s.add(row)
            s.commit()

    def ability_provenance(self, artifact: str, task_class: str,
                           scale_version: str) -> str | None:
        with self._session() as s:
            row = s.get(Ability, (artifact, task_class, scale_version))
            return row.provenance if row else None

    def supersede_artifact(self, artifact: str, scale_version: str, *,
                           now: str) -> tuple[int, int]:
        """ADR 15: this is a NEW artifact and inherits NOTHING. Returns (scores dropped,
        new generation).

        Two effects, and both are needed. Dropping the stored scores alone left the *inputs*
        to the next score in place: ability is recomputed from every eval_run recorded for
        the (artifact, task_class), so the re-eval averaged the new artifact's items together
        with the old artifact's — a model that now passes everything was recorded at 8.0
        instead of 10.0, carrying the superseded measurement forward. So opening a new
        generation is part of the same act, not a separate chore for each caller to remember
        (there are two: the heartbeat's digest change, and the operator's /ability/clear).

        Runs still in flight for the previous generation are retired rather than left
        pending: their jobs measure the artifact that is no longer here, and leaving them
        `pending` would also keep the artifact permanently "under eval" and undispatchable.
        """
        with self._session() as s:
            rows = list(s.exec(select(Ability).where(
                Ability.artifact == artifact, Ability.scale_version == scale_version
            )))
            for row in rows:
                s.delete(row)

            for run in s.exec(select(EvalRun).where(
                EvalRun.artifact == artifact, EvalRun.state == "pending"
            )):
                run.state = "stale"
                s.add(run)

            marker = s.get(EvalGeneration, artifact)
            if marker is None:
                marker = EvalGeneration(artifact=artifact, generation=2, updated_at=now)
            else:
                marker.generation += 1
                marker.updated_at = now
            s.add(marker)
            s.commit()
            return len(rows), marker.generation

    def current_eval_generation(self, artifact: str) -> int:
        """The measurement round new runs for this artifact belong to."""
        with self._session() as s:
            marker = s.get(EvalGeneration, artifact)
            return marker.generation if marker else 1

    def failed_eval_runs(self, artifact: str, task_class: str, generation: int = 1) -> int:
        """No-signal runs in ONE generation. Counting every generation let a batch that
        once failed its way to the cap block the artifact from ever being re-measured."""
        with self._conn() as c:
            return c.execute(
                "SELECT COUNT(*) AS n FROM eval_runs "
                "WHERE artifact = ? AND task_class = ? AND state = 'failed' "
                "AND generation = ?",
                (artifact, task_class, generation),
            ).fetchone()["n"]

    def get_ability(self, artifact: str, task_class: str, scale_version: str) -> float | None:
        with self._session() as s:
            row = s.get(Ability, (artifact, task_class, scale_version))
            return float(row.score) if row else None

    def ability_matrix(self, scale_version: str) -> list[Ability]:
        with self._session() as s:
            stmt = (
                select(Ability)
                .where(Ability.scale_version == scale_version)
                .order_by(Ability.artifact, Ability.task_class)
            )
            return list(s.exec(stmt))

    # --- catalog / observed models / proposals (M6b) ---

    def upsert_catalog(self, *, artifact: str, family: str | None, params_b: float | None,
                       quant: str | None, size_gb: float, min_ram_gb: float, source: str,
                       registry_ref: str, expected_ability: float | None,
                       added_at: str) -> None:
        with self._session() as s:
            row = s.get(CatalogEntry, artifact)
            if row is None:
                row = CatalogEntry(
                    artifact=artifact, family=family, params_b=params_b, quant=quant,
                    size_gb=size_gb, min_ram_gb=min_ram_gb, source=source,
                    registry_ref=registry_ref, expected_ability=expected_ability,
                    added_at=added_at,
                )
            else:
                row.family = family
                row.params_b = params_b
                row.quant = quant
                row.size_gb = size_gb
                row.min_ram_gb = min_ram_gb
                row.source = source
                row.registry_ref = registry_ref
                row.expected_ability = expected_ability
            s.add(row)
            s.commit()

    def list_catalog(self) -> list[CatalogEntry]:
        with self._session() as s:
            stmt = select(CatalogEntry).order_by(CatalogEntry.min_ram_gb, CatalogEntry.artifact)
            return list(s.exec(stmt))

    def catalog_count(self) -> int:
        with self._conn() as c:
            return c.execute("SELECT COUNT(*) AS n FROM catalog").fetchone()["n"]

    def observe_node_models(self, node_id: str, artifacts: dict[str, str | None],
                            now: str) -> list[tuple[str, str | None, str | None]]:
        """Record what a node reports having. Returns (artifact, old_digest, new_digest)
        for artifacts whose digest CHANGED — i.e. the artifact was updated upstream."""
        changed: list[tuple[str, str | None, str | None]] = []
        with self._session() as s:
            for artifact, digest in artifacts.items():
                row = s.get(NodeModel, (node_id, artifact))
                if row is None:
                    s.add(NodeModel(node_id=node_id, artifact=artifact, digest=digest,
                                    first_seen=now, last_seen=now))
                else:
                    old = row.digest
                    if digest is not None and old is not None and digest != old:
                        changed.append((artifact, old, digest))
                    row.digest = digest if digest is not None else row.digest
                    row.last_seen = now
                    s.add(row)
            s.commit()
        return changed

    def node_models(self, node_id: str) -> list[NodeModel]:
        with self._session() as s:
            stmt = (
                select(NodeModel)
                .where(NodeModel.node_id == node_id)
                .order_by(NodeModel.artifact)
            )
            return list(s.exec(stmt))

    def insert_proposal(self, *, id: str, kind: str, node_id: str, artifact: str,
                        incumbent: str | None, task_class: str | None, rationale: str,
                        status: str, created_at: str) -> None:
        with self._session() as s:
            s.add(Proposal(
                id=id, kind=kind, node_id=node_id, artifact=artifact, incumbent=incumbent,
                task_class=task_class, rationale=rationale, status=status,
                created_at=created_at,
            ))
            s.commit()

    def open_proposal_exists(self, *, kind: str, node_id: str, artifact: str) -> bool:
        """True if an undecided/approved proposal already covers this — keeps the scan
        idempotent so a repeating tick can't spam duplicates."""
        with self._session() as s:
            stmt = select(Proposal).where(
                Proposal.kind == kind, Proposal.node_id == node_id,
                Proposal.artifact == artifact,
                Proposal.status.in_(["pending", "approved"]),
            ).limit(1)
            return s.exec(stmt).first() is not None

    def list_proposals(self, status: str | None = None) -> list[Proposal]:
        with self._session() as s:
            stmt = select(Proposal)
            if status:
                stmt = stmt.where(Proposal.status == status)
            stmt = stmt.order_by(Proposal.created_at.desc())
            return list(s.exec(stmt))

    def get_proposal(self, id: str) -> Proposal | None:
        with self._session() as s:
            return s.get(Proposal, id)

    def decide_proposal(self, id: str, status: str, decided_at: str) -> bool:
        """Approve/deny a pending proposal. False if it wasn't pending (already decided)."""
        with self._session() as s:
            row = s.get(Proposal, id)
            if row is None or row.status != "pending":
                return False
            row.status = status
            row.decided_at = decided_at
            s.add(row)
            s.commit()
            return True

    def set_proposal_status(self, id: str, status: str) -> None:
        with self._session() as s:
            row = s.get(Proposal, id)
            if row is not None:
                row.status = status
                s.add(row)
                s.commit()

    def approved_proposals_for_node(self, node_id: str, kind: str) -> list[Proposal]:
        with self._session() as s:
            stmt = select(Proposal).where(
                Proposal.node_id == node_id, Proposal.kind == kind,
                Proposal.status == "approved",
            ).order_by(Proposal.created_at)
            return list(s.exec(stmt))

    def set_node_flags(self, node_id: str, *, disk_quota_gb: float | None = None,
                       auto_approve: bool | None = None,
                       auto_update: bool | None = None) -> None:
        with self._session() as s:
            node = s.get(Node, node_id)
            if node is None:
                return
            if disk_quota_gb is not None:
                node.disk_quota_gb = disk_quota_gb
            if auto_approve is not None:
                node.auto_approve = 1 if auto_approve else 0
            if auto_update is not None:
                node.auto_update = 1 if auto_update else 0
            s.add(node)
            s.commit()

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
                     item_index: int, result_key: str, created_at: str,
                     generation: int = 1, suite_version: str = "") -> None:
        with self._session() as s:
            if s.get(EvalRun, job_id) is not None:
                return  # INSERT OR IGNORE equivalent
            s.add(EvalRun(
                job_id=job_id, artifact=artifact, task_class=task_class,
                item_index=item_index, result_key=result_key, created_at=created_at,
                generation=generation, suite_version=suite_version,
            ))
            s.commit()

    def pending_eval_runs(self) -> list[EvalRun]:
        with self._session() as s:
            stmt = (
                select(EvalRun)
                .where(EvalRun.state == "pending")
                .order_by(EvalRun.created_at)
            )
            return list(s.exec(stmt))

    def score_eval_run(self, job_id: str, passed: bool) -> None:
        with self._session() as s:
            row = s.get(EvalRun, job_id)
            if row is not None:
                row.state = "scored"
                row.passed = 1 if passed else 0
                s.add(row)
                s.commit()

    def fail_eval_run(self, job_id: str) -> None:
        """The job itself failed/expired — the item yields no signal, so don't score it."""
        with self._session() as s:
            row = s.get(EvalRun, job_id)
            if row is not None:
                row.state = "failed"
                s.add(row)
                s.commit()

    def eval_progress(self, artifact: str, task_class: str,
                      generation: int = 1) -> tuple[int, int, int, int]:
        """(pending, scored, passed, failed) for ONE batch — one (artifact, task_class,
        generation).

        `failed` is reported separately and deliberately: a run that produced no signal is
        neither pending nor scored, so omitting it made a partly-broken batch look finished
        and recorded an ability from only the surviving items.

        `generation` is what keeps a batch a batch. Aggregating across generations meant a
        re-measured artifact was scored on its predecessor's items too (ADR 15); retired
        (`stale`) runs are excluded for the same reason.
        """
        with self._conn() as c:
            r = c.execute(
                "SELECT "
                "SUM(state = 'pending') AS pending, "
                "SUM(state = 'scored') AS scored, "
                "SUM(passed = 1) AS passed, "
                "SUM(state = 'failed') AS failed "
                "FROM eval_runs WHERE artifact = ? AND task_class = ? AND generation = ?",
                (artifact, task_class, generation),
            ).fetchone()
            return (r["pending"] or 0, r["scored"] or 0, r["passed"] or 0, r["failed"] or 0)

    def artifacts_under_eval(self) -> set[str]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT DISTINCT artifact FROM eval_runs WHERE state = 'pending'"
            ).fetchall()
            return {r["artifact"] for r in rows}

    def eval_runs_summary(self, limit: int = 50) -> list[sqlite3.Row]:
        """One row per batch. Grouped by generation as well as (artifact, task_class), so a
        re-measured artifact reads as a fresh batch rather than as its predecessor's totals
        plus its own — the same separation the score itself depends on."""
        with self._conn() as c:
            return c.execute(
                "SELECT artifact, task_class, generation, "
                "SUM(state = 'pending') AS pending, SUM(state = 'scored') AS scored, "
                "SUM(state = 'failed') AS failed, SUM(passed = 1) AS passed "
                "FROM eval_runs WHERE state != 'stale' "
                "GROUP BY artifact, task_class, generation "
                "ORDER BY artifact, task_class, generation DESC LIMIT ?",
                (limit,),
            ).fetchall()

    def installed_artifacts(self) -> list[tuple[str, str, list[str]]]:
        """(node_id, artifact, node_capabilities) for every artifact observed on a node."""
        out: list[tuple[str, str, list[str]]] = []
        for n in self.list_nodes():
            caps = json.loads(n.capabilities or "[]")
            for artifact in json.loads(n.installed or "[]"):
                out.append((n.node_id, artifact, caps))
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
        with self._session() as s:
            s.add(Reservation(
                id=id, status=status, state=state, task_class=task_class,
                min_ability=min_ability, capability=capability, node=node,
                artifact=artifact, priority=priority, privacy=privacy, load=load,
                duration_min=duration_min, est_jobs=est_jobs, warm_by=warm_by,
                starts=starts, ends=ends, created_at=created_at,
            ))
            s.commit()

    def get_reservation(self, id: str) -> Reservation | None:
        with self._session() as s:
            return s.get(Reservation, id)

    def list_reservations(self, limit: int = 50) -> list[Reservation]:
        with self._session() as s:
            stmt = (
                select(Reservation)
                .order_by(Reservation.created_at.desc())
                .limit(limit)
            )
            return list(s.exec(stmt))

    def active_reservations(self) -> list[Reservation]:
        """Confirmed reservations still in a live lifecycle state (for the reconciler)."""
        with self._session() as s:
            stmt = select(Reservation).where(
                Reservation.status == "confirmed",
                Reservation.state.in_(["scheduled", "warming", "open", "draining"]),
            )
            return list(s.exec(stmt))

    def set_reservation_state(self, id: str, state: str) -> None:
        with self._session() as s:
            r = s.get(Reservation, id)
            if r is not None:
                r.state = state
                s.add(r)
                s.commit()

    def cancel_reservation(self, id: str) -> bool:
        """Flip to cancelled unless already terminal. Returns True if it changed."""
        with self._session() as s:
            r = s.get(Reservation, id)
            if r is None or r.state in ("closed", "cancelled"):
                return False
            r.state = "cancelled"
            s.add(r)
            s.commit()
            return True

    # --- usage metering (M3a) ---

    def jobs_awaiting_usage(self) -> list[Job]:
        """Jobs with no usage record yet. Capture is gated on this (not job status), so a
        client's GET flipping status to done can't make the scan miss a job. Rows drop out
        once a usage row exists, keeping the scan bounded to uncaptured work."""
        with self._session() as s:
            stmt = (
                select(Job)
                .join(Usage, Usage.job_id == Job.id, isouter=True)
                .where(Usage.job_id.is_(None))
            )
            return list(s.exec(stmt))

    def record_usage(self, *, job_id: str, ts: str, capability: str | None,
                     model: str | None, node: str | None, venue: str,
                     tokens_in: int, tokens_out: int, outcome: str, cost: float,
                     day: str) -> None:
        """Write one usage record. Idempotent under tick/GET races — silently does
        nothing if a row for this job already exists (INSERT OR IGNORE equivalent)."""
        with self._session() as s:
            if s.get(Usage, job_id) is not None:
                return
            s.add(Usage(
                job_id=job_id, ts=ts, capability=capability, model=model, node=node,
                venue=venue, tokens_in=tokens_in, tokens_out=tokens_out, outcome=outcome,
                cost=cost, day=day,
            ))
            s.commit()

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

    def recent_usage(self, limit: int = 40) -> list[Usage]:
        """Most recent usage rows, newest first — drives the dashboard timeline. `ts` is
        ISO-8601 with a 'Z' suffix (usage_scan), so ordering it as text sorts chronologically."""
        with self._session() as s:
            stmt = select(Usage).order_by(Usage.ts.desc()).limit(limit)
            return list(s.exec(stmt))

    def usage_daily_by_venue(self, days: int = 30) -> list[sqlite3.Row]:
        """Jobs/tokens/cost per (day, venue) over the trailing `days` — drives the usage
        page's activity-over-time chart. `day` is 'YYYY-MM-DD' (usage_scan), so lexicographic
        comparison sorts and filters chronologically without parsing."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        with self._conn() as c:
            return c.execute(
                "SELECT day, venue, COUNT(*) AS jobs, "
                "COALESCE(SUM(tokens_in), 0) AS tokens_in, "
                "COALESCE(SUM(tokens_out), 0) AS tokens_out, "
                "COALESCE(SUM(cost), 0) AS cost "
                "FROM usage WHERE day >= ? GROUP BY day, venue ORDER BY day",
                (cutoff,),
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
        with self._session() as s:
            s.add(JoinToken(token=token, created_at=created_at))
            s.commit()

    def unused_token_count(self) -> int:
        """Join tokens minted but not yet burned.

        Exists so the bootstrap route's token hygiene is assertable: validating the join
        password BEFORE minting is what stops an unauthenticated caller growing this table
        with failed guesses.
        """
        with self._session() as s:
            return len(list(s.exec(select(JoinToken).where(JoinToken.used == 0))))

    def claim_token(self, token: str, used_by: str) -> bool:
        """Atomically burn a one-time join token. True only on the first claim."""
        with self._session() as s:
            t = s.get(JoinToken, token)
            if t is None or t.used:
                return False
            t.used = 1
            t.used_by = used_by
            s.add(t)
            s.commit()
            return True

    def enroll_node(self, *, node_id: str, node_key: str, req, capabilities: str,
                    enrolled_at: str) -> None:
        with self._session() as s:
            s.add(Node(
                node_id=node_id, node_key=node_key, hostname=req.hostname, os=req.os,
                arch=req.arch, ram_gb=req.hw.ram_gb, accelerator=req.hw.accelerator,
                vram_gb=req.hw.vram_gb, disk_free_gb=req.hw.disk_free_gb,
                bench_tps_small=req.hw.bench_tps_small, profile=req.profile,
                capabilities=capabilities, mode="active", enrolled_at=enrolled_at,
            ))
            s.commit()

    def get_node(self, node_id: str) -> Node | None:
        with self._session() as s:
            return s.get(Node, node_id)

    def list_nodes(self) -> list[Node]:
        with self._session() as s:
            stmt = select(Node).order_by(Node.enrolled_at)
            return list(s.exec(stmt))

    def record_heartbeat(self, *, node_id: str, mode: str, installed: str, loaded: str,
                         queues: str, jobs_done: int | None, tps: float | None,
                         last_heartbeat: str, agent_version: str | None = None,
                         agent_flavour: str | None = None,
                         protocol_version: int | None = None,
                         fitness: str | None = None,
                         fitness_reason: str | None = None) -> bool:
        with self._session() as s:
            node = s.get(Node, node_id)
            if node is None:
                return False
            node.mode = mode
            node.installed = installed
            node.loaded = loaded
            node.queues = queues
            if agent_version is not None:
                node.agent_version = agent_version
            if agent_flavour is not None:
                node.agent_flavour = agent_flavour
            if protocol_version is not None:
                node.protocol_version = protocol_version
            node.fitness = fitness
            node.fitness_reason = fitness_reason
            if jobs_done is not None:
                node.jobs_done = jobs_done
            if tps is not None:
                node.tps = tps
            node.last_heartbeat = last_heartbeat
            s.add(node)
            s.commit()
            return True

    # --- performance load tests ---

    def create_perf_run(self, *, id: str, label: str, config: str, snapshot: str | None,
                        started_at: str) -> None:
        with self._session() as s:
            s.add(PerfRun(id=id, label=label, status="running", config=config,
                          snapshot=snapshot, started_at=started_at))
            s.commit()

    def finish_perf_run(self, id: str, status: str, finished_at: str) -> None:
        with self._session() as s:
            run = s.get(PerfRun, id)
            if run is None:
                return
            run.status = status
            run.finished_at = finished_at
            s.add(run)
            s.commit()

    def get_perf_run(self, id: str) -> PerfRun | None:
        with self._session() as s:
            return s.get(PerfRun, id)

    def list_perf_runs(self, limit: int = 50) -> list[PerfRun]:
        with self._session() as s:
            stmt = select(PerfRun).order_by(PerfRun.started_at.desc()).limit(limit)
            return list(s.exec(stmt))

    def add_perf_sample(self, *, id: str, run_id: str, job_id: str | None, category: str,
                        task_class: str, min_ability: int, capability: str | None,
                        node: str | None, phase: str, submitted_at: float,
                        completed_at: float | None, latency_s: float | None,
                        tokens_in: int | None, tokens_out: int | None, outcome: str,
                        passed: bool | None, detail: str | None) -> None:
        with self._session() as s:
            s.add(PerfSample(
                id=id, run_id=run_id, job_id=job_id, category=category,
                task_class=task_class, min_ability=min_ability, capability=capability,
                node=node, phase=phase, submitted_at=submitted_at,
                completed_at=completed_at, latency_s=latency_s, tokens_in=tokens_in,
                tokens_out=tokens_out, outcome=outcome,
                passed=None if passed is None else (1 if passed else 0), detail=detail,
            ))
            s.commit()

    def perf_run_samples(self, run_id: str) -> list[PerfSample]:
        with self._session() as s:
            stmt = (
                select(PerfSample)
                .where(PerfSample.run_id == run_id)
                .order_by(PerfSample.submitted_at)
            )
            return list(s.exec(stmt))

    def perf_run_stats(self, run_id: str) -> sqlite3.Row:
        """Aggregate stats computed in SQL, for the runs-list view — avoids loading every
        sample of every run on each dashboard poll. No percentiles (needs the sorted
        sample list); the single-run detail view still uses perf_run_samples for those."""
        with self._conn() as c:
            return c.execute(
                "SELECT "
                "SUM(phase = 'measure') AS n_measured, "
                "SUM(phase = 'warmup') AS n_warmup, "
                "SUM(phase = 'measure' AND outcome = 'done') AS n_served, "
                "SUM(phase = 'measure' AND outcome = 'unassigned') AS n_unassigned, "
                "SUM(phase = 'measure' AND outcome IN ('failed', 'expired', 'timeout')) "
                "  AS n_failed, "
                "SUM(phase = 'measure' AND outcome = 'done' AND passed = 1) AS n_passed, "
                "SUM(phase = 'measure' AND outcome = 'done' AND passed IS NOT NULL) "
                "  AS n_scored, "
                "AVG(CASE WHEN phase = 'measure' AND outcome = 'done' "
                "  THEN latency_s END) AS mean_latency_s, "
                "SUM(CASE WHEN phase = 'measure' AND outcome = 'done' "
                "  THEN COALESCE(tokens_in, 0) + COALESCE(tokens_out, 0) ELSE 0 END) "
                "  AS total_tokens, "
                "MIN(CASE WHEN phase = 'measure' THEN submitted_at END) AS span_start, "
                "MAX(CASE WHEN phase = 'measure' "
                "  THEN COALESCE(completed_at, submitted_at) END) AS span_end "
                "FROM perf_samples WHERE run_id = ?",
                (run_id,),
            ).fetchone()

    def cancel_stale_perf_runs(self, finished_at: str) -> int:
        """No perf task can still be running from a previous process — reconcile any row
        left 'running' (e.g. after a SIGKILL) so its cancel button isn't a no-op forever."""
        with self._session() as s:
            stale = list(s.exec(select(PerfRun).where(PerfRun.status == "running")))
            for run in stale:
                run.status = "cancelled"
                run.finished_at = finished_at
                s.add(run)
            s.commit()
            return len(stale)
