"""The `jobs` table: coordinator-side lifecycle + escalation tracking for one job.

Distinct from `JobRecord` in `models.py`, which mirrors `contract/job.schema.json` (the
Redis queue payload sent to a worker) — this is the SQLite side, read by the escalation
engine, reaper, and usage scan to track status and urgency trajectory
without re-reading queue payloads. The two shapes already differ (e.g. `escalate_at`
here is an absolute epoch deadline computed from `JobRecord`'s `escalate_after_min`) and
that's intentional, not drift to fix as part of this migration.
"""

from __future__ import annotations

from sqlalchemy import Index
from sqlmodel import Field, SQLModel


class Job(SQLModel, table=True):
    __tablename__ = "jobs"

    # Declared as a separate Index rather than Field(unique=True) deliberately. A
    # column-level unique renders INSIDE `CREATE TABLE`, which the legacy pre-Alembic
    # bootstrap cannot reproduce with `ALTER TABLE ADD COLUMN` — so the two schema paths
    # would silently diverge and the constraint would simply not exist on an upgraded
    # database. As its own index it renders as `CREATE UNIQUE INDEX`, identical on both.
    #
    # A plain (not partial) unique index is correct because SQLite treats NULLs as
    # DISTINCT: every pre-existing row and every submit without a key coexists freely,
    # and only two submits sharing a real key collide. Do not "fix" this into a partial
    # index — that is what would diverge the two DDL paths.
    __table_args__ = (
        Index("ix_jobs_idempotency_key", "idempotency_key", unique=True),
    )

    # nullable=True on a PK looks wrong but is deliberate: this table has always been
    # `id TEXT PRIMARY KEY` with no explicit NOT NULL, and SQLite (unlike most engines)
    # doesn't imply NOT NULL from PRIMARY KEY alone on a non-INTEGER column — but
    # SQLAlchemy's own default for primary_key=True *is* nullable=False, so it must be
    # overridden here to avoid silently tightening the constraint on new databases.
    id: str = Field(primary_key=True, nullable=True)
    result_key: str
    capability: str
    status: str
    created_at: str
    # server_default (not just a Python-side default) so this matches the original
    # `NOT NULL DEFAULT ...` DDL exactly, in case anything ever inserts a bare row.
    urgency: str = Field(default="waitable", sa_column_kwargs={"server_default": "'waitable'"})
    escalate_at: float | None = None  # epoch seconds; only set for waitable(N)
    escalated: int = Field(default=0, sa_column_kwargs={"server_default": "0"})
    reservation: str | None = None  # opt-in reservation id this job queues against
    deadline_epoch: float | None = None  # epoch seconds; for expiry sweep
    client_key: str | None = None  # opaque client label (the eval harness tags with it)
    task_class: str | None = None  # need-shaped task class, as submitted
    promoted_by: str | None = None  # null | 'age' — escalation provenance
    attempts: int = Field(default=0, sa_column_kwargs={"server_default": "0"})  # delivery attempts, incremented by the reaper

    # --- caller provenance (protocols.md §1b) ---------------------------------------
    # Flattened from JobRecord's nested `submitter`: this table is scanned by the
    # escalation/reaper/usage ticks, and columns keep those scans indexable where a JSON
    # blob would not. All nullable — jobs from clients predating this carry none.
    submitter_app: str | None = None
    submitter_instance: str | None = None
    # The duplicate-vs-repeat discriminator. Deliberately NOT unique: a repeated value is
    # a client retrying one logical call, which is the signal, not a constraint breach.
    submitter_request_id: str | None = None
    # The client's own clock. Advisory only — `created_at` stays authoritative.
    submitted_at: str | None = None
    # Stamped server-side at receipt, never read from the request body (a client could
    # lie). This is the only provenance that identifies a client sending none at all —
    # exactly the case a runaway submitter presents. Coordinator-side only: it is not in
    # contract/job.schema.json, because the worker has no use for it.
    observed_ip: str | None = None

    # --- lifecycle timing + delivery (protocols.md §1b) -----------------------------
    # All coordinator-observed, so a polling client can render an honest wait state
    # without timing jobs locally. Nullable with no default: rows predating this have no
    # observation, and NULL is the honest value.
    #
    # `started_at` is when the coordinator OBSERVED a claim (from the stream's
    # pending-entries list), stamped as the true delivery instant rather than the tick
    # clock. A job claimed and acked between two ticks never enters the PEL, so it ends
    # up with `finished_at` set and this NULL — that is expected, not a bug, and
    # `started_at IS NULL` must never be read as "not started".
    started_at: str | None = None
    # When the coordinator first wrote a terminal status. Stamped inside
    # `Store.set_status` (not at its call sites) so every terminal path gets it.
    finished_at: str | None = None
    # Who CLAIMED the job, from the PEL consumer name. Distinct from `usage.node`, which
    # records who *completed* it: different writer, different time. This is the only
    # thing that can answer "has anything picked this up" while a job is still running.
    claimed_by: str | None = None
    # The stream entry currently representing this job, and the stream it sits on.
    # Written after the XADD (the row is inserted before it), and rewritten by the
    # reaper, which mints a NEW entry id on every requeue. `stream` is stored rather
    # than derived because escalation changes a job's urgency *after* enqueue, so the
    # entry can sit on a stream that no longer matches its current urgency.
    entry_id: str | None = None
    stream: str | None = None

    # --- idempotency + cancellation (protocols.md §1b) -------------------------------
    # A client-supplied key making POST /jobs safe to retry: the unique index above is
    # the arbiter, so a lost submit response can be retried without risking a duplicate
    # job. Distinct from `submitter_request_id`, which is identification only and is
    # deliberately never enforced.
    idempotency_key: str | None = None
    # Set when a cancel could not be proven (a worker already holds the entry, or the
    # entry is gone). It exists because Redis 7's XAUTOCLAIM DROPS pending entries whose
    # stream entry no longer exists: withdrawing a claimed entry and then losing the
    # worker would leave the job with no blob, no terminal status, and nothing to recover
    # it. Paired with a floored `deadline_epoch`, the expiry sweep terminalises it.
    cancel_requested: int = Field(default=0, sa_column_kwargs={"server_default": "0"})
