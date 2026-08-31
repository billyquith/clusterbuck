"""The `jobs` table: coordinator-side lifecycle + escalation tracking for one job.

Distinct from `JobRecord` in `models.py`, which mirrors `contract/job.schema.json` (the
Redis queue payload sent to a worker) — this is the SQLite side, read by the escalation
engine, reaper, attention tick, and usage scan to track status and urgency trajectory
without re-reading queue payloads. The two shapes already differ (e.g. `escalate_at`
here is an absolute epoch deadline computed from `JobRecord`'s `escalate_after_min`) and
that's intentional, not drift to fix as part of this migration.
"""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class Job(SQLModel, table=True):
    __tablename__ = "jobs"

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
    client_key: str | None = None  # optional client identity (for attention scoping)
    task_class: str | None = None  # need-shaped task class (for attention scoping)
    promoted_by: str | None = None  # null | 'age' | 'attention' (escalation provenance)
    attempts: int = Field(default=0, sa_column_kwargs={"server_default": "0"})  # delivery attempts, incremented by the reaper
