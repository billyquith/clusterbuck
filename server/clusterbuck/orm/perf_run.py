"""The `perf_runs` table (Performance page): one row per load-test run.

A stream of randomized queries against the real /jobs API for a sustained duration,
scored for adequacy as each reply lands. No cloud escalation — a request the local fleet
can't meet is recorded as `unassigned` (see `perf_samples`), not retried elsewhere.
"""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class PerfRun(SQLModel, table=True):
    __tablename__ = "perf_runs"

    # nullable=True on a PK looks wrong but is deliberate — see clusterbuck/orm/job.py's
    # note on the same SQLite quirk.
    id: str = Field(primary_key=True, nullable=True)
    label: str
    status: str  # running | done | cancelled
    config: str  # json: categories, concurrency, duration_s, etc.
    snapshot: str | None = None  # json: nodes/fleet state observed at start
    started_at: str
    finished_at: str | None = None
