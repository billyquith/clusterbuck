"""The `perf_samples` table (Performance page): one row per generated query.

`job_id` is null for `unassigned` (no capability met the requested ability, so no job
was ever created). `passed` is the query's own check verdict (null when there was no
reply to score — failed/expired/unassigned).
"""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class PerfSample(SQLModel, table=True):
    __tablename__ = "perf_samples"

    # nullable=True on a PK looks wrong but is deliberate — see clusterbuck/orm/job.py's
    # note on the same SQLite quirk.
    id: str = Field(primary_key=True, nullable=True)
    run_id: str
    job_id: str | None = None
    category: str
    task_class: str
    min_ability: int
    capability: str | None = None
    node: str | None = None
    phase: str  # warmup | measure
    submitted_at: float
    completed_at: float | None = None
    latency_s: float | None = None
    tokens_in: int | None = None
    tokens_out: int | None = None
    outcome: str  # done | failed | timeout | unassigned
    passed: int | None = None  # 1 | 0 | NULL
    detail: str | None = None
