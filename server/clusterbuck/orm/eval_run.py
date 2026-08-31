"""The `eval_runs` table (M7): one row per dispatched tier-1 eval item. Evals are
ordinary jobs on the fleet (model-evaluation.md — "the harness is just another
client"), so this table is what correlates a job back to the item it was measuring.
"""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class EvalRun(SQLModel, table=True):
    __tablename__ = "eval_runs"

    # nullable=True on a PK looks wrong but is deliberate — see clusterbuck/orm/job.py's
    # note on the same SQLite quirk.
    job_id: str = Field(primary_key=True, nullable=True)
    artifact: str
    task_class: str
    item_index: int
    result_key: str
    # server_default (not just a Python-side default) to match the original
    # `NOT NULL DEFAULT 'pending'` DDL exactly.
    state: str = Field(default="pending", sa_column_kwargs={"server_default": "'pending'"})
    passed: int | None = None  # 1/0 once scored
    created_at: str
