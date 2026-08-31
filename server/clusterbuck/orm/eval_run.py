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
    # `stale` = retired when the artifact was superseded mid-flight.
    state: str = Field(default="pending", sa_column_kwargs={"server_default": "'pending'"})
    passed: int | None = None  # 1/0 once scored
    created_at: str
    # The measurement round this run belongs to (see orm/eval_generation.py). Ability is
    # recomputed from these rows, so without it a re-eval averaged the NEW artifact's items
    # together with the old one's — the inheritance ADR 15 forbids.
    generation: int = Field(default=1, sa_column_kwargs={"server_default": "1"})
