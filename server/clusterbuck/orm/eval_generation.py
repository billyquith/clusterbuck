"""The `eval_generations` table: the current measurement round per artifact (ADR 15).

Ability is recomputed from the `eval_runs` rows for an (artifact, task_class), so
"this artifact must be re-measured" has to invalidate those measurements as well as the
score itself — otherwise the next batch is averaged with the previous artifact's items and
the superseded measurement is carried forward under a new digest.

A separate table rather than `MAX(eval_runs.generation)` because the bump has to survive an
artifact that has no runs yet: an operator can clear an artifact that was never measured.
"""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class EvalGeneration(SQLModel, table=True):
    __tablename__ = "eval_generations"

    # nullable=True on a PK looks wrong but is deliberate — see clusterbuck/orm/job.py's
    # note on the same SQLite quirk.
    artifact: str = Field(primary_key=True, nullable=True)
    generation: int
    updated_at: str
