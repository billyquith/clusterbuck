"""The `ability` table (ADR 15 / model-evaluation.md): ability(artifact, task_class) on
an anchored 1-10 scale, versioned. artifact = model + quantisation. Stored data the
router reads; tier-1 programmatic eval writes it.
"""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class Ability(SQLModel, table=True):
    __tablename__ = "ability"

    artifact: str = Field(primary_key=True)
    task_class: str = Field(primary_key=True)
    score: float
    scale_version: str = Field(primary_key=True)
    updated_at: str
    # 'seed' = an anchored placeholder so routing works before anything is measured;
    # 'measured' = earned from a real eval run. Seeds must never look measured, or the
    # fleet's own models are exempted from evaluation forever.
    # server_default (not just a Python-side default) to match the original
    # `NOT NULL DEFAULT 'measured'` DDL exactly.
    provenance: str = Field(
        default="measured", sa_column_kwargs={"server_default": "'measured'"}
    )
    # How much evidence is behind `score`. model-evaluation.md requires scores to be
    # reported "with an uncertainty note", which was impossible while the table held a bare
    # number: a 10 from one item and a 7 from forty looked identical. Nullable because a
    # seeded placeholder rests on no items at all, which is itself the useful signal.
    n_items: int | None = None
    n_passed: int | None = None
