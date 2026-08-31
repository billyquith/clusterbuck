"""eval: scope a measurement to a generation, so a re-eval inherits nothing

Adds `eval_runs.generation` and the `eval_generations` table, mirroring
`clusterbuck/orm/eval_run.py` and `clusterbuck/orm/eval_generation.py` — see
`tests/test_migrations.py`, which asserts a database built purely by
`alembic upgrade head` matches one Store() bootstraps.

Why: ability is recomputed from the eval_runs rows for an (artifact, task_class), so
clearing the score alone left the *inputs* to the next score in place and a re-measured
artifact was averaged with its predecessor's items (ADR 15).

Existing rows default to generation 1, which is correct: they were all recorded under the
one round that existed before this column, and `eval_generations` being empty reads as
generation 1 for every artifact.

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-31 22:40:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0003'
down_revision: Union[str, Sequence[str], None] = '0002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "eval_runs",
        sa.Column("generation", sa.Integer(), nullable=False, server_default="1"),
    )

    op.create_table(
        "eval_generations",
        # nullable=True on a PK column looks wrong but is deliberate — see
        # 0001_baseline.py's note on the same SQLite quirk.
        sa.Column("artifact", sa.Text(), nullable=True),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("artifact"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("eval_generations")
    op.drop_column("eval_runs", "generation")
