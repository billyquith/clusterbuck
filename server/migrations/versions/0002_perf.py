"""perf: load-test driver tables (Performance page)

Adds `perf_runs` and `perf_samples`, mirroring `clusterbuck/orm/perf_run.py` and
`clusterbuck/orm/perf_sample.py` — see `tests/test_migrations.py`, which asserts a
database built purely by `alembic upgrade head` matches one Store() bootstraps.

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-31 20:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0002'
down_revision: Union[str, Sequence[str], None] = '0001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "perf_runs",
        # nullable=True on a PK column looks wrong but is deliberate — see
        # 0001_baseline.py's note on the same SQLite quirk.
        sa.Column("id", sa.Text(), nullable=True),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("config", sa.Text(), nullable=False),
        sa.Column("snapshot", sa.Text(), nullable=True),
        sa.Column("started_at", sa.Text(), nullable=False),
        sa.Column("finished_at", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "perf_samples",
        sa.Column("id", sa.Text(), nullable=True),  # see note on perf_runs.id above
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("job_id", sa.Text(), nullable=True),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("task_class", sa.Text(), nullable=False),
        sa.Column("min_ability", sa.Integer(), nullable=False),
        sa.Column("capability", sa.Text(), nullable=True),
        sa.Column("node", sa.Text(), nullable=True),
        sa.Column("phase", sa.Text(), nullable=False),
        sa.Column("submitted_at", sa.REAL(), nullable=False),
        sa.Column("completed_at", sa.REAL(), nullable=True),
        sa.Column("latency_s", sa.REAL(), nullable=True),
        sa.Column("tokens_in", sa.Integer(), nullable=True),
        sa.Column("tokens_out", sa.Integer(), nullable=True),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("passed", sa.Integer(), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("perf_samples")
    op.drop_table("perf_runs")
