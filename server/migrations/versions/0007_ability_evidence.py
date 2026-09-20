"""ability: record how much evidence a score rests on; eval_runs: which suite it measured

Adds `ability.n_items` / `ability.n_passed` and `eval_runs.suite_version`, mirroring
`clusterbuck/orm/ability.py` and `clusterbuck/orm/eval_run.py` — see
`tests/test_migrations.py`, which asserts a database built purely by
`alembic upgrade head` matches one Store() bootstraps.

Why the counts: model-evaluation.md requires scores to be reported at half-point
granularity "with an uncertainty note". The table held a bare number, so a 10 derived
from a single one-word item and a 7 derived from forty were indistinguishable to every
reader — the router, the dashboard, and anyone deciding whether to trust a route. They
are nullable because a seeded placeholder rests on no items at all, and recording that
as 0 would claim a measurement of zero rather than the absence of one.

Why the suite version: item identity within a suite is POSITIONAL (`item_index`). A run
dispatched against one suite and collected after the suite changed would be scored
against whatever now occupies that index — measuring one item and recording the result
as another. The existing `generation` column scopes a measurement to an ARTIFACT round,
which does not help here: the artifact is unchanged, the ruler moved. model-evaluation.md
names this fix directly ("suites are versioned alongside the scale").

Existing rows keep NULL counts and an empty suite version. Neither is a problem in
practice, because this ships alongside a SCALE_VERSION bump: routing reads only the
current scale version, so prior scores are not consulted and prior runs are not
collected. The defaults exist so an old database still loads.

Revision ID: 0007
Revises: 0006
Create Date: 2026-09-20 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0007'
down_revision: Union[str, Sequence[str], None] = '0006'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("ability", sa.Column("n_items", sa.Integer(), nullable=True))
    op.add_column("ability", sa.Column("n_passed", sa.Integer(), nullable=True))
    op.add_column(
        "eval_runs",
        sa.Column("suite_version", sa.Text(), nullable=False, server_default="''"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("eval_runs", "suite_version")
    op.drop_column("ability", "n_passed")
    op.drop_column("ability", "n_items")
