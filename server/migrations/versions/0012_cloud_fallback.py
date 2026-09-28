"""cloud fallback: rescue bookkeeping on jobs, honest cost provenance on usage

Mirrors `clusterbuck/orm/job.py` and `clusterbuck/orm/usage.py` - see
`tests/test_migrations.py`, which asserts a database built purely by
`alembic upgrade head` matches one Store() bootstraps.

jobs gains what the rescue sweep (`rescue.py`) needs to move an unserved `cloud_ok` job
to the cloud without re-reading its payload or re-resolving it against ability data that
may have moved since: `privacy`, the `cloud_alternate` capability and artifact chosen at
submit, a committed-but-unreported `est_cost` for the budget, and `rescued_from` - the tier a
rescued job left, so a rescue interrupted mid-move can be found and undone.

usage gains `tokens_cached_in` and `cost_source`. Cloud cost now comes from LiteLLM's
price map for the model that actually answered, cache pricing included; the fleet's flat
rate is the fallback, and a row has to say which one it is, or an estimate reads like a
quote.

All nullable or defaulted: existing rows keep their meaning, and a job row with no
`privacy` is treated as `local_only`, which is what it was submitted as by default.

Revision ID: 0012
Revises: 0011
Create Date: 2026-09-28 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0012'
down_revision: Union[str, Sequence[str], None] = '0011'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("jobs", sa.Column("privacy", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("cloud_alternate", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("cloud_alternate_model", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("est_cost", sa.Float(), nullable=True))
    op.add_column("jobs", sa.Column("rescued_from", sa.Text(), nullable=True))
    op.add_column("usage", sa.Column("tokens_cached_in", sa.Integer(), nullable=False,
                                     server_default="0"))
    op.add_column("usage", sa.Column("cost_source", sa.Text(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("usage") as b:
        b.drop_column("cost_source")
        b.drop_column("tokens_cached_in")
    with op.batch_alter_table("jobs") as b:
        b.drop_column("rescued_from")
        b.drop_column("est_cost")
        b.drop_column("cloud_alternate_model")
        b.drop_column("cloud_alternate")
        b.drop_column("privacy")
