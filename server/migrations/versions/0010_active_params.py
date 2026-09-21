"""catalog: parameters ACTIVE per token, so the fits gate stops libelling MoE models

Adds `catalog.active_params_b`, mirroring `clusterbuck/orm/catalog_entry.py` - see
`tests/test_migrations.py`, which asserts a database built purely by
`alembic upgrade head` matches one Store() bootstraps.

`fits()` (ADR 37) compared a candidate's total size against the node's VRAM and called
anything that overflowed "degraded - it will run from system RAM and be far slower".
That is true for a DENSE model, where every parameter is read for every token. It is not
true for a mixture-of-experts, which reads only its active parameters, so most of what
sits in system RAM is never touched on a given token.

The difference was measured rather than reasoned about. A 30B-A3B (~3B active) held 54%
on a 12 GB card ran at 71 tok/s; the same artifact fully resident in 48 GB on another
node managed 52. The partially-resident node was the FASTER one, and the gate was telling
the operator to avoid the best configuration in the fleet.

Nullable, and absence means dense. An artifact that merely omits the figure must not be
excused from the gate - that would let a genuinely oversized dense model through on a
technicality, which is the failure the gate exists to catch.

Revision ID: 0010
Revises: 0009
Create Date: 2026-09-21 15:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0010'
down_revision: Union[str, Sequence[str], None] = '0009'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("catalog", sa.Column("active_params_b", sa.Float(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("catalog", "active_params_b")
