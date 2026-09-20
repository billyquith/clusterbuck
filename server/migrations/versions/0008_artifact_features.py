"""catalog: what an artifact CAN DO, not just how good it is

Adds `catalog.context_tokens`, `supports_tools`, `supports_json_schema` and
`supports_vision`, mirroring `clusterbuck/orm/catalog_entry.py` — see
`tests/test_migrations.py`, which asserts a database built purely by
`alembic upgrade head` matches one Store() bootstraps.

Ability (ADR 15) answers "how good is this model at a task class". It cannot answer
"can this model do the thing at all", and those are different questions with different
shapes: ability is a graded 1-10 scale, while context window is a number with a hard
edge and tool calling is a boolean. A 4k-context model and a 128k one can both be a
6 at summarize, and routing a long document to the first silently truncates it. A
client that needs tool calling could not ask for it at all, so it either named a
capability explicitly — abandoning need-shaped addressing, which was the point — or
submitted and hoped.

All four are nullable, and null means NOT CURATED rather than false. The distinction
is load-bearing at the routing gate: an artifact that does not declare a feature is
excluded from jobs requiring it, and the refusal says so, rather than the fleet
quietly serving a job on a model that may or may not be able to do it.

Revision ID: 0008
Revises: 0007
Create Date: 2026-09-20 13:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0008'
down_revision: Union[str, Sequence[str], None] = '0007'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("catalog", sa.Column("context_tokens", sa.Integer(), nullable=True))
    op.add_column("catalog", sa.Column("supports_tools", sa.Boolean(), nullable=True))
    op.add_column("catalog", sa.Column("supports_json_schema", sa.Boolean(), nullable=True))
    op.add_column("catalog", sa.Column("supports_vision", sa.Boolean(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("catalog", "supports_vision")
    op.drop_column("catalog", "supports_json_schema")
    op.drop_column("catalog", "supports_tools")
    op.drop_column("catalog", "context_tokens")
