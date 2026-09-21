"""drop attention_leases: the client-attention lease never had a client

Removes the `attention_leases` table and, with it, the last trace of the client
attention feature (former ADR 18 / protocols.md §9). The `POST /attention` route,
`attention.py`, its coordinator tick and `AttentionLease` all go in the same change -
see `tests/test_migrations.py`, which asserts a database built purely by
`alembic upgrade head` matches one Store() bootstraps.

The feature let a client announce that its user had become active, promoting that
client's waitable backlog to necessary for a TTL'd lease. It was reachable only from
its own unit test: no e2e script, no worker path, no dashboard control and no client
ever posted to it. On the live coordinator the table held **0 rows** at every
inspection, against 661 jobs and 575 of them carrying a `client_key`.

Two columns it touched deliberately survive, because they have other owners:

  * `jobs.client_key` - an opaque client label. The eval harness tags every job it
    dispatches with `cbk:eval` (`eval_runner.py`), and it stays accepted on `POST /jobs`
    because `JobSubmit` forbids unknown fields, so withdrawing it would turn a client
    still sending one into a 422.
  * `jobs.promoted_by` - escalation provenance. Attention wrote `'attention'` here, but
    `Store.mark_escalated` writes `'age'`, which is the only value present in live data.
    The column belongs to escalation and keeps its remaining half.

Downgrade recreates the table empty. It cannot restore rows, which is harmless: there
have never been any.

Revision ID: 0011
Revises: 0010
Create Date: 2026-09-21 16:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '0011'
down_revision: Union[str, Sequence[str], None] = '0010'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table("attention_leases")


def downgrade() -> None:
    op.create_table(
        "attention_leases",
        sa.Column("client_key", sa.Text(), nullable=True),  # as 0001_baseline declared it
        sa.Column("scope", sa.Text(), nullable=True),
        sa.Column("expires_at", sa.REAL(), nullable=False),
        sa.PrimaryKeyConstraint("client_key"),
    )
