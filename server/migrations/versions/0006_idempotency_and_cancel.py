"""jobs: an idempotency key that is actually binding, and a cancellation marker

Adds `jobs.idempotency_key` with the repo's first UNIQUE index, and
`jobs.cancel_requested`, mirroring `clusterbuck/orm/job.py` — see
`tests/test_migrations.py`, which asserts a database built purely by
`alembic upgrade head` matches one Store() bootstraps.

Why the key: `POST /jobs` minted a new job on every call, so a client that lost the
response to a submit (a killed request, a dropped connection) could not tell "submitted"
from "not submitted" and had no safe way to retry — it risked either a duplicate job or
losing the work. The unique index is what makes a retry safe; a repeat returns the
existing job rather than creating a second one.

This is deliberately NOT `submitter_request_id` (0004), which is documented in five
places as identification only and never enforced — a client reusing that one is
*describing a retry*, which is a signal we want to keep, not a constraint to enforce.
Enforcement gets its own opt-in field.

A plain unique index rather than a partial one: SQLite treats NULLs as distinct, so every
existing row and every submit without a key coexists freely.

Why `cancel_requested`: since Redis 7, `XAUTOCLAIM` drops pending entries whose stream
entry no longer exists. Withdrawing an entry a worker already holds and then losing that
worker would therefore leave the job with no result blob, no terminal status, and nothing
able to recover it. The flag, paired with a floored `deadline_epoch`, lets the existing
expiry sweep terminalise it.

Revision ID: 0006
Revises: 0005
Create Date: 2026-09-02 02:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0006'
down_revision: Union[str, Sequence[str], None] = '0005'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("jobs", sa.Column("idempotency_key", sa.Text(), nullable=True))
    op.add_column(
        "jobs",
        sa.Column("cancel_requested", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index(
        "ix_jobs_idempotency_key", "jobs", ["idempotency_key"], unique=True
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_jobs_idempotency_key", table_name="jobs")
    op.drop_column("jobs", "cancel_requested")
    op.drop_column("jobs", "idempotency_key")
