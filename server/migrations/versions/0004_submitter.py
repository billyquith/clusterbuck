"""jobs: caller provenance, so a burst of identical payloads can be attributed

Adds `jobs.submitter_app`, `submitter_instance`, `submitter_request_id`, `submitted_at`
and `observed_ip`, mirroring `clusterbuck/orm/job.py` — see `tests/test_migrations.py`,
which asserts a database built purely by `alembic upgrade head` matches one Store()
bootstraps.

Why: a queue holding N byte-identical jobs cannot say whether it is one logical call
retried N times (a client retry loop) or N genuinely repeated calls.
`submitter_request_id` is that discriminator; `submitter_app`/`submitter_instance` name
the caller; `observed_ip` is stamped server-side and is the only one that identifies a
client which sends no provenance at all.

Every column is nullable with no server_default: existing rows predate provenance, and
NULL is the honest value for them — a default would invent an attribution.

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-02 00:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0004'
down_revision: Union[str, Sequence[str], None] = '0003'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    for column in ("submitter_app", "submitter_instance", "submitter_request_id",
                   "submitted_at", "observed_ip"):
        op.add_column("jobs", sa.Column(column, sa.Text(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    for column in ("observed_ip", "submitted_at", "submitter_request_id",
                   "submitter_instance", "submitter_app"):
        op.drop_column("jobs", column)
