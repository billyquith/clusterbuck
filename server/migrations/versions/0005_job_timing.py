"""jobs: lifecycle timing and delivery, so a polling client can see its own wait

Adds `jobs.started_at`, `finished_at`, `claimed_by`, `entry_id` and `stream`, mirroring
`clusterbuck/orm/job.py` — see `tests/test_migrations.py`, which asserts a database built
purely by `alembic upgrade head` matches one Store() bootstraps.

Why: `GET /jobs/{id}` could report no timestamps at all, so a client had to time jobs
from its own submit (lost on a process restart) and could not distinguish "queued" from
"running" — `worker` is only populated from the result blob, i.e. once the job is already
over. `started_at`/`claimed_by` come from the stream's pending-entries list, which knows
the claiming consumer before any result exists; `finished_at` is stamped wherever the
coordinator first writes a terminal status.

`entry_id`/`stream` record the delivery the coordinator granted. `queue.enqueue` always
returned the entry id and the caller discarded it; keeping it is what makes queue
position, withdrawal and (later) urgency tiering exact rather than a stream scan. `stream`
is stored rather than derived because escalation changes a job's urgency *after* enqueue,
so the entry can sit on a stream that no longer matches its current urgency.

Every column is nullable with no server_default: existing rows predate the observation,
and NULL is the honest value — a default would invent a timestamp.

Revision ID: 0005
Revises: 0004
Create Date: 2026-09-02 01:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0005'
down_revision: Union[str, Sequence[str], None] = '0004'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    for column in ("started_at", "finished_at", "claimed_by", "entry_id", "stream"):
        op.add_column("jobs", sa.Column(column, sa.Text(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    for column in ("stream", "entry_id", "claimed_by", "finished_at", "started_at"):
        op.drop_column("jobs", column)
