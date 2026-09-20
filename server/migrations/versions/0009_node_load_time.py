"""nodes: measured cold-load seconds; drop the benchmark that never ran

Adds `nodes.load_s` and removes `nodes.bench_tps_small`, mirroring
`clusterbuck/orm/node.py` — see `tests/test_migrations.py`, which asserts a database
built purely by `alembic upgrade head` matches one Store() bootstraps.

Why `load_s`: reservations and wake pre-load a model ahead of a window using
`DEFAULT_WARM_LEAD_S`, a hardcoded five minutes for every node and every model. That
constant stands in for the one number nobody measured — how long this machine actually
takes to bring a model up, which is storage speed times model size and varies from a
few seconds on NVMe to minutes on a slow disk. Over-estimating wastes a node's time
awake; under-estimating means the window opens onto a model still loading, which is
exactly what pre-warming exists to prevent.

It is measured, not probed. A cold job's wall time is load plus generation, so the load
share is recovered by subtracting the generation the node's own measured throughput
accounts for. Sampled only when the inventory gives POSITIVE evidence the model was
cold; a model server whose adapter cannot report what is resident yields no samples
rather than wrong ones, and those nodes keep the constant.

Why `bench_tps_small` goes: it was declared in the enrollment contract, carried through
the worker's probe model, the server's model and this column — and never computed at one
end or read at the other, through a whole milestone. `stats.tps` now does the job it was
reaching for, measured from real work rather than a synthetic benchmark on an idle
machine. Removing it from the contract means a *fresh* enrollment from a pre-0.9.0
artifact is rejected; that happens while an operator is installing a worker and can see
the error, whereas an already-enrolled node never re-sends `hw` at all.

Revision ID: 0009
Revises: 0008
Create Date: 2026-09-20 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0009'
down_revision: Union[str, Sequence[str], None] = '0008'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("nodes", sa.Column("load_s", sa.REAL(), nullable=True))
    op.drop_column("nodes", "bench_tps_small")


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column("nodes", sa.Column("bench_tps_small", sa.REAL(), nullable=True))
    op.drop_column("nodes", "load_s")
