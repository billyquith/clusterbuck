"""baseline: full schema as of the jobs-domain ORM slice

This is a *baseline*, not a real migration: it exists so Alembic has a starting point to
diff future migrations against, and is not yet wired into `Store.__init__` or app
startup (see clusterbuck/store.py and clusterbuck/db.py docstrings) — `Store` still
bootstraps its own schema directly. `upgrade()` reproduces every table currently created
by `Store.__init__` (the `_SCHEMA` DDL string plus every column ever added via
`_MIGRATIONS`, plus `jobs` per `clusterbuck/orm/job.py`), so a database created purely by
`alembic upgrade head` matches one created by `Store()` exactly —
`tests/test_migrations.py` asserts this by building both and comparing.

Before this is ever pointed at a real deployment's database (e.g. via `alembic stamp
head`), diff `PRAGMA table_info` on that live database against what this migration
produces — `_MIGRATIONS` has been patching deployed databases incrementally, so an old
deployment could in principle be missing a column this baseline assumes.

Revision ID: 0001
Revises:
Create Date: 2026-08-31 11:16:41.168739

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0001'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "jobs",
        # nullable=True on a PK column looks wrong but is deliberate: SQLite only
        # enforces NOT NULL on a PK column if it's declared explicitly (or it's the
        # INTEGER PRIMARY KEY rowid alias) — a bare `id TEXT PRIMARY KEY`, which is what
        # this table (and every other single-column-PK table below) has always been,
        # allows NULL. See tests/test_migrations.py's affinity-based comparison.
        sa.Column("id", sa.Text(), nullable=True),
        sa.Column("result_key", sa.Text(), nullable=False),
        sa.Column("capability", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("urgency", sa.Text(), nullable=False, server_default=sa.text("'waitable'")),
        sa.Column("escalate_at", sa.REAL(), nullable=True),
        sa.Column("escalated", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("reservation", sa.Text(), nullable=True),
        sa.Column("deadline_epoch", sa.REAL(), nullable=True),
        sa.Column("client_key", sa.Text(), nullable=True),
        sa.Column("task_class", sa.Text(), nullable=True),
        sa.Column("promoted_by", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "catalog",
        sa.Column("artifact", sa.Text(), nullable=True),  # see note on jobs.id above
        sa.Column("family", sa.Text(), nullable=True),
        sa.Column("params_b", sa.REAL(), nullable=True),
        sa.Column("quant", sa.Text(), nullable=True),
        sa.Column("size_gb", sa.REAL(), nullable=False),
        sa.Column("min_ram_gb", sa.REAL(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("registry_ref", sa.Text(), nullable=False),
        sa.Column("expected_ability", sa.REAL(), nullable=True),
        sa.Column("added_at", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("artifact"),
    )

    op.create_table(
        "node_models",
        sa.Column("node_id", sa.Text(), nullable=False),
        sa.Column("artifact", sa.Text(), nullable=False),
        sa.Column("digest", sa.Text(), nullable=True),
        sa.Column("first_seen", sa.Text(), nullable=False),
        sa.Column("last_seen", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("node_id", "artifact"),
    )

    op.create_table(
        "proposals",
        sa.Column("id", sa.Text(), nullable=True),  # see note on jobs.id above
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("node_id", sa.Text(), nullable=False),
        sa.Column("artifact", sa.Text(), nullable=False),
        sa.Column("incumbent", sa.Text(), nullable=True),
        sa.Column("task_class", sa.Text(), nullable=True),
        sa.Column("rationale", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("decided_at", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "ability",
        sa.Column("artifact", sa.Text(), nullable=False),
        sa.Column("task_class", sa.Text(), nullable=False),
        sa.Column("score", sa.REAL(), nullable=False),
        sa.Column("scale_version", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.Text(), nullable=False),
        sa.Column("provenance", sa.Text(), nullable=False, server_default=sa.text("'measured'")),
        sa.PrimaryKeyConstraint("artifact", "task_class", "scale_version"),
    )

    op.create_table(
        "eval_runs",
        sa.Column("job_id", sa.Text(), nullable=True),  # see note on jobs.id above
        sa.Column("artifact", sa.Text(), nullable=False),
        sa.Column("task_class", sa.Text(), nullable=False),
        sa.Column("item_index", sa.Integer(), nullable=False),
        sa.Column("result_key", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("passed", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("job_id"),
    )

    op.create_table(
        "attention_leases",
        sa.Column("client_key", sa.Text(), nullable=True),  # see note on jobs.id above
        sa.Column("scope", sa.Text(), nullable=True),
        sa.Column("expires_at", sa.REAL(), nullable=False),
        sa.PrimaryKeyConstraint("client_key"),
    )

    op.create_table(
        "join_tokens",
        sa.Column("token", sa.Text(), nullable=True),  # see note on jobs.id above
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.Column("used", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("used_by", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("token"),
    )

    op.create_table(
        "nodes",
        sa.Column("node_id", sa.Text(), nullable=True),  # see note on jobs.id above
        sa.Column("node_key", sa.Text(), nullable=False),
        sa.Column("hostname", sa.Text(), nullable=True),
        sa.Column("os", sa.Text(), nullable=True),
        sa.Column("arch", sa.Text(), nullable=True),
        sa.Column("ram_gb", sa.REAL(), nullable=True),
        sa.Column("accelerator", sa.Text(), nullable=True),
        sa.Column("vram_gb", sa.REAL(), nullable=True),
        sa.Column("disk_free_gb", sa.REAL(), nullable=True),
        sa.Column("bench_tps_small", sa.REAL(), nullable=True),
        sa.Column("profile", sa.Text(), nullable=True),
        sa.Column("capabilities", sa.Text(), nullable=True),
        sa.Column("disk_quota_gb", sa.REAL(), nullable=True),
        sa.Column("auto_approve", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("auto_update", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("mode", sa.Text(), nullable=True),
        sa.Column("installed", sa.Text(), nullable=True),
        sa.Column("loaded", sa.Text(), nullable=True),
        sa.Column("queues", sa.Text(), nullable=True),
        sa.Column("jobs_done", sa.Integer(), nullable=True, server_default=sa.text("0")),
        sa.Column("tps", sa.REAL(), nullable=True),
        sa.Column("last_heartbeat", sa.Text(), nullable=True),
        sa.Column("agent_version", sa.Text(), nullable=True),
        sa.Column("agent_flavour", sa.Text(), nullable=True),
        sa.Column("protocol_version", sa.Integer(), nullable=True),
        sa.Column("fitness", sa.Text(), nullable=True),
        sa.Column("fitness_reason", sa.Text(), nullable=True),
        sa.Column("enrolled_at", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("node_id"),
    )

    op.create_table(
        "usage",
        sa.Column("job_id", sa.Text(), nullable=True),  # see note on jobs.id above
        sa.Column("ts", sa.Text(), nullable=False),
        sa.Column("capability", sa.Text(), nullable=True),
        sa.Column("model", sa.Text(), nullable=True),
        sa.Column("node", sa.Text(), nullable=True),
        sa.Column("venue", sa.Text(), nullable=False),
        sa.Column("tokens_in", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("tokens_out", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("cost", sa.REAL(), nullable=False, server_default=sa.text("0")),
        sa.Column("day", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("job_id"),
    )

    op.create_table(
        "reservations",
        sa.Column("id", sa.Text(), nullable=True),  # see note on jobs.id above
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=True),
        sa.Column("task_class", sa.Text(), nullable=True),
        sa.Column("min_ability", sa.Integer(), nullable=True),
        sa.Column("capability", sa.Text(), nullable=True),
        sa.Column("node", sa.Text(), nullable=True),
        sa.Column("artifact", sa.Text(), nullable=True),
        sa.Column("priority", sa.Text(), nullable=True),
        sa.Column("privacy", sa.Text(), nullable=True),
        sa.Column("load", sa.Text(), nullable=True),
        sa.Column("duration_min", sa.Integer(), nullable=True),
        sa.Column("est_jobs", sa.Integer(), nullable=True),
        sa.Column("warm_by", sa.REAL(), nullable=True),
        sa.Column("starts", sa.REAL(), nullable=True),
        sa.Column("ends", sa.REAL(), nullable=True),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("reservations")
    op.drop_table("usage")
    op.drop_table("nodes")
    op.drop_table("join_tokens")
    op.drop_table("attention_leases")
    op.drop_table("eval_runs")
    op.drop_table("ability")
    op.drop_table("proposals")
    op.drop_table("node_models")
    op.drop_table("catalog")
    op.drop_table("jobs")
