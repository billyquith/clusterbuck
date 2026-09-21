"""The `nodes` table (ADR 9): the dynamic fleet registry.

A node self-enrolls (hardware probe → profile + proposed capabilities) and then
heartbeats; `fleet.yaml` is only the seed. `installed`/`loaded`/`queues`/`capabilities`
are JSON-encoded arrays (SQLite has no array type) — callers `json.loads`/`json.dumps`
at the boundary, same as before this migration.
"""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class Node(SQLModel, table=True):
    __tablename__ = "nodes"

    # nullable=True on a PK looks wrong but is deliberate — see clusterbuck/orm/job.py's
    # note on the same SQLite quirk (a bare `node_id TEXT PRIMARY KEY` isn't NOT
    # NULL-enforced, unlike SQLAlchemy's own default of nullable=False for primary_key=True).
    node_id: str = Field(primary_key=True, nullable=True)
    node_key: str
    hostname: str | None = None
    os: str | None = None
    arch: str | None = None
    ram_gb: float | None = None
    accelerator: str | None = None
    vram_gb: float | None = None
    # Measured seconds to bring this node's model up from cold (heartbeat `stats.load_s`).
    # Storage speed times model size, which no probe of either alone can predict, and the
    # number `DEFAULT_WARM_LEAD_S` was standing in for. None where the model server cannot
    # report what is resident, so coldness is unprovable.
    load_s: float | None = None
    disk_free_gb: float | None = None
    profile: str | None = None
    capabilities: str | None = None  # json array
    disk_quota_gb: float | None = None  # owner's contract for model storage (profile-derived)
    # server_default (not just a Python-side default) to match the original
    # `NOT NULL DEFAULT 0` DDL exactly.
    auto_approve: int = Field(default=0, sa_column_kwargs={"server_default": "0"})
    auto_update: int = Field(default=0, sa_column_kwargs={"server_default": "0"})
    mode: str | None = None
    installed: str | None = None  # json array
    loaded: str | None = None  # json array
    queues: str | None = None  # json array
    # Nullable with a default (unlike auto_approve/auto_update, which are NOT NULL) —
    # the original DDL is `INTEGER DEFAULT 0`, no NOT NULL.
    jobs_done: int | None = Field(default=0, sa_column_kwargs={"server_default": "0"})
    tps: float | None = None
    last_heartbeat: str | None = None
    agent_version: str | None = None  # the worker's build-stamped release version
    # which artifact this node can execute; 'python' is the only implementation
    agent_flavour: str | None = None
    protocol_version: int | None = None  # queue-contract version it speaks
    fitness: str | None = None  # ok | stale | quarantine (coordinator's last verdict)
    fitness_reason: str | None = None
    enrolled_at: str
