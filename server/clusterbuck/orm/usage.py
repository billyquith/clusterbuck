"""The `usage` table (fleet-management.md → Usage accounting; ADR 11): METADATA ONLY.
No prompt or completion text ever lands here; `outcome` is a status enum, not the error
string (errors can echo input). One row per job (PRIMARY KEY) ⇒ capture is idempotent.
"""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class Usage(SQLModel, table=True):
    __tablename__ = "usage"

    # nullable=True on a PK looks wrong but is deliberate — see clusterbuck/orm/job.py's
    # note on the same SQLite quirk.
    job_id: str = Field(primary_key=True, nullable=True)
    ts: str
    capability: str | None = None
    model: str | None = None
    node: str | None = None  # worker id, or 'cloud:<provider>' (future)
    venue: str  # local | cloud
    # server_default (not just a Python-side default) to match the original
    # `NOT NULL DEFAULT 0` DDL exactly.
    tokens_in: int = Field(default=0, sa_column_kwargs={"server_default": "0"})
    tokens_out: int = Field(default=0, sa_column_kwargs={"server_default": "0"})
    outcome: str  # done | failed | expired (enum, never error text)
    # local: avoided (tokens×cloud rate); cloud: actual
    cost: float = Field(default=0, sa_column_kwargs={"server_default": "0"})
    day: str  # YYYY-MM-DD, for rollups
