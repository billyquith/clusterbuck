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
    node: str | None = None  # worker id, 'cloud:<provider>', or 'sync' (a local sync call)
    venue: str  # local | cloud
    # server_default (not just a Python-side default) to match the original
    # `NOT NULL DEFAULT 0` DDL exactly.
    tokens_in: int = Field(default=0, sa_column_kwargs={"server_default": "0"})
    tokens_out: int = Field(default=0, sa_column_kwargs={"server_default": "0"})
    # Of tokens_in, how many the provider served from its prompt cache — billed at a
    # fraction of the input rate, which is why a flat per-1k price overstates them.
    tokens_cached_in: int = Field(default=0, sa_column_kwargs={"server_default": "0"})
    outcome: str  # done | failed | expired (enum, never error text)
    # local: avoided (tokens×cloud rate); cloud: actual
    cost: float = Field(default=0, sa_column_kwargs={"server_default": "0"})
    # Where `cost` came from: 'litellm' (the provider's published price for the model that
    # answered), 'fleet' (fleet.yaml's price_*_per_1k — always so for local rows, an
    # estimate for cloud ones) or 'none' (nothing priced it, so it is $0). Null on rows
    # written before this column existed.
    cost_source: str | None = None
    day: str  # YYYY-MM-DD, for rollups
