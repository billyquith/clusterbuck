"""The `join_tokens` table (ADR 9): one-time tokens gating `/nodes/enroll`."""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class JoinToken(SQLModel, table=True):
    __tablename__ = "join_tokens"

    # nullable=True on a PK looks wrong but is deliberate — see clusterbuck/orm/job.py's
    # note on the same SQLite quirk.
    token: str = Field(primary_key=True, nullable=True)
    created_at: str
    used: int = Field(default=0, sa_column_kwargs={"server_default": "0"})
    used_by: str | None = None
