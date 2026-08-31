"""The `attention_leases` table (ADR 18 / protocols.md §9): a client's active-user
signal that promotes its waitable backlog; expiry demotes the unstarted promotions."""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class AttentionLease(SQLModel, table=True):
    __tablename__ = "attention_leases"

    # nullable=True on a PK looks wrong but is deliberate — see clusterbuck/orm/job.py's
    # note on the same SQLite quirk.
    client_key: str = Field(primary_key=True, nullable=True)
    scope: str | None = None  # json array of task_class, or null (all)
    expires_at: float
