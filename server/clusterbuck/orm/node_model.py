"""The `node_models` table: what each node's model server actually reports having,
with digests, so an artifact changing upstream is detectable (ADR 15: a new digest is
a NEW artifact)."""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class NodeModel(SQLModel, table=True):
    __tablename__ = "node_models"

    node_id: str = Field(primary_key=True)
    artifact: str = Field(primary_key=True)
    digest: str | None = None
    first_seen: str
    last_seen: str
