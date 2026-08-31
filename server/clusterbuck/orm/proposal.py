"""The `proposals` table (fleet-management.md): SUGGESTIONS, never silent changes.
Multi-GB weights are never fetched without a human decision (per-node `auto_approve`
is opt-in). Kinds: `upgrade` (fits + beats incumbent), `reeval` (digest changed
upstream — same name, new artifact), `reclaim` (unused install, disk back to owner).
"""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class Proposal(SQLModel, table=True):
    __tablename__ = "proposals"

    # nullable=True on a PK looks wrong but is deliberate — see clusterbuck/orm/job.py's
    # note on the same SQLite quirk.
    id: str = Field(primary_key=True, nullable=True)
    kind: str  # upgrade | reeval | reclaim
    node_id: str
    artifact: str
    incumbent: str | None = None
    task_class: str | None = None
    rationale: str
    status: str  # pending | approved | denied | applied | failed
    created_at: str
    decided_at: str | None = None
