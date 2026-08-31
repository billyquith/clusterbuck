"""The `reservations` table (protocols.md §8; ADR 17): workload reservations.

Two independent axes stored side by side (see reservations.py's module docstring):
admission `status` (confirmed | declined, set once at creation) and lifecycle `state`
(scheduled → warming → open → draining → closed, or cancelled — advanced by the
reconciler tick, never per-reservation timers). `state` is null only for a declined
reservation, which has no lifecycle to advance.
"""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class Reservation(SQLModel, table=True):
    __tablename__ = "reservations"

    # nullable=True on a PK looks wrong but is deliberate — see clusterbuck/orm/job.py's
    # note on the same SQLite quirk (a bare `id TEXT PRIMARY KEY` isn't NOT NULL-enforced,
    # unlike SQLAlchemy's own default of nullable=False for primary_key=True).
    id: str = Field(primary_key=True, nullable=True)
    status: str  # confirmed | declined
    state: str | None = None  # scheduled|warming|open|draining|closed|cancelled
    task_class: str | None = None
    min_ability: int | None = None
    capability: str | None = None
    node: str | None = None
    artifact: str | None = None
    priority: str | None = None
    privacy: str | None = None
    load: str | None = None
    duration_min: int | None = None
    est_jobs: int | None = None
    warm_by: float | None = None
    starts: float | None = None
    ends: float | None = None
    created_at: str
