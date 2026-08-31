"""SQLModel table definitions — the ORM migration, one domain at a time (`jobs` first).

Import a domain's module here once it exists so `SQLModel.metadata` (and therefore
Alembic autogenerate, see `server/migrations/env.py`) knows about its table.
"""

from __future__ import annotations

from .attention_lease import AttentionLease
from .job import Job
from .join_token import JoinToken
from .node import Node
from .reservation import Reservation

__all__ = ["AttentionLease", "Job", "JoinToken", "Node", "Reservation"]
