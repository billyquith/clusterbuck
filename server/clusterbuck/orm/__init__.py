"""SQLModel table definitions — the ORM migration, one domain at a time (`jobs` first).

Import a domain's module here once it exists so `SQLModel.metadata` (and therefore
Alembic autogenerate, see `server/migrations/env.py`) knows about its table.
"""

from __future__ import annotations

from .job import Job
from .reservation import Reservation

__all__ = ["Job", "Reservation"]
