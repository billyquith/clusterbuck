"""SQLModel table definitions — the ORM migration, one domain at a time (`jobs` first).

Import a domain's module here once it exists so `SQLModel.metadata` (and therefore
Alembic autogenerate, see `server/migrations/env.py`) knows about its table.
"""

from __future__ import annotations

from .ability import Ability
from .attention_lease import AttentionLease
from .catalog_entry import CatalogEntry
from .eval_run import EvalRun
from .job import Job
from .join_token import JoinToken
from .node import Node
from .node_model import NodeModel
from .proposal import Proposal
from .reservation import Reservation

__all__ = [
    "Ability", "AttentionLease", "CatalogEntry", "EvalRun", "Job", "JoinToken",
    "Node", "NodeModel", "Proposal", "Reservation",
]
