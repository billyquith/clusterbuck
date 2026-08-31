"""The `catalog` table (fleet-management.md → Model catalog): curated known-good
artifacts with the metadata the "fits" gate needs. `expected_ability` is an
admin-curated hint used only to RANK proposals — the real gate is measured ability
after install (ADR 15).
"""

from __future__ import annotations

from sqlmodel import Field, SQLModel


class CatalogEntry(SQLModel, table=True):
    __tablename__ = "catalog"

    # nullable=True on a PK looks wrong but is deliberate — see clusterbuck/orm/job.py's
    # note on the same SQLite quirk (a bare `artifact TEXT PRIMARY KEY` isn't NOT
    # NULL-enforced, unlike SQLAlchemy's own default of nullable=False for primary_key=True).
    artifact: str = Field(primary_key=True, nullable=True)
    family: str | None = None
    params_b: float | None = None
    quant: str | None = None
    size_gb: float
    min_ram_gb: float
    source: str  # model-manager that can install it (e.g. ollama)
    registry_ref: str  # what to ask the model manager to pull
    expected_ability: float | None = None
    added_at: str
