"""The `catalog` table (fleet-management.md → Model catalog): curated known-good
artifacts with the metadata the "fits" gate needs. `expected_ability` is an
admin-curated hint used only to RANK proposals — the real gate is measured ability
after install (ADR 15).

It also holds each artifact's **capabilities** — context window, tool calling, schema-
constrained output, vision. Those are not quality questions with a good/bad axis, so no
1-10 score can ever express them: a 4k-context model and a 128k one can both be "a 6 at
summarize", and sending a long document to the first silently truncates it. They are hard
yes/no facts about what a model CAN DO, and routing filters on them before it compares
ability at all.
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
    # Parameters ACTIVE per token. Equal to params_b for a dense model; far smaller for a
    # mixture-of-experts, where only a few experts fire per token. The distinction is what
    # decides whether partial VRAM residency actually costs anything (ADR 39).
    active_params_b: float | None = None
    quant: str | None = None
    size_gb: float
    min_ram_gb: float
    source: str  # model-manager that can install it (e.g. ollama)
    registry_ref: str  # what to ask the model manager to pull
    expected_ability: float | None = None
    # --- capabilities (ADR 37). None = not curated, which is NOT the same as false: a
    # caller requiring a feature is told the artifact does not declare it, rather than
    # being quietly handed a model that may or may not do the job.
    context_tokens: int | None = None
    supports_tools: bool | None = None
    supports_json_schema: bool | None = None
    supports_vision: bool | None = None
    added_at: str
