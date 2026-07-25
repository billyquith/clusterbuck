"""Pydantic models.

`JobRecord` and `Result` mirror the JSON Schema in `contract/` (the cross-language
seam); the conformance test asserts they stay in agreement. `JobSubmit` is the client
request shape (protocols.md §1b) and is server-only, so it is not part of `contract/`.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class Urgency(str, Enum):
    urgent = "urgent"
    necessary = "necessary"
    waitable = "waitable"


class Privacy(str, Enum):
    local_only = "local_only"
    cloud_ok = "cloud_ok"


class Message(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str


class JobSubmit(BaseModel):
    """Client request body for POST /jobs (protocols.md §1b).

    Addressing is one of two forms: a need (`task_class` + `min_ability`, preferred) or
    an explicit supply-side `capability` (advanced). The server resolves either to a
    concrete capability before enqueue.
    """

    model_config = {"extra": "forbid"}

    task_class: str | None = None
    min_ability: int | None = Field(default=None, ge=1, le=10)
    capability: str | None = None

    messages: list[Message] | None = None
    prompt: str | None = None

    params: dict[str, Any] = Field(default_factory=dict)
    urgency: Urgency = Urgency.waitable
    escalate_after_min: int | None = Field(default=None, ge=0)
    privacy: Privacy = Privacy.local_only
    deadline: str | None = None
    callback_url: str | None = None

    @model_validator(mode="after")
    def _check(self) -> "JobSubmit":
        if self.capability is None and (
            self.task_class is None or self.min_ability is None
        ):
            raise ValueError(
                "provide either `capability`, or both `task_class` and `min_ability`"
            )
        if self.messages is None and self.prompt is None:
            raise ValueError("provide either `messages` or `prompt`")
        return self


class JobRecord(BaseModel):
    """The enqueued job record — mirrors contract/job.schema.json."""

    model_config = {"extra": "forbid"}

    id: str
    created_at: str
    capability: str
    messages: list[Message] | None = None
    prompt: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    urgency: Urgency
    escalate_after_min: int | None = None
    privacy: Privacy
    deadline: str | None = None
    result_key: str
    attempts: int = 0
    max_attempts: int = 3

    def to_wire(self) -> dict[str, Any]:
        """Contract-shaped dict: drop null optionals so it validates cleanly."""
        return self.model_dump(mode="json", exclude_none=True)
