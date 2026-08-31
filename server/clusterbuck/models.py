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
    reservation: str | None = None  # opt-in reservation id to queue against (§8)
    client_key: str | None = None  # optional client identity (attention scoping, §9)

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


class Window(BaseModel):
    model_config = {"extra": "forbid"}

    start: str = "asap"  # "asap" or "HH:MM" (local)
    recur: str | None = None  # M2b: recurrence deferred — must be null


class ReservationSubmit(BaseModel):
    """Client request body for POST /reservations (protocols.md §8).

    Server-only seam (client ↔ HTTP), so — like JobSubmit — it is validated by Pydantic
    and is not part of contract/.
    """

    model_config = {"extra": "forbid"}

    task_class: str
    min_ability: int = Field(ge=1, le=10)
    load: Literal["light", "medium", "heavy"] = "light"
    duration_min: int = Field(default=30, ge=1)
    est_jobs: int | None = Field(default=None, ge=0)
    priority: Literal["low", "medium", "high"] = "medium"
    privacy: Privacy = Privacy.local_only
    window: Window = Field(default_factory=Window)

    @model_validator(mode="after")
    def _check(self) -> "ReservationSubmit":
        if self.window.recur is not None:
            raise ValueError("recurring reservations are not supported yet (M2b)")
        if self.window.start != "asap":
            hh, _, mm = self.window.start.partition(":")
            if not (hh.isdigit() and mm.isdigit() and 0 <= int(hh) < 24 and 0 <= int(mm) < 60):
                raise ValueError("window.start must be 'asap' or 'HH:MM'")
        return self


class PerfRunSubmit(BaseModel):
    """Client request body for POST /perf/runs (the Performance page's load-test driver).

    Server-only seam — like JobSubmit — so it is validated by Pydantic and is not part of
    contract/.
    """

    model_config = {"extra": "forbid"}

    label: str = "load test"
    categories: list[str] | None = None  # null ⇒ all shipped categories
    concurrency: int = Field(default=4, ge=1, le=64)
    duration_s: float = Field(default=60.0, gt=0, le=3600)
    warmup_s: float = Field(default=15.0, ge=0)
    n_jobs: int | None = Field(default=None, ge=1)
    min_ability_override: int | None = Field(default=None, ge=1, le=10)
    pin_model: str | None = None


class AttentionRequest(BaseModel):
    """Client attention signal (protocols.md §9). Server-only ⇒ not in contract/."""

    model_config = {"extra": "forbid"}

    client_key: str
    state: Literal["active", "idle"] = "active"
    scope: list[str] | None = None  # task-class filter; null = all
    ttl_s: int = Field(default=600, ge=1)


class NodePolicy(BaseModel):
    """Owner's per-node contract (POST /nodes/{id}/policy). A body, not query params —
    bare scalars bound as query parameters, which made the approval gate trivially flippable."""

    model_config = {"extra": "forbid"}

    disk_quota_gb: float | None = Field(default=None, ge=0)
    auto_approve: bool | None = None
    # Opt-in: apply a signed worker update without asking. An update channel is RCE by
    # design (ADR 13), so this is off by default and per-node, like auto_approve.
    auto_update: bool | None = None


class HwProbe(BaseModel):
    """Hardware probe (contract/enroll-request.schema.json → hw)."""

    model_config = {"extra": "forbid"}

    ram_gb: float = Field(ge=0)
    accelerator: Literal["metal", "cuda", "cpu"]
    vram_gb: float | None = Field(default=None, ge=0)
    disk_free_gb: float = Field(ge=0)
    bench_tps_small: float | None = Field(default=None, ge=0)


class EnrollRequest(BaseModel):
    """Worker enrollment request — mirrors contract/enroll-request.schema.json."""

    model_config = {"extra": "forbid"}

    join_token: str
    hostname: str
    os: str
    arch: str
    hw: HwProbe
    profile: Literal["dedicated", "shared", "background"]


class ActionResult(BaseModel):
    """Outcome of a model-management action (contract/heartbeat-request → action_result)."""

    model_config = {"extra": "forbid"}

    proposal_id: str
    ok: bool
    error: str | None = None


class HeartbeatRequest(BaseModel):
    """Worker heartbeat — mirrors contract/heartbeat-request.schema.json."""

    model_config = {"extra": "forbid"}

    mode: Literal["active", "away", "paused"]
    installed: list[str] = Field(default_factory=list)
    loaded: list[str] = Field(default_factory=list)
    # artifact → content digest, where the model server exposes it (drives re-eval, ADR 15).
    digests: dict[str, str] | None = None
    # The worker's build-stamped release version. The coordinator judges FITNESS from this,
    # not just protocol compatibility (ADR 27) — optional so a worker predating the field
    # still heartbeats rather than 422-ing, and is assessed as unverifiable instead.
    agent_version: str | None = None
    # Which runtime this node runs, and so which release artifact it can EXECUTE
    # ('python' → py3-none-any artifact; 'dotnet' → legacy .NET RID). Absent ⇒ dotnet.
    # An unrecognised value is offered no update rather than the wrong one (fails closed).
    agent_flavour: str | None = None
    queues: list[str] = Field(default_factory=list)
    stats: dict[str, Any] = Field(default_factory=dict)
    protocol_version: int | None = None
    action_result: ActionResult | None = None


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
