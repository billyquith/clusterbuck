"""Pydantic models.

`JobRecord` and `Result` mirror the JSON Schema in `contract/` (the cross-language
seam); the conformance test asserts they stay in agreement. `JobSubmit` is the client
request shape (protocols.md §1b) and is server-only, so it is not part of `contract/`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

# The statuses a WORKER can write into a result blob (contract/result.schema.json).
# Lives here rather than in api.py because `Store.set_status` needs it too, and importing
# it from the API layer would invert the dependency. A coordinator-only lifecycle status
# (e.g. a cancellation) does NOT belong in this set — that would imply a worker could
# produce one.
RESULT_STATUSES = frozenset({"done", "failed", "expired"})

# Every status a job's LIFECYCLE can end in. Strictly a superset of RESULT_STATUSES:
# `cancelled` is coordinator-side, because no worker can produce it — a worker writes a
# result blob, and the blob's status enum is RESULT_STATUSES. Keeping the two named apart
# is what stops "terminal" meaning two different things in two places.
TERMINAL_STATUSES = RESULT_STATUSES | {"cancelled"}


def now_iso() -> str:
    """UTC now, RFC 3339 with a `Z` suffix — the timestamp format on every seam."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class Urgency(str, Enum):
    urgent = "urgent"
    necessary = "necessary"
    waitable = "waitable"


class Privacy(str, Enum):
    local_only = "local_only"
    cloud_ok = "cloud_ok"


# Bounds on what a client can put on the wire. Not a quota and not a rate limit — those
# are separate and still absent — just a ceiling, so a single request cannot exhaust
# Redis or the disk. Redis holds the payload of every queued job in memory, so an
# unbounded `content` was an unbounded write to the broker from one POST.
#
# Sized to be irrelevant to real work and fatal to abuse: 1 MiB of prompt is far past any
# model's context window, and 512 messages is a conversation nothing local will hold.
# JobRecord carries them too: it is the shape that actually goes on the stream.
MAX_CONTENT_CHARS = 1_048_576
MAX_MESSAGES = 512
# A node's self-reported inventory writes one `node_models` row per entry, per node, and
# is never pruned. No real node serves hundreds of artifacts.
MAX_INVENTORY = 256


class Message(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str = Field(max_length=MAX_CONTENT_CHARS)


class Submitter(BaseModel):
    """Caller provenance (contract/job.schema.json → `submitter`).

    Answers "who submitted this, and is a burst of identical jobs one call retried or
    many real calls?" — the question a queue full of byte-identical payloads cannot
    answer on its own. Every field is optional so pre-existing clients keep working.

    Identification only: nothing in the coordinator dedupes, collapses, reorders or
    rejects on these. `request_id` is deliberately NOT unique-constrained — a client
    reusing one is *describing a retry*, which is exactly the signal we want to keep.
    """

    model_config = {"extra": "forbid"}

    app: str | None = Field(default=None, min_length=1)
    instance: str | None = Field(default=None, min_length=1)
    request_id: str | None = Field(default=None, min_length=1)
    # Advisory only — a clock-skew / queue-delay signal. `created_at` (server-stamped at
    # receipt) stays authoritative for ordering, escalation and metering.
    submitted_at: str | None = None


class Requires(BaseModel):
    """Hard capability requirements (ADR 37) — what a model must be able to DO.

    Deliberately not part of the ability score. Ability is a graded 1-10 judgement of how
    *well* a model does a task class; these are not that shape. A context window is a
    number with a hard edge, tool calling is a boolean, and a 4k-context model and a 128k
    one can both honestly be "a 6 at summarize" — so the matrix cannot see the difference
    between them, and routing a 60k-token document to the first silently truncates it.
    Filter on what a model *can* do, then compare how *well* it does it.

    Every field optional: a job that requires nothing is the overwhelmingly common case
    and must stay free of ceremony.
    """

    model_config = {"extra": "forbid"}

    context_tokens: int | None = Field(default=None, ge=1)
    tools: bool | None = None
    json_schema: bool | None = None
    vision: bool | None = None

    def asked_for(self) -> dict[str, object]:
        """Only the requirements actually stated. `False`/`None` are not requirements —
        "I do not need vision" must not exclude a model that happens to have it."""
        out: dict[str, object] = {}
        if self.context_tokens:
            out["context_tokens"] = self.context_tokens
        for name in ("tools", "json_schema", "vision"):
            if getattr(self, name):
                out[name] = True
        return out


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
    # Applied BEFORE ability is compared (ADR 37), for both addressing forms.
    requires: Requires | None = None

    messages: list[Message] | None = Field(default=None, max_length=MAX_MESSAGES)
    prompt: str | None = Field(default=None, max_length=MAX_CONTENT_CHARS)

    params: dict[str, Any] = Field(default_factory=dict)
    urgency: Urgency = Urgency.waitable
    escalate_after_min: int | None = Field(default=None, ge=0)
    privacy: Privacy = Privacy.local_only
    deadline: str | None = None
    reservation: str | None = None  # opt-in reservation id to queue against (§8)
    # An opaque client label, stored and never interpreted — the same standing as
    # `submitter`. It used to select a client's backlog for attention promotion; with
    # that feature gone it is kept because `extra: forbid` would turn a client still
    # sending it into a 422, and because the eval harness tags its own jobs with it.
    client_key: str | None = None
    submitter: Submitter | None = None  # optional caller provenance (§1b)

    @model_validator(mode="after")
    def _check(self) -> JobSubmit:
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
    def _check(self) -> ReservationSubmit:
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


class NodePolicy(BaseModel):
    """Owner's per-node contract (POST /nodes/{id}/policy). A body, not query params —
    bare scalars bound as query parameters, which made the approval gate trivially flippable."""

    model_config = {"extra": "forbid"}

    disk_quota_gb: float | None = Field(default=None, ge=0)
    auto_approve: bool | None = None
    # Opt-in: apply a signed worker update without asking. An update channel is RCE by
    # design (ADR 13), so this is off by default and per-node, like auto_approve.
    auto_update: bool | None = None


class CatalogEntrySubmit(BaseModel):
    """A catalog candidate the planner may propose installing (POST /catalog).

    `seed_catalog` only writes when the table is empty, so without a write path the
    catalog froze at whatever shipped and no newer model could ever be proposed. Curating
    it is an operator job, so this sits behind the operator key like the rest of the admin
    surface (ADR 26).

    `expected_ability` stays an admin ranking HINT (ADR 15): it orders candidates for the
    planner and is never the score routing uses. An approved install is measured by the
    eval harness before anything routes to it, so a wrong hint costs an eval, not a bad
    route.
    """

    model_config = {"extra": "forbid"}

    artifact: str = Field(min_length=1)       # primary key; re-POSTing edits in place
    registry_ref: str = Field(min_length=1)   # what the model-manager adapter pulls
    size_gb: float = Field(ge=0)
    min_ram_gb: float = Field(ge=0)
    source: str = "ollama"
    family: str | None = None
    params_b: float | None = Field(default=None, ge=0)
    # Parameters active per token (ADR 39). Omit for a dense model. Setting it below
    # params_b tells the fits gate this is a mixture-of-experts, where partial VRAM
    # residency is not reliably a penalty.
    active_params_b: float | None = Field(default=None, ge=0)
    quant: str | None = None
    # The 1-10 anchored scale (model-evaluation.md). Out-of-range hints are a typo, not a
    # preference, and would silently distort every ranking they took part in.
    expected_ability: float | None = Field(default=None, ge=1, le=10)
    # Capabilities, curated (ADR 37). Unlike `expected_ability` these ARE load-bearing at
    # routing time: a job requiring one is refused rather than served by an artifact that
    # does not declare it. Null means "not curated", which is deliberately not the same as
    # false — an uncurated artifact is excluded with a message saying so, instead of being
    # quietly assumed incapable or quietly assumed fine.
    context_tokens: int | None = Field(default=None, ge=1)
    supports_tools: bool | None = None
    supports_json_schema: bool | None = None
    supports_vision: bool | None = None


class HwProbe(BaseModel):
    """Hardware probe (contract/enroll-request.schema.json → hw)."""

    model_config = {"extra": "forbid"}

    ram_gb: float = Field(ge=0)
    accelerator: Literal["metal", "cuda", "cpu"]
    vram_gb: float | None = Field(default=None, ge=0)
    disk_free_gb: float = Field(ge=0)


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
    installed: list[str] = Field(default_factory=list, max_length=MAX_INVENTORY)
    loaded: list[str] = Field(default_factory=list, max_length=MAX_INVENTORY)
    # artifact → content digest, where the model server exposes it (drives re-eval, ADR 15).
    digests: dict[str, str] | None = None
    # The worker's build-stamped release version. The coordinator judges FITNESS from this,
    # not just protocol compatibility (ADR 27) — optional so a worker predating the field
    # still heartbeats rather than 422-ing, and is assessed as unverifiable instead.
    agent_version: str | None = None
    # Which runtime this node runs, and so which release artifact it can EXECUTE
    # ('python' → the py3-none-any artifact). Absent ⇒ 'python'; an unknown flavour is
    # offered no update at all (see api.artifact_key_for).
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
    messages: list[Message] | None = Field(default=None, max_length=MAX_MESSAGES)
    prompt: str | None = Field(default=None, max_length=MAX_CONTENT_CHARS)
    params: dict[str, Any] = Field(default_factory=dict)
    urgency: Urgency
    escalate_after_min: int | None = None
    privacy: Privacy
    deadline: str | None = None
    # Rides the wire because the worker acts on `json_schema`: it is what licenses
    # forwarding `params.response_format` to the model server, now that routing has
    # actually checked the chosen artifact can honour it (ADR 37).
    requires: Requires | None = None
    # Carried to the worker so it can refuse rather than under-serve (contract §job).
    result_key: str
    attempts: int = 0
    max_attempts: int = 3
    submitter: Submitter | None = None

    def to_wire(self) -> dict[str, Any]:
        """Contract-shaped dict: drop null optionals so it validates cleanly."""
        return self.model_dump(mode="json", exclude_none=True)

    @classmethod
    def from_wire(cls, data: dict[str, Any]) -> JobRecord:
        """The inverse of `to_wire` — used by the coordinator's own cloud executor (ADR 30)
        to read back a job it (or a client) enqueued, the same way a worker parses one."""
        return cls.model_validate(data)
