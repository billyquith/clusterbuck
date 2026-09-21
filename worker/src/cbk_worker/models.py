"""Wire types for the shared contract (contract/*.schema.json).

The schemas in `contract/` are the source of truth; these are this worker's view of them and
the conformance suite asserts the two cannot drift (ADR 22).

Nulls are omitted on the way out: a "done" result carries a real completion object and
no stray nulls, and the shared schemas permit absent optionals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _prune(d: dict[str, Any]) -> dict[str, Any]:
    """Drop keys whose value is None (absent ≠ null on this wire)."""
    return {k: v for k, v in d.items() if v is not None}


# --- async plane: the queue contract (protocols.md §2) ---------------------------------


@dataclass(frozen=True)
class Job:
    """An enqueued job the worker consumes from a capability stream (job.schema.json).
    Parse-only on the worker side."""

    id: str
    capability: str
    result_key: str
    created_at: str = ""
    messages: list[dict[str, str]] | None = None
    prompt: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    urgency: str = ""
    privacy: str = ""
    escalate_after_min: int | None = None
    deadline: str | None = None
    attempts: int = 0
    max_attempts: int = 0

    @staticmethod
    def from_wire(d: dict[str, Any]) -> Job:
        return Job(
            id=d["id"],
            capability=d.get("capability", ""),
            result_key=d["result_key"],
            created_at=d.get("created_at", ""),
            messages=d.get("messages"),
            prompt=d.get("prompt"),
            params=d.get("params") or {},
            urgency=d.get("urgency", ""),
            privacy=d.get("privacy", ""),
            escalate_after_min=d.get("escalate_after_min"),
            deadline=d.get("deadline"),
            attempts=d.get("attempts", 0),
            max_attempts=d.get("max_attempts", 0),
        )


@dataclass(frozen=True)
class Result:
    """The terminal result the worker writes to the result store (result.schema.json).
    Produce-only on the worker side."""

    job_id: str
    status: str
    worker: str
    # Measured around the model call: `started_at` immediately before it, `finished_at`
    # immediately after. The pair is the real inference duration, taken where it happens.
    started_at: str
    finished_at: str
    completion: dict[str, Any] | None = None
    error: str | None = None
    usage: dict[str, Any] | None = None

    def to_wire(self) -> dict[str, Any]:
        return _prune({
            "job_id": self.job_id,
            "status": self.status,
            "worker": self.worker,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            # DEPRECATED alias, kept for one release. It was always stamped BEFORE
            # inference, so it is a start time despite the name — emitting `started_at`
            # here means no existing reader's numbers silently change while readers move
            # to the two honest fields.
            "completed_at": self.started_at,
            "completion": self.completion,
            "error": self.error,
            "usage": self.usage,
        })


# --- fleet membership (protocols.md §6) ------------------------------------------------


@dataclass(frozen=True)
class HwProbe:
    ram_gb: float
    accelerator: str = "cpu"
    disk_free_gb: float = 0.0
    vram_gb: float | None = None

    def to_wire(self) -> dict[str, Any]:
        return _prune({
            "ram_gb": self.ram_gb,
            "accelerator": self.accelerator,
            "disk_free_gb": self.disk_free_gb,
            "vram_gb": self.vram_gb,
        })


@dataclass(frozen=True)
class EnrollRequest:
    join_token: str
    hostname: str
    os: str
    arch: str
    hw: HwProbe
    profile: str = "shared"

    def to_wire(self) -> dict[str, Any]:
        return {
            "join_token": self.join_token,
            "hostname": self.hostname,
            "os": self.os,
            "arch": self.arch,
            "hw": self.hw.to_wire(),
            "profile": self.profile,
        }


@dataclass(frozen=True)
class ActionResult:
    """Outcome of an approved model action, reported on the next heartbeat."""

    proposal_id: str
    ok: bool
    error: str | None = None

    def to_wire(self) -> dict[str, Any]:
        return _prune({"proposal_id": self.proposal_id, "ok": self.ok, "error": self.error})


@dataclass(frozen=True)
class HeartbeatRequest:
    mode: str
    installed: list[str] = field(default_factory=list)
    loaded: list[str] = field(default_factory=list)
    queues: list[str] = field(default_factory=list)
    digests: dict[str, str] | None = None
    stats: dict[str, Any] = field(default_factory=dict)
    protocol_version: int | None = None
    agent_version: str | None = None
    agent_flavour: str | None = None
    action_result: ActionResult | None = None

    def to_wire(self) -> dict[str, Any]:
        return _prune({
            "mode": self.mode,
            "installed": self.installed,
            "loaded": self.loaded,
            "queues": self.queues,
            "digests": self.digests,
            "stats": self.stats,
            "protocol_version": self.protocol_version,
            "agent_version": self.agent_version,
            "agent_flavour": self.agent_flavour,
            "action_result": self.action_result.to_wire() if self.action_result else None,
        })


@dataclass(frozen=True)
class Fitness:
    """The coordinator's verdict on whether this build may run jobs (ADR 27).

    `ok` — fit. `stale` — usable but behind; keep working, warn the operator.
    `quarantine` — do NOT claim jobs: this version is below the supported floor or explicitly
    blocked as buggy, so anything it produced would be suspect.
    """

    status: str = "ok"
    reason: str | None = None
    current_version: str | None = None

    @staticmethod
    def from_wire(d: dict[str, Any] | None) -> Fitness | None:
        if not d:
            return None
        return Fitness(status=d.get("status", "ok"), reason=d.get("reason"),
                       current_version=d.get("current_version"))


@dataclass(frozen=True)
class ModelAction:
    """An approved model-management action issued by the coordinator."""

    proposal_id: str
    kind: str
    artifact: str
    registry_ref: str | None = None
    source: str = ""

    @staticmethod
    def from_wire(d: dict[str, Any] | None) -> ModelAction | None:
        if not d:
            return None
        return ModelAction(
            proposal_id=d.get("proposal_id", ""), kind=d.get("kind", ""),
            artifact=d.get("artifact", ""), registry_ref=d.get("registry_ref"),
            source=d.get("source", ""),
        )


@dataclass(frozen=True)
class UpdateManifest:
    """A signed worker release manifest (update-manifest.schema.json)."""

    version: str = ""
    rid: str = ""
    url: str = ""
    sha256: str = ""
    channel: str = ""
    signature: str = ""
    protocol_version: int | None = None

    @staticmethod
    def from_wire(d: dict[str, Any]) -> UpdateManifest:
        return UpdateManifest(
            version=d.get("version", ""), rid=d.get("rid", ""), url=d.get("url", ""),
            sha256=d.get("sha256", ""), channel=d.get("channel", ""),
            signature=d.get("signature", ""), protocol_version=d.get("protocol_version"),
        )


@dataclass(frozen=True)
class NodeState:
    """Persisted node identity + mode (survives restarts; written by enroll/pause)."""

    node_id: str
    node_key: str
    server: str
    capabilities: list[str] = field(default_factory=list)
    ladder: dict[str, list[str]] | None = None
    mode: str = "active"

    @staticmethod
    def from_wire(d: dict[str, Any]) -> NodeState:
        return NodeState(
            node_id=d["node_id"], node_key=d["node_key"], server=d.get("server", ""),
            capabilities=d.get("capabilities") or [], ladder=d.get("ladder"),
            mode=d.get("mode", "active"),
        )

    def to_wire(self) -> dict[str, Any]:
        return _prune({
            "node_id": self.node_id,
            "node_key": self.node_key,
            "server": self.server,
            "capabilities": self.capabilities,
            "ladder": self.ladder,
            "mode": self.mode,
        })
