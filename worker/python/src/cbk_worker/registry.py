"""Enrollment + heartbeat over HTTP to the coordinator (protocols.md §6), and the persisted
node identity that survives restarts."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import httpx

from .models import EnrollRequest, Fitness, HeartbeatRequest, ModelAction, NodeState


class HeartbeatResponse:
    """The coordinator's reply: a verdict, maybe an update, maybe an approved action."""

    def __init__(self, body: dict[str, Any]) -> None:
        self.raw = body
        self.update: dict[str, Any] | None = body.get("update")
        self.action: ModelAction | None = ModelAction.from_wire(body.get("action"))
        self.fitness: Fitness | None = Fitness.from_wire(body.get("fitness"))
        self.planner_notes: list[str] = body.get("planner_notes") or []


class RegistryClient:
    def __init__(self, client: httpx.AsyncClient, server: str) -> None:
        self._client = client
        self._server = server.rstrip("/")

    async def enroll(self, req: EnrollRequest) -> dict[str, Any]:
        # Enrollment is authenticated by the join token, not the operator key (ADR 26), so a
        # plain client is correct here — a worker never needs the admin secret.
        resp = await self._client.post(f"{self._server}/nodes/enroll", json=req.to_wire())
        resp.raise_for_status()
        return resp.json()

    async def heartbeat(self, node_id: str, node_key: str,
                        req: HeartbeatRequest) -> HeartbeatResponse:
        resp = await self._client.post(
            f"{self._server}/nodes/{node_id}/heartbeat",
            json=req.to_wire(), headers={"X-CBK-Node-Key": node_key})
        resp.raise_for_status()
        return HeartbeatResponse(resp.json())


def default_state_path() -> Path:
    override = os.environ.get("CBK_NODE_STATE")
    if override:
        return Path(override)
    return Path.home() / ".clusterbuck" / "node.json"


def load_state(path: Path | str) -> NodeState | None:
    p = Path(path)
    if not p.is_file():
        return None
    return NodeState.from_wire(json.loads(p.read_text()))


def save_state(path: Path | str, state: NodeState) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state.to_wire(), indent=2))
