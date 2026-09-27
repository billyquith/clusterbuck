"""Can the coordinator reach each capability's model server? Measured, not declared.

`/fleet` used to echo `fleet.yaml`, so a capability whose model server was down looked
exactly as usable as one that was up, and a client found out by sending a request. The
sync plane is the case that matters: it calls the capability's `model_server` from the
coordinator directly, with no worker in the path, so a worker's heartbeat says nothing
about it. A worker can be heartbeating happily against a server bound to its own loopback
that the coordinator cannot reach at all — the node is healthy and the sync path is dead.

So this is the coordinator's own **measurement of the sync path**: a periodic `GET
{model_server}/models`, cached, and read by `/fleet`. It is deliberately advisory. The
sync plane never refuses a request on it (a server that came back since the last probe
would be turned away for a whole interval); a failed call is still classified by the call
itself. What this adds is the answer *before* the call, and a timestamp saying how old it is.

States:

- `unknown`  — not probed yet, or a provider account (no host to probe; a probe would
  spend a paid API call to learn what the provider's status page already says).
- `ready`    — reachable, and it lists the model the capability is registered to run.
- `degraded` — reachable, but answering non-2xx, or not listing that model: requests
  will reach a server that is likely to refuse or substitute.
- `unreachable` — no connection, or no answer within the probe timeout.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from .fleet import Fleet, _artifact_aliases, resolve_api_key

_log = logging.getLogger("clusterbuck.model_health")


def _iso(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, UTC).isoformat().replace("+00:00", "Z")


@dataclass
class _Probe:
    """What one model server said, once. Shared by every capability that points at it."""

    ok: bool                     # connected and answered 2xx
    reachable: bool              # connected at all
    checked_at: float
    listed: set[str] = field(default_factory=set)
    error: str | None = None


class ModelHealth:
    def __init__(self, fleet: Fleet | None, *, timeout_s: float = 2.0,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._fleet = fleet
        self._timeout_s = timeout_s
        self._transport = transport
        self._probes: dict[str, _Probe] = {}      # keyed by model_server URL
        self._last_ok: dict[str, float] = {}

    async def probe_all(self) -> None:
        """Probe every distinct model server once, concurrently."""
        if self._fleet is None:
            return
        # One probe per URL: several tiers commonly share one server, and probing it once
        # per tier would triple the traffic to say the same thing three times.
        targets: dict[str, str | None] = {}
        for spec in self._fleet.capabilities.values():
            if spec.model_server:
                targets.setdefault(spec.model_server, resolve_api_key(spec))
        if not targets:
            return
        async with httpx.AsyncClient(timeout=self._timeout_s,
                                     transport=self._transport) as client:
            results = await asyncio.gather(
                *(self._probe(client, url, key) for url, key in targets.items()))
        for url, probe in zip(targets, results, strict=True):
            prev = self._probes.get(url)
            if prev is not None and prev.ok != probe.ok:
                # Transitions only: a dead server probed every interval would otherwise
                # log the same line forever.
                _log.info("model server %s is now %s%s", url,
                          "reachable" if probe.ok else "failing",
                          f" ({probe.error})" if probe.error else "")
            self._probes[url] = probe
            if probe.ok:
                self._last_ok[url] = probe.checked_at

    async def _probe(self, client: httpx.AsyncClient, url: str, key: str | None) -> _Probe:
        headers = {"Authorization": f"Bearer {key}"} if key else None
        now = time.time()
        try:
            resp = await client.get(url.rstrip("/") + "/models", headers=headers)
        except httpx.TimeoutException:
            return _Probe(False, False, now, error=f"no answer within {self._timeout_s:g}s")
        except httpx.TransportError as e:
            return _Probe(False, False, now, error=f"connection failed: {type(e).__name__}")
        if resp.status_code >= 300:
            return _Probe(False, True, now, error=f"HTTP {resp.status_code} from /models")
        try:
            listed = {str(m.get("id")) for m in resp.json().get("data") or []}
        except (ValueError, AttributeError):
            return _Probe(False, True, now, error="/models answered with no model list")
        return _Probe(True, True, now, listed=listed)

    def view(self, capability: str) -> dict:
        """The health block `/fleet` shows for one capability."""
        spec = self._fleet.capabilities.get(capability) if self._fleet else None
        blank = {"state": "unknown", "checked_at": None, "last_ok_at": None,
                 "last_error": None}
        if spec is None or not spec.model_server:
            return blank
        probe = self._probes.get(spec.model_server)
        if probe is None:
            return blank
        last_ok = _iso(self._last_ok.get(spec.model_server))
        if not probe.reachable:
            state, error = "unreachable", probe.error
        elif not probe.ok:
            state, error = "degraded", probe.error
        elif not (_artifact_aliases(spec.model) & probe.listed):
            state, error = "degraded", f"server does not list {spec.model!r}"
        else:
            state, error = "ready", None
        return {"state": state, "checked_at": _iso(probe.checked_at),
                "last_ok_at": last_ok, "last_error": error}
