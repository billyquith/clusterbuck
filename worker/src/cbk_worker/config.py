"""Worker configuration, from environment with LAN-dev defaults."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field, replace
from pathlib import Path
from urllib.parse import unquote, urlparse

# This agent's build-stamped release version, and which implementation it is.
#
# AGENT_FLAVOUR decides which release artifact the coordinator offers us
# (contract/heartbeat-request.schema.json → agent_flavour): the single platform-independent
# `py3-none-any` build, never a platform-specific id. It is a constant, not a setting
# because a worker that misreports it would install a foreign executable over its own
# entrypoint — the coordinator verifies nothing about our runtime, it trusts this.
AGENT_VERSION = "0.19.0"
AGENT_FLAVOUR = "python"

# The queue-contract protocol version this worker speaks (protocols.md §7).
PROTOCOL_VERSION = 1


def _int_env(key: str, default: int) -> int:
    try:
        return int(os.environ[key])
    except (KeyError, ValueError):
        return default


def _float_env(key: str, default: float) -> float:
    try:
        return float(os.environ[key])
    except (KeyError, ValueError):
        return default


@dataclass(frozen=True)
class WorkerConfig:
    redis_url: str = "redis://localhost:6379"
    consumer_group: str = "cbk-workers"
    worker_id: str = field(default_factory=lambda: f"node-{uuid.uuid4().hex[:6]}")
    model_server_url: str = "http://localhost:11434/v1"
    model_name: str = "llama3.2:3b"
    capabilities: tuple[str, ...] = ("8b-extract",)
    poll_s: float = 1.0
    result_ttl_s: int = 86400
    heartbeat_s: float = 10.0
    # How long to wait on the model server for one completion.
    #
    # MUST stay below the coordinator's CBK_REAPER_MIN_IDLE_MS. A worker mid-generation is
    # not reading from Redis, so the reaper judges it by stream idle time alone: if the two
    # are equal, every generation that reaches the limit is reclaimed and re-run at the same
    # moment this worker gives up on it — double the compute for one answer. The coordinator
    # documents the other half of the pair at its own definition.
    inference_timeout_s: float = 600.0
    # Ceiling on jobs this node runs at once. 1 keeps the historical behaviour exactly,
    # which is why it is the default: raising it is an operator's decision about their
    # own machine, not something an upgrade should do to every node in a fleet.
    #
    # It is a CEILING, not a target. The effective limit is derived per beat from the
    # node's profile and the owner's presence (`WorkLoop.effective_limit`), because a
    # flat number cannot be right for both a dedicated box that exists to serve and a
    # laptop somebody is using.
    #
    # Worth raising when the model server batches well — vLLM's continuous batching, or
    # Ollama with OLLAMA_NUM_PARALLEL — since a single-request-at-a-time worker leaves
    # most of a fast accelerator idle. Worth leaving at 1 when it does not: concurrent
    # requests to a server that serialises them internally buy nothing and cost memory.
    max_concurrent_jobs: int = 1
    # Which model-manager adapter handles the non-OpenAI bits (residency, digests,
    # installs, unloading): auto | ollama | lmstudio | none.
    #
    # `auto` PROBES for whichever native API answers rather than assuming Ollama, which
    # is what it used to mean — an LM Studio node left on the default therefore reported
    # nothing warm and no digests, indistinguishable from a healthy idle one. `none`
    # opts out of native calls entirely: discovery via the portable /v1/models only.
    model_manager: str = "auto"
    # How long the owner must be stably away before the ladder climbs to the big models
    # (ADR 10). Cold loads are expensive, so this is damped by default.
    ladder_hysteresis_s: float = 120.0

    @staticmethod
    def from_environment() -> WorkerConfig:
        caps = os.environ.get("CBK_CAPABILITIES", "").strip()
        return WorkerConfig(
            redis_url=os.environ.get("CBK_REDIS_URL") or "redis://localhost:6379",
            consumer_group=os.environ.get("CBK_CONSUMER_GROUP") or "cbk-workers",
            worker_id=os.environ.get("CBK_WORKER_ID") or f"node-{uuid.uuid4().hex[:6]}",
            model_server_url=(os.environ.get("CBK_MODEL_SERVER_URL")
                              or "http://localhost:11434/v1"),
            model_name=os.environ.get("CBK_MODEL") or "llama3.2:3b",
            capabilities=(tuple(c.strip() for c in caps.split(",") if c.strip())
                          if caps else ("8b-extract",)),
            poll_s=_int_env("CBK_POLL_MS", 1000) / 1000.0,
            result_ttl_s=_int_env("CBK_RESULT_TTL_S", 86400),
            heartbeat_s=_int_env("CBK_HEARTBEAT_MS", 10000) / 1000.0,
            inference_timeout_s=_float_env("CBK_INFERENCE_TIMEOUT_S", 600.0),
            max_concurrent_jobs=max(1, _int_env("CBK_MAX_CONCURRENT_JOBS", 1)),
            model_manager=os.environ.get("CBK_MODEL_MANAGER") or "auto",
            ladder_hysteresis_s=_float_env("CBK_LADDER_HYSTERESIS_S", 120.0),
        )

    def with_overrides(self, *, capabilities: str | None = None,
                       model: str | None = None) -> WorkerConfig:
        """Apply CLI overrides on top of the environment."""
        out = self
        if capabilities:
            parsed = tuple(c.strip() for c in capabilities.split(",") if c.strip())
            if parsed:
                out = replace(out, capabilities=parsed)
        if model:
            out = replace(out, model_name=model)
        return out


def normalise_redis_url(value: str) -> str:
    """Accept a bare host:port as well as a redis:// URL."""
    if "://" in value:
        return value
    return f"redis://{value}"


def redis_password_from_url(value: str) -> str | None:
    """The password in a redis:// URL, if any. Split out so tests can assert it survives:
    silently dropping it makes an authenticated broker look simply unreachable."""
    parsed = urlparse(normalise_redis_url(value))
    return unquote(parsed.password) if parsed.password else None


def update_public_key_pem() -> str | None:
    """PEM of the update-signing public key this agent pins (CBK_UPDATE_PUBKEY = a file path,
    or the PEM inline). UNSET ⇒ self-update is refused outright: an update channel is RCE by
    design, so there is no unsigned path (ADR 13)."""
    value = os.environ.get("CBK_UPDATE_PUBKEY", "").strip()
    if not value:
        return None
    if "BEGIN PUBLIC KEY" in value:
        return value
    path = Path(value)
    return path.read_text() if path.is_file() else None


def api_key() -> str | None:
    """Operator shared secret for the coordinator API (CBK_API_KEY). Needed by the admin CLI
    verbs; the worker loop itself does not use it, because enroll is join-token authenticated
    and heartbeat is node-key authenticated (ADR 26)."""
    return os.environ.get("CBK_API_KEY") or None


def model_server_api_key() -> str | None:
    """Bearer token for THIS node's own configured model server (CBK_MODEL_SERVER_API_KEY).

    Node-local config, same trust boundary as CBK_MODEL_SERVER_URL — an operator who put an
    authenticated gateway behind that URL can now give the worker a key for it. This is
    NOT how cloud provider accounts are reached: those are registered on the coordinator
    and called by the coordinator itself, so their keys never reach a worker (ADR 30). A
    job's own `params` can never substitute a different key or base URL — see
    model_client.py's `_PARAMS_NOT_FORWARDED`.
    """
    return os.environ.get("CBK_MODEL_SERVER_API_KEY") or None


def admin_headers() -> dict[str, str]:
    """Headers presenting the operator key, when one is configured."""
    key = api_key()
    return {"X-CBK-Api-Key": key} if key else {}


# Must match the coordinator's own default (server/clusterbuck/__main__.py → DEFAULT_PORT).
# The two drifted once already: the docs and installers moved to 8018 while both code
# defaults stayed on 8000, so an un-configured `cbk submit` talked to the wrong port.
DEFAULT_SERVER_URL = "http://localhost:8018"


def server_url(cli_value: str | None = None) -> str:
    return (cli_value or os.environ.get("CBK_SERVER_URL") or DEFAULT_SERVER_URL).rstrip("/")
