"""Configuration and the hardware probe."""

from __future__ import annotations

from cbk_worker import probe
from cbk_worker.config import (
    AGENT_FLAVOUR,
    AGENT_VERSION,
    WorkerConfig,
    admin_headers,
    normalise_redis_url,
    redis_password_from_url,
    server_url,
    update_public_key_pem,
)

_ENV = ["CBK_REDIS_URL", "CBK_CONSUMER_GROUP", "CBK_WORKER_ID", "CBK_MODEL_SERVER_URL",
        "CBK_MODEL", "CBK_CAPABILITIES", "CBK_POLL_MS", "CBK_RESULT_TTL_S",
        "CBK_HEARTBEAT_MS", "CBK_MODEL_MANAGER", "CBK_LADDER_HYSTERESIS_S",
        "CBK_UPDATE_PUBKEY", "CBK_API_KEY", "CBK_SERVER_URL", "CBK_NODE_STATE"]


def _clean(monkeypatch):
    for k in _ENV:
        monkeypatch.delenv(k, raising=False)


def test_worker_defaults(monkeypatch):
    _clean(monkeypatch)
    cfg = WorkerConfig.from_environment()
    assert cfg.redis_url == "redis://localhost:6379"
    assert cfg.consumer_group == "cbk-workers"
    assert cfg.model_server_url == "http://localhost:11434/v1"
    assert cfg.model_name == "llama3.2:3b"
    assert cfg.capabilities == ("8b-extract",)
    assert cfg.result_ttl_s == 86400
    assert cfg.model_manager == "auto"
    assert cfg.ladder_hysteresis_s == 120.0
    assert cfg.worker_id.startswith("node-")


def test_every_setting_is_readable_from_the_environment(monkeypatch):
    _clean(monkeypatch)
    monkeypatch.setenv("CBK_REDIS_URL", "redis://broker:6380/3")
    monkeypatch.setenv("CBK_CONSUMER_GROUP", "other-group")
    monkeypatch.setenv("CBK_WORKER_ID", "node-fixed")
    monkeypatch.setenv("CBK_MODEL_SERVER_URL", "http://lm:1234/v1")
    monkeypatch.setenv("CBK_MODEL", "qwen2.5:32b")
    monkeypatch.setenv("CBK_CAPABILITIES", " 8b-extract , 32b-reason ")
    monkeypatch.setenv("CBK_POLL_MS", "250")
    monkeypatch.setenv("CBK_RESULT_TTL_S", "60")
    monkeypatch.setenv("CBK_HEARTBEAT_MS", "2000")
    monkeypatch.setenv("CBK_MODEL_MANAGER", "none")
    monkeypatch.setenv("CBK_LADDER_HYSTERESIS_S", "0")

    cfg = WorkerConfig.from_environment()
    assert cfg.redis_url == "redis://broker:6380/3"
    assert cfg.consumer_group == "other-group"
    assert cfg.worker_id == "node-fixed"
    assert cfg.model_server_url == "http://lm:1234/v1"
    assert cfg.model_name == "qwen2.5:32b"
    # Whitespace around a comma-separated list is the operator's, not a capability's.
    assert cfg.capabilities == ("8b-extract", "32b-reason")
    assert cfg.poll_s == 0.25            # ms on the wire, seconds internally
    assert cfg.heartbeat_s == 2.0
    assert cfg.result_ttl_s == 60
    assert cfg.model_manager == "none"
    assert cfg.ladder_hysteresis_s == 0.0


def test_a_junk_numeric_setting_falls_back_instead_of_crashing_the_worker(monkeypatch):
    _clean(monkeypatch)
    monkeypatch.setenv("CBK_POLL_MS", "not-a-number")
    monkeypatch.setenv("CBK_LADDER_HYSTERESIS_S", "")
    cfg = WorkerConfig.from_environment()
    assert cfg.poll_s == 1.0 and cfg.ladder_hysteresis_s == 120.0


def test_cli_overrides_layer_on_top_of_the_environment(monkeypatch):
    _clean(monkeypatch)
    monkeypatch.setenv("CBK_MODEL", "from-env")
    cfg = WorkerConfig.from_environment().with_overrides(
        capabilities="70b-reason", model="from-cli")
    assert cfg.capabilities == ("70b-reason",) and cfg.model_name == "from-cli"
    # An empty override is not an override.
    assert cfg.with_overrides(capabilities="", model=None).capabilities == ("70b-reason",)


def test_bare_host_port_is_accepted_as_well_as_a_url():
    """CBK_REDIS_URL may be set as a bare host:port or a redis:// URL."""
    assert normalise_redis_url("localhost:6379") == "redis://localhost:6379"
    assert normalise_redis_url("redis://h:6379/2") == "redis://h:6379/2"


def test_a_password_in_the_redis_url_survives():
    """Dropping it silently makes an authenticated broker look simply unreachable."""
    assert redis_password_from_url("redis://:s3cr3t@host:6379/0") == "s3cr3t"
    assert redis_password_from_url("redis://user:p%40ss@host:6379") == "p@ss"
    assert redis_password_from_url("localhost:6379") is None


def test_update_pubkey_accepts_inline_pem_or_a_path(monkeypatch, tmp_path):
    """Whichever form the operator uses, the PEM that comes back must still be usable for
    verification — trimming stray whitespace must not quietly break the key."""
    from pathlib import Path

    _clean(monkeypatch)
    assert update_public_key_pem() is None          # unset ⇒ self-update refused (ADR 13)

    contract = Path(__file__).resolve().parents[2] / "contract" / "examples"
    pem = (contract / "update-signing.pub.pem").read_text()

    def _usable(value: str | None) -> bool:
        from cryptography.hazmat.primitives.serialization import load_pem_public_key
        return value is not None and load_pem_public_key(value.encode()) is not None

    monkeypatch.setenv("CBK_UPDATE_PUBKEY", pem)            # inline
    assert _usable(update_public_key_pem())
    monkeypatch.setenv("CBK_UPDATE_PUBKEY", f"  {pem}  ")   # inline, sloppily quoted
    assert _usable(update_public_key_pem())

    p = tmp_path / "k.pub.pem"
    p.write_text(pem)
    monkeypatch.setenv("CBK_UPDATE_PUBKEY", str(p))         # path
    assert _usable(update_public_key_pem())

    monkeypatch.setenv("CBK_UPDATE_PUBKEY", str(tmp_path / "missing.pem"))
    assert update_public_key_pem() is None          # a bad path must not be a bypass


def test_operator_key_is_presented_only_when_configured(monkeypatch):
    _clean(monkeypatch)
    assert admin_headers() == {}
    monkeypatch.setenv("CBK_API_KEY", "shh")
    assert admin_headers() == {"X-CBK-Api-Key": "shh"}


def test_server_url_precedence(monkeypatch):
    _clean(monkeypatch)
    assert server_url() == "http://localhost:8000"
    monkeypatch.setenv("CBK_SERVER_URL", "http://coordinator:8077/")
    assert server_url() == "http://coordinator:8077"
    assert server_url("http://explicit:9000/") == "http://explicit:9000"


def test_this_build_declares_itself_as_the_python_flavour():
    assert AGENT_FLAVOUR == "python"
    assert AGENT_VERSION.count(".") == 2


# --- hardware probe ---------------------------------------------------------------------


def test_probe_reports_this_machine_plausibly():
    req = probe.build("jt_token", "shared")
    assert req.join_token == "jt_token" and req.profile == "shared"
    assert req.os in ("darwin", "linux", "windows", "unknown")
    assert req.hw.ram_gb > 0, "probed no RAM at all"
    assert req.hw.accelerator in ("metal", "cuda", "cpu")
    assert req.hostname


def test_arch_is_normalised_to_the_coordinators_vocabulary():
    """The coordinator's RID table is keyed on x64/arm64, so uname's x86_64/aarch64 spellings
    have to be mapped here or no artifact is ever matched."""
    assert probe.arch_name() in ("x64", "arm64"), probe.arch_name()
