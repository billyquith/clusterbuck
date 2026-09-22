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
    pem = (contract / "update-signing.pub.pem").read_text(encoding="utf-8")

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
    assert server_url() == "http://localhost:8018"
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


# --- VRAM: the number that decides whether a model runs FAST, not merely at all ----------


def test_metal_vram_is_the_wired_share_of_unified_memory_not_all_of_it(monkeypatch):
    """Reporting a Mac's full RAM as usable by the GPU overstates it by a quarter.

    macOS caps what the GPU may wire down, so a 64 GB machine offers ~48 GB to a model. A
    fits gate handed 64 would approve a pull that then swaps — the failure mode the cap
    exists to prevent.
    """
    monkeypatch.setattr(probe, "_run", lambda *a, **k: "")  # no explicit limit set
    ram = 64 * 2**30
    got = probe._metal_vram_bytes(ram)
    assert got is not None and got < ram
    assert round(got / 2**30) == 48


def test_a_small_mac_is_allowed_a_smaller_share_than_a_large_one(monkeypatch):
    monkeypatch.setattr(probe, "_run", lambda *a, **k: "")
    small = probe._metal_vram_bytes(16 * 2**30) / (16 * 2**30)
    large = probe._metal_vram_bytes(64 * 2**30) / (64 * 2**30)
    assert small < large, "the default wired fraction rises with machine size"


def test_an_explicit_wired_limit_is_honoured_over_the_default_fraction(monkeypatch):
    """`sysctl iogpu.wired_limit_mb` is an operator saying what they want; believe them."""
    monkeypatch.setattr(probe, "_run", lambda *a, **k: "57344\n")   # 56 GiB
    got = probe._metal_vram_bytes(64 * 2**30)
    assert round(got / 2**30) == 56


def test_a_wired_limit_cannot_claim_more_than_the_machine_has(monkeypatch):
    monkeypatch.setattr(probe, "_run", lambda *a, **k: "999999\n")
    ram = 16 * 2**30
    assert probe._metal_vram_bytes(ram) == ram


def test_zero_wired_limit_means_default_not_a_gpu_with_no_memory(monkeypatch):
    """0 is the sentinel macOS reports when the cap is unset. Taking it literally would
    report a Metal node as having no VRAM at all and exclude it from every fast path."""
    monkeypatch.setattr(probe, "_run", lambda *a, **k: "0\n")
    got = probe._metal_vram_bytes(32 * 2**30)
    assert got is not None and got > 0


def test_cuda_vram_takes_the_largest_card_not_the_sum(monkeypatch):
    """A model has to fit in ONE device to run at device speed. Summing two 12 GB cards
    would claim 24 GB of capacity that no single artifact can actually use."""
    monkeypatch.setattr(probe, "_run", lambda *a, **k: "12288\n24576\n")
    assert round(probe._cuda_vram_bytes() / 2**30) == 24


def test_cpu_node_reports_unknown_vram_rather_than_zero():
    """None and 0.0 mean different things to the fits gate: unknown falls back to judging
    by RAM, whereas zero is positive evidence that nothing will fit on an accelerator."""
    assert probe.vram_bytes("cpu", 8 * 2**30) is None


def test_cuda_vram_degrades_to_unknown_when_the_query_says_nothing(monkeypatch):
    monkeypatch.setattr(probe, "_run", lambda *a, **k: "")
    assert probe._cuda_vram_bytes() is None


# --- disk: the volume the weights actually land on --------------------------------------


def test_model_store_env_var_wins_over_the_default_path(monkeypatch, tmp_path):
    store = tmp_path / "weights"
    store.mkdir()
    monkeypatch.setenv("OLLAMA_MODELS", str(store))
    assert probe.model_store_path() == str(store)


def test_disk_free_is_measured_where_the_pull_will_land(monkeypatch, tmp_path):
    """The old probe always measured the ROOT filesystem. Weights live wherever the model
    server keeps them, which on plenty of machines is a different volume — so the disk
    quota gate could approve a 40 GB pull onto a full disk, or refuse one onto an empty."""
    seen = {}

    def fake_usage(path):
        seen["path"] = path
        import collections
        return collections.namedtuple("U", "total used free")(0, 0, 123.0)

    store = tmp_path / "models"
    store.mkdir()
    monkeypatch.setenv("OLLAMA_MODELS", str(store))
    monkeypatch.setattr(probe.shutil, "disk_usage", fake_usage)
    assert probe.disk_free_bytes() == 123.0
    assert seen["path"] == str(store)


def test_a_missing_store_path_measures_its_nearest_existing_parent(tmp_path):
    """disk_usage raises on a path that doesn't exist. A node whose model server has never
    pulled anything yet has no store directory, and must still enrol."""
    assert probe.disk_free_bytes(str(tmp_path / "not" / "created" / "yet")) > 0


def test_probe_reports_vram_only_where_there_is_an_accelerator():
    req = probe.build("jt_token", "shared")
    if req.hw.accelerator == "cpu":
        assert req.hw.vram_gb is None
    else:
        assert req.hw.vram_gb and req.hw.vram_gb > 0


# --- broker connection settings (B3) --------------------------------------------------


def _kwargs():
    from cbk_worker import broker

    return broker.connect("redis://localhost:6379/0").connection_pool.connection_kwargs


def test_the_broker_client_states_every_setting_that_makes_sleep_survivable():
    """These are asserted, not assumed, because assuming them is the bug.

    `Redis.from_url(url, decode_responses=True)` produced a working client only because
    the redis-py we happen to vendor defaults close to what a sleeping fleet needs. On
    5.x the same call gives no socket timeout, no keepalive and no retries — and a
    machine that suspends does not close its TCP connections, so the next read on the
    dead socket blocks until the kernel gives up, which without a timeout is never. The
    worker then sits alive and silent, claiming nothing and crashing nowhere, so neither
    launchd nor systemd notices. A zombie is strictly worse than a crash.
    """
    from cbk_worker import broker

    kw = _kwargs()
    assert kw["socket_timeout"] == broker.SOCKET_TIMEOUT_S
    assert kw["socket_connect_timeout"] == broker.CONNECT_TIMEOUT_S
    assert kw["socket_keepalive"] is True
    assert kw["health_check_interval"] == broker.HEALTH_CHECK_INTERVAL_S


def test_no_setting_is_left_at_the_librarys_default():
    """The point is not the particular numbers, it is that none of them is None/off.

    A future redis-py changing its defaults must not be able to change this worker's
    behaviour on a machine that sleeps.
    """
    kw = _kwargs()
    assert kw["socket_timeout"] not in (None, 0), "None here is 'block forever after a wake'"
    assert kw["socket_connect_timeout"] not in (None, 0)
    assert kw["socket_keepalive"], "a peer that forgot us must be detected by the kernel"
    assert kw["health_check_interval"], (
        "the setting aimed squarely at suspend/resume: a connection is only ever idle "
        "this long because the machine was not running, so the first command after a "
        "wake must validate the socket instead of sending a real claim into a dead one"
    )


def test_connection_failures_are_retried_before_they_reach_the_loop():
    """Ten attempts over ~10s: long enough to ride out a broker restart or a Wi-Fi
    reassociation, short enough that an absent broker still surfaces as an error."""
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    retry = _kwargs()["retry"]
    assert retry is not None, "an unset retry is the 5.x default: none at all"
    assert retry.get_retries() >= 3
    supported = set(retry._supported_errors)
    assert RedisConnectionError in supported
    assert RedisTimeoutError in supported


def test_a_bare_host_port_is_still_accepted():
    """`connect` must keep the URL normalisation the old call site had."""
    from cbk_worker import broker

    client = broker.connect("localhost:6379")
    assert client.connection_pool.connection_kwargs["host"] == "localhost"
