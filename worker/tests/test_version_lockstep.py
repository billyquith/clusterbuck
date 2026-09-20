"""The version reported by the worker must match the package metadata.

`CBK_WORKER_CURRENT_VERSION` on the coordinator is one value for the whole fleet (ADR 27/29),
so the version must agree in two places:

  * worker/src/cbk_worker/config.py   AGENT_VERSION   (what the worker reports at runtime)
  * worker/pyproject.toml             version         (the wheel/zipapp metadata)

Same idiom as contract/validate.py and the conformance suites: drift that a comment warns
about is drift a test should catch.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _agent_version() -> str:
    text = (REPO / "worker/src/cbk_worker/config.py").read_text(encoding="utf-8")
    return re.search(r'AGENT_VERSION = "([^"]+)"', text).group(1)


def _pyproject_version() -> str:
    text = (REPO / "worker/pyproject.toml").read_text(encoding="utf-8")
    return re.search(r'(?m)^version = "([^"]+)"', text).group(1)


def test_agent_version_matches_pyproject():
    agent, proj = _agent_version(), _pyproject_version()
    assert agent == proj, (
        "worker versions have drifted — the coordinator's single "
        "CBK_WORKER_CURRENT_VERSION would judge this build stale forever (ADR 29):\n"
        f"  config.py  AGENT_VERSION = {agent}\n"
        f"  pyproject  version       = {proj}"
    )


def _coordinator_default_port() -> str:
    text = (REPO / "server/clusterbuck/__main__.py").read_text(encoding="utf-8")
    return re.search(r"DEFAULT_PORT = (\d+)", text).group(1)


def test_default_server_url_matches_the_coordinators_default_port():
    """The worker's un-configured CBK_SERVER_URL must point at where the coordinator
    actually listens by default.

    These drifted once: c8d06c0 moved the documented default 8000 → 8018 across docs and
    installers but left both code defaults on 8000, so anyone who started the coordinator by
    hand and followed the quickstart curl'd a closed port, and `cbk submit` with no
    CBK_SERVER_URL talked to the wrong one.
    """
    from cbk_worker.config import DEFAULT_SERVER_URL

    port = _coordinator_default_port()
    assert DEFAULT_SERVER_URL.endswith(f":{port}"), (
        "the worker's default coordinator URL and the coordinator's default port have "
        f"drifted:\n  worker  DEFAULT_SERVER_URL = {DEFAULT_SERVER_URL}\n"
        f"  server  DEFAULT_PORT       = {port}"
    )


def test_installers_and_docs_agree_on_that_port():
    """The third place the port is written down. All three or none."""
    port = _coordinator_default_port()
    installer = (REPO / "install/coordinator/install.sh").read_text(encoding="utf-8")
    assert re.search(rf'^PORT="{port}"', installer, re.M), \
        f"install/coordinator/install.sh does not default to port {port}"
    env_example = (REPO / "deploy/systemd/server.env.example").read_text(encoding="utf-8")
    assert f"CBK_PORT={port}" in env_example
