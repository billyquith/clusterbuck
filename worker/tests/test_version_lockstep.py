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
    text = (REPO / "worker/src/cbk_worker/config.py").read_text()
    return re.search(r'AGENT_VERSION = "([^"]+)"', text).group(1)


def _pyproject_version() -> str:
    text = (REPO / "worker/pyproject.toml").read_text()
    return re.search(r'(?m)^version = "([^"]+)"', text).group(1)


def test_agent_version_matches_pyproject():
    agent, proj = _agent_version(), _pyproject_version()
    assert agent == proj, (
        "worker versions have drifted — the coordinator's single "
        "CBK_WORKER_CURRENT_VERSION would judge this build stale forever (ADR 29):\n"
        f"  config.py  AGENT_VERSION = {agent}\n"
        f"  pyproject  version       = {proj}"
    )
