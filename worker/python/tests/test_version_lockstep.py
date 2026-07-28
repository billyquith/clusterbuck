"""The two worker flavours must report the SAME version.

`CBK_WORKER_CURRENT_VERSION` on the coordinator is one value for the whole fleet (ADR 27/29),
so if the Python and .NET workers stamped different versions, one flavour would be permanently
judged `stale` and offered an update it does not need. The version therefore lives in three
places that must agree, and nothing but this test enforces it:

  * worker/python/src/cbk_worker/config.py   AGENT_VERSION   (what a Python worker reports)
  * worker/python/pyproject.toml              version        (the wheel/zipapp metadata)
  * worker/dotnet/.../Clusterbuck.Worker.csproj  <Version>   (what a .NET worker reports)

Same idiom as contract/validate.py and the conformance suites: drift that a comment warns
about is drift a test should catch.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]


def _python_agent_version() -> str:
    text = (REPO / "worker/python/src/cbk_worker/config.py").read_text()
    return re.search(r'AGENT_VERSION = "([^"]+)"', text).group(1)


def _pyproject_version() -> str:
    text = (REPO / "worker/python/pyproject.toml").read_text()
    # The first top-level `version = "…"`, i.e. under [project] before any other table.
    return re.search(r'(?m)^version = "([^"]+)"', text).group(1)


def _dotnet_version() -> str:
    text = (REPO / "worker/dotnet/src/Clusterbuck.Worker/Clusterbuck.Worker.csproj").read_text()
    return re.search(r"<Version>([^<]+)</Version>", text).group(1)


def test_all_three_version_sources_agree():
    py, proj, net = _python_agent_version(), _pyproject_version(), _dotnet_version()
    assert py == proj == net, (
        "worker versions have drifted — the coordinator's single "
        "CBK_WORKER_CURRENT_VERSION would flag one flavour stale forever (ADR 29):\n"
        f"  config.py  AGENT_VERSION = {py}\n"
        f"  pyproject  version       = {proj}\n"
        f"  csproj     <Version>     = {net}")
