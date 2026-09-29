"""Publishing a worker release (release.py): one command, one record.

A release used to be three hand edits that named a version each and never checked each
other. These pin the replacement: `release.json` is the record, the version comes from the
build itself, and fitness and the join download follow the manifest unless overridden.
"""

from __future__ import annotations

import hashlib
import json
import zipfile

import pytest
from clusterbuck import release as rel
from clusterbuck.api import create_app
from fastapi.testclient import TestClient

BASE = "http://coordinator.test:8018/releases"


def _build(tmp_path, version: str, name: str | None = None):
    """A stand-in worker build: a zip whose config.py bakes AGENT_VERSION, as build.py does."""
    p = tmp_path / (name or f"built-{version}.pyz")
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("__main__.py", "")
        z.writestr("cbk_worker/config.py", f'AGENT_VERSION = "{version}"\n')
    return p


def _manifest(tmp_path, version="0.21.0"):
    d = tmp_path / "releases"
    d.mkdir(exist_ok=True)
    art = _build(d, version, f"cbk-{version}.pyz")
    path = d / "release.json"
    path.write_text(json.dumps({
        "version": version, "channel": "stable", "protocol_version": 1,
        "artifacts": {rel.RID: {"url": f"{BASE}/cbk-{version}.pyz",
                                "sha256": hashlib.sha256(art.read_bytes()).hexdigest()}},
    }))
    return path


def test_publish_points_the_manifest_at_the_new_build(tmp_path):
    path = _manifest(tmp_path)
    src = _build(tmp_path, "0.22.0")
    new = rel.publish(src, path)

    assert json.loads(path.read_text()) == new
    art = new["artifacts"][rel.RID]
    assert new["version"] == "0.22.0"
    assert art["url"] == f"{BASE}/cbk-0.22.0.pyz"        # base taken from the old manifest
    assert art["sha256"] == hashlib.sha256(src.read_bytes()).hexdigest()
    assert (path.parent / "cbk-0.22.0.pyz").read_bytes() == src.read_bytes()
    assert json.loads((path.parent / "release.json.bak-0.21.0").read_text())["version"] \
        == "0.21.0"


def test_the_version_comes_from_the_build_not_a_label(tmp_path):
    """A manifest that disagrees with the build's own AGENT_VERSION is an update loop."""
    path = _manifest(tmp_path)
    src = _build(tmp_path, "0.22.0", name="cbk-9.9.9.pyz")   # misleading filename
    assert rel.publish(src, path)["version"] == "0.22.0"


@pytest.mark.parametrize("version", ["0.21.0", "0.20.9", "0.9.0"])
def test_a_build_that_is_not_newer_is_refused_and_changes_nothing(tmp_path, version):
    path = _manifest(tmp_path)
    before = path.read_text()
    with pytest.raises(rel.ReleaseError, match="not newer"):
        rel.publish(_build(tmp_path, version), path)
    assert path.read_text() == before


def test_a_digest_that_does_not_match_is_refused(tmp_path):
    path = _manifest(tmp_path)
    with pytest.raises(rel.ReleaseError, match="digest mismatch"):
        rel.publish(_build(tmp_path, "0.22.0"), path, expected_sha256="0" * 64)
    assert rel.released_version(path) == "0.21.0"


def test_not_a_worker_build_is_refused(tmp_path):
    junk = tmp_path / "cbk.pyz"
    junk.write_bytes(b"not a zip")
    with pytest.raises(rel.ReleaseError, match="not a worker build"):
        rel.publish(junk, _manifest(tmp_path))


def test_a_first_release_needs_a_url_base(tmp_path):
    """No manifest to copy the address from, and guessing one is how loopback ships."""
    path = tmp_path / "releases" / "release.json"
    with pytest.raises(rel.ReleaseError, match="--url-base"):
        rel.publish(_build(tmp_path, "0.1.0"), path)
    new = rel.publish(_build(tmp_path, "0.1.0"), path, url_base=BASE + "/")
    assert new["artifacts"][rel.RID]["url"] == f"{BASE}/cbk-0.1.0.pyz"


def test_the_cli_publishes_and_reports(tmp_path, capsys):
    path = _manifest(tmp_path)
    assert rel.main(["publish", str(_build(tmp_path, "0.22.0")), "--release", str(path)]) == 0
    assert "published 0.22.0" in capsys.readouterr().out
    assert rel.main(["publish", str(_build(tmp_path, "0.22.0")), "--release", str(path)]) == 1


# --- the other two settings follow the manifest -------------------------------------


def test_fitness_follows_the_release_when_no_override_is_set(tmp_path):
    from clusterbuck.config import settings
    from clusterbuck.versions import assess, policy_from_settings

    path = _manifest(tmp_path, "0.22.0")
    policy = policy_from_settings(settings, str(path))
    assert policy.current == "0.22.0"
    assert assess(policy, agent_version="0.21.0", protocol_version=1).status == "stale"
    assert assess(policy, agent_version="0.22.0", protocol_version=1).status == "ok"


def test_a_joining_node_gets_the_released_build(tmp_path, redis_url):
    """Join and update now hand out the same bytes by construction, not by an operator
    remembering to copy the build to a second place."""
    path = _manifest(tmp_path, "0.22.0")
    fleet = tmp_path / "fleet.yaml"
    fleet.write_text("capabilities: {}\n")
    app = create_app(redis_url=redis_url, db_path=str(tmp_path / "r.db"),
                     fleet_path=str(fleet), start_scheduler=False,
                     join_password="a-sufficiently-long-join-password",
                     update_release=str(path),
                     broker_advertise_url="redis://:pw@192.168.1.10:6379/0")
    header = {"X-CBK-Join-Password": "a-sufficiently-long-join-password"}
    with TestClient(app) as c:
        served = c.get("/worker/artifact", headers=header)
        body = c.post("/nodes/bootstrap", headers=header).json()
    released = (path.parent / "cbk-0.22.0.pyz").read_bytes()
    assert served.status_code == 200 and served.content == released
    assert body["artifact_sha256"] == hashlib.sha256(released).hexdigest()
