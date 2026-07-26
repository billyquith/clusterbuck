"""Shared test fixtures.

Redis: uses a reachable Redis (CBK_REDIS_URL, default localhost:6379) on an isolated
DB index, flushed per test. Tests skip cleanly if no Redis is reachable. (CI uses a
Redis service / Testcontainers; the contract is identical.)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# Keep any Wake-on-LAN broadcasts during tests on loopback (harmless), set before the
# clusterbuck package (and its frozen Settings) is first imported by a fixture.
os.environ.setdefault("CBK_WOL_BROADCAST", "127.0.0.1")

import pytest

CONTRACT_DIR = Path(__file__).resolve().parents[2] / "contract"
REDIS_URL_EXPLICIT = bool(os.environ.get("CBK_TEST_REDIS_URL"))
TEST_REDIS_URL = os.environ.get("CBK_TEST_REDIS_URL", "redis://localhost:6379/15")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


@pytest.fixture(scope="session")
def contract_dir() -> Path:
    return CONTRACT_DIR


@pytest.fixture()
def redis_url() -> str:
    """A flushed Redis DB for one test.

    If CBK_TEST_REDIS_URL was set explicitly (CI, or a developer who means it), an
    unreachable Redis is a **failure** — a suite that silently skips half its tests reports
    green for work it never did. Without the variable we fall back to a local default and
    skip, so `pytest` still works on a machine with no broker running.
    """
    import redis

    client = redis.from_url(TEST_REDIS_URL)
    try:
        client.ping()
    except redis.exceptions.ConnectionError as e:
        if REDIS_URL_EXPLICIT:
            pytest.fail(
                f"CBK_TEST_REDIS_URL is set to {TEST_REDIS_URL} but Redis is unreachable "
                f"({e}). Refusing to skip: an explicitly configured broker that is down is "
                f"a failure, not a reason to report green."
            )
        pytest.skip(
            f"no Redis reachable at the default {TEST_REDIS_URL} — start one with "
            f"`docker run -d --name cbk-redis -p 6379:6379 redis:7-alpine`, or set "
            f"CBK_TEST_REDIS_URL to make this a hard failure"
        )
    client.flushdb()
    client.close()
    return TEST_REDIS_URL


@pytest.fixture()
def client(redis_url, tmp_path):
    from fastapi.testclient import TestClient

    from clusterbuck.api import create_app

    app = create_app(
        redis_url=redis_url, db_path=str(tmp_path / "test.db"), start_scheduler=False
    )
    with TestClient(app) as c:
        yield c
