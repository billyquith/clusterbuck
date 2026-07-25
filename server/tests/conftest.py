"""Shared test fixtures.

Redis: uses a reachable Redis (CBK_REDIS_URL, default localhost:6379) on an isolated
DB index, flushed per test. Tests skip cleanly if no Redis is reachable. (CI uses a
Redis service / Testcontainers; the contract is identical.)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

CONTRACT_DIR = Path(__file__).resolve().parents[2] / "contract"
TEST_REDIS_URL = os.environ.get("CBK_TEST_REDIS_URL", "redis://localhost:6379/15")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


@pytest.fixture(scope="session")
def contract_dir() -> Path:
    return CONTRACT_DIR


@pytest.fixture()
def redis_url() -> str:
    import redis

    client = redis.from_url(TEST_REDIS_URL)
    try:
        client.ping()
    except redis.exceptions.ConnectionError:
        pytest.skip(f"no Redis reachable at {TEST_REDIS_URL}")
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
