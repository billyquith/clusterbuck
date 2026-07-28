"""Shared fixtures. Mirrors the coordinator's Redis policy deliberately: an explicitly
configured broker that is down is a failure, not a reason to report green."""

from __future__ import annotations

import os

import pytest

REDIS_URL_EXPLICIT = bool(os.environ.get("CBK_TEST_REDIS_URL"))
TEST_REDIS_URL = os.environ.get("CBK_TEST_REDIS_URL", "redis://localhost:6379/14")


@pytest.fixture()
async def redis_client():
    """A flushed Redis DB for one test, as an async client.

    If CBK_TEST_REDIS_URL was set explicitly (CI, or a developer who means it), an unreachable
    Redis is a **failure** — a suite that silently skips half its tests reports green for work
    it never did. Without the variable we fall back to a local default and skip, so `pytest`
    still works on a machine with no broker running.
    """
    import redis.asyncio as aioredis
    from redis.exceptions import ConnectionError as RedisConnectionError

    client = aioredis.from_url(TEST_REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except (RedisConnectionError, OSError) as e:
        await client.aclose()
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
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()
