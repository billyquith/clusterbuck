"""Runtime configuration, from environment with sensible LAN-dev defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    redis_url: str = os.environ.get("CBK_REDIS_URL", "redis://localhost:6379/0")
    # SQLite is the durable system of record (ADR: Redis stays purely the broker).
    db_path: str = os.environ.get("CBK_DB_PATH", "cbk.db")
    # Result blobs live in Redis under result_key with a TTL (protocols.md §2).
    result_ttl_s: int = int(os.environ.get("CBK_RESULT_TTL_S", "86400"))
    # Shared consumer group name; workers for a capability share it so each job
    # is delivered to exactly one worker.
    consumer_group: str = os.environ.get("CBK_CONSUMER_GROUP", "cbk-workers")


settings = Settings()
