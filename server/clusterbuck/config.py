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
    # Static registry seed for the sync plane (protocols.md §5).
    fleet_path: str = os.environ.get("CBK_FLEET_PATH", "fleet.yaml")
    # Opt-in cloud fallback for the sync plane; unset ⇒ sync plane is local-only.
    cloud_fallback_model: str | None = os.environ.get("CBK_CLOUD_FALLBACK_MODEL") or None

    # --- availability / wake (M2) ---
    # A consumer idle longer than this is treated as gone; must exceed the longest
    # inference (a busy worker isn't polling). Real heartbeats (M4) supersede this.
    worker_dead_ms: int = int(os.environ.get("CBK_WORKER_DEAD_MS", "60000"))
    # At most one wake per capability per this window (coalesce, don't stampede).
    wake_cooldown_s: float = float(os.environ.get("CBK_WAKE_COOLDOWN_S", "60"))
    # How often the escalation engine scans for due waitable jobs.
    escalation_interval_s: float = float(os.environ.get("CBK_ESCALATION_INTERVAL_S", "10"))
    wol_broadcast: str = os.environ.get("CBK_WOL_BROADCAST", "255.255.255.255")
    wol_port: int = int(os.environ.get("CBK_WOL_PORT", "9"))


settings = Settings()
