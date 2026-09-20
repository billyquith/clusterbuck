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
    # Orphan sweep: a job whose SQLite row exists but which was never enqueued (the
    # coordinator died between the insert and the XADD) is invisible to everything —
    # no worker will see it, and the reaper only walks the pending list. Always on,
    # because that is a crash artifact rather than a policy choice. Must exceed
    # reaper_min_idle_ms so the two recovery paths cannot race the same job.
    orphan_grace_s: int = int(os.environ.get("CBK_ORPHAN_GRACE_S", "900"))
    # Backstop for a properly-enqueued job nobody ever claims. UNSET means disabled, and
    # that is deliberate: on a fleet whose nodes sleep for days a patient `waitable` job
    # outliving any fixed cutoff is correct, so clusterbuck must not impose one. When
    # unset, the only bounds on a job are the ones its client set (`deadline`,
    # `escalate_after_min`) — which is what `expires_at: null` reports (protocols.md §1b).
    max_queue_age_s: int | None = (
        int(os.environ["CBK_MAX_QUEUE_AGE_S"])
        if os.environ.get("CBK_MAX_QUEUE_AGE_S")
        else None
    )
    # Worker bootstrap (`install/worker/join.py`). A joining machine has no operator key —
    # it presents this password once and receives a one-time join token plus the broker
    # URL. UNSET ⇒ the bootstrap routes 404, so no surface is added by default.
    #
    # This password guards the REDIS CREDENTIAL, so it is as strong as the broker. A value
    # shorter than JOIN_PASSWORD_MIN_LEN leaves the routes disabled rather than weakly
    # protected — fail closed on the feature, not on the service (see api.bootstrap_ready).
    join_password: str | None = os.environ.get("CBK_JOIN_PASSWORD") or None
    # Path to the built `cbk.pyz` the coordinator hands to joining workers, so every node
    # runs one blessed build rather than whatever its own checkout produced. Unset ⇒ 404.
    worker_artifact: str | None = os.environ.get("CBK_WORKER_ARTIFACT") or None
    # Urgency-tiered streams (ADR 34). `auto` tiers a capability only once every node
    # enrolled for it demonstrably reads the urgent stream (it says so in the `queues` it
    # heartbeats), because a worker that predates tiering reads only the base stream and an
    # urgent-tier write would strand the job. `on`/`off` force it either way.
    urgent_streams: str = os.environ.get("CBK_URGENT_STREAMS", "auto")
    # Reaper (ADR 20): a claimed entry idle longer than this is treated as abandoned and
    # requeued. MUST exceed the longest plausible inference, or a merely-busy worker's job
    # would be stolen and re-run. Default 10 min.
    reaper_min_idle_ms: int = int(os.environ.get("CBK_REAPER_MIN_IDLE_MS", "600000"))
    # Approximate cap on stream length, so the broker isn't a permanent log of every prompt.
    stream_maxlen: int = int(os.environ.get("CBK_STREAM_MAXLEN", "10000"))
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
    # Reservations: wake + pre-load this long before a window opens (ADR 17).
    warm_lead_s: int = int(os.environ.get("CBK_WARM_LEAD_S", "300"))
    # Shared-secret for the coordinator API (DESIGN.md → Security). Unset ⇒ auth disabled
    # (dev default, warned at startup).
    api_key: str | None = os.environ.get("CBK_API_KEY") or None
    # Worker version governance (ADR 27). All unset ⇒ every worker is judged `ok`, so a
    # fleet works before an operator has opinions. `blocked` is a comma-separated list of
    # exact versions known to be broken — bugs are not monotonic, so a floor alone cannot
    # express "1.4.2 is bad but 1.4.1 and 1.4.3 are fine".
    worker_current_version: str | None = os.environ.get("CBK_WORKER_CURRENT_VERSION") or None
    worker_min_version: str | None = os.environ.get("CBK_WORKER_MIN_VERSION") or None
    worker_blocked_versions: str | None = os.environ.get("CBK_WORKER_BLOCKED_VERSIONS") or None
    # Self-update (ADR 13). Both unset ⇒ /updates/manifest returns 404 (no update channel).
    # The signing key is operator-held and must live OUTSIDE the repo (it's RCE if leaked).
    update_signing_key: str | None = os.environ.get("CBK_UPDATE_SIGNING_KEY") or None
    update_release: str | None = os.environ.get("CBK_UPDATE_RELEASE") or None
    # Monthly cloud budget cap (USD). Unset ⇒ no cap: shown as null on /usage and never
    # gates routing (budget.py). Set ⇒ actually enforced (ADR 30), not just displayed.
    cloud_budget_monthly: float | None = (
        float(os.environ["CBK_CLOUD_BUDGET_MONTHLY"])
        if os.environ.get("CBK_CLOUD_BUDGET_MONTHLY")
        else None
    )
    # Fraction of the monthly cap held back for `urgent` jobs only (budget.py). The rest
    # (the "paced pool") is what `necessary` jobs may draw from, spread across the month
    # so week one can't burn it — model-evaluation.md's "soft daily allowance".
    cloud_budget_reserve_fraction: float = float(
        os.environ.get("CBK_CLOUD_BUDGET_RESERVE_FRACTION", "0.2")
    )


settings = Settings()


# Minimum length for CBK_JOIN_PASSWORD. It stands in front of the broker credential, so a
# guessable value is equivalent to publishing that credential on the LAN.
JOIN_PASSWORD_MIN_LEN = 16
