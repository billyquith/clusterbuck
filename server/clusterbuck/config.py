"""Runtime configuration, from environment with sensible LAN-dev defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    redis_url: str = os.environ.get("CBK_REDIS_URL", "redis://localhost:6379/0")
    # SQLite is the durable system of record (ADR: Redis stays purely the broker).
    #
    # The default is RELATIVE, so it resolves against the working directory — the same
    # footgun `fleet_path` is already warned about at its own use site, and a worse one
    # here. A unit file with a different `WorkingDirectory`, or a `cd` before a manual
    # start, silently opens a DIFFERENT database: the coordinator comes up healthy with
    # an empty schema, no enrolled nodes and no ability matrix, which reads as "the
    # fleet forgot everything" rather than "you are looking at the wrong file". The
    # startup log states the resolved absolute path for that reason — set CBK_DB_PATH to
    # an absolute path on any real deployment.
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
    # reaper_min_idle_ms so the two recovery paths cannot race the same job — asserted by
    # `validate()` below, having previously held at the defaults only by coincidence.
    orphan_grace_s: int = int(os.environ.get("CBK_ORPHAN_GRACE_S", "3600"))
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
    # The broker address handed to JOINING WORKERS, which is not always the one the
    # coordinator uses itself: Redis usually runs on the coordinator box, so its own
    # CBK_REDIS_URL is loopback — and handing that to a remote worker points it at its own
    # localhost, where it finds nothing. Unset ⇒ falls back to CBK_REDIS_URL, and bootstrap
    # refuses to advertise a loopback address rather than serving one that cannot work.
    broker_advertise_url: str | None = (
        os.environ.get("CBK_BROKER_ADVERTISE_URL") or None
    )
    # Path to the built `cbk.pyz` the coordinator hands to joining workers, so every node
    # runs one blessed build rather than whatever its own checkout produced. Unset ⇒ 404.
    worker_artifact: str | None = os.environ.get("CBK_WORKER_ARTIFACT") or None
    # Urgency-tiered streams (ADR 34). `auto` tiers a capability only once every node
    # CURRENTLY REPORTING QUEUES for it demonstrably reads the urgent stream (it says so
    # in the `queues` it heartbeats), because a worker that predates tiering reads only
    # the base stream and an urgent-tier write would strand the job. A node reporting no
    # queues abstains rather than vetoing — it is paused or cut off, which says nothing
    # about what it could read on resuming — and if none is reporting any the gate stays
    # shut. `on`/`off` force it either way.
    urgent_streams: str = os.environ.get("CBK_URGENT_STREAMS", "auto")
    # Reaper (ADR 20): a claimed entry idle longer than this is treated as abandoned and
    # requeued. MUST exceed the longest plausible inference, or a merely-busy worker's job
    # would be stolen and re-run.
    #
    # It used to default to 600_000 — exactly the worker's own inference timeout, which was
    # itself hardcoded. "Must exceed" was therefore false at the defaults: a generation that
    # ran the full ten minutes was reclaimed at the same instant the worker gave up on it,
    # so the fleet paid twice for one answer and the two racing writers made the result a
    # coin toss. The pair is now explicit at both ends:
    #
    #     worker CBK_INFERENCE_TIMEOUT_S  <  CBK_REAPER_MIN_IDLE_MS  <  CBK_ORPHAN_GRACE_S
    #
    # 30 min gives 3x headroom over the worker's 600 s default, which is room for a long
    # generation on a slow node without leaving a genuinely dead worker's job sitting for
    # an hour. Raise the worker's timeout and this must move with it — `validate()` below
    # refuses the startup rather than letting the invariant quietly lapse again.
    reaper_min_idle_ms: int = int(os.environ.get("CBK_REAPER_MIN_IDLE_MS", "1800000"))
    # Approximate cap on stream length, so the broker isn't a permanent log of every prompt.
    stream_maxlen: int = int(os.environ.get("CBK_STREAM_MAXLEN", "10000"))
    # Static registry seed for the sync plane (protocols.md §5).
    fleet_path: str = os.environ.get("CBK_FLEET_PATH", "fleet.yaml")
    # Opt-in cloud fallback for the sync plane; unset ⇒ sync plane is local-only.
    cloud_fallback_model: str | None = os.environ.get("CBK_CLOUD_FALLBACK_MODEL") or None
    # How long the sync plane waits on a model server for one completion, before
    # LiteLLM's own default of 6000s does. A person is waiting on this call — the whole
    # reason a sync request exists rather than a job — so a hung upstream should read as
    # `model_server_timeout` (§1c) well before a browser tab, or the client's own
    # timeout, gives up first with nothing to act on.
    sync_timeout_s: float = float(os.environ.get("CBK_SYNC_TIMEOUT_S", "120"))

    # --- availability / wake (M2) ---
    # A consumer idle longer than this is treated as gone; must exceed the longest
    # inference (a busy worker isn't polling). Real heartbeats (M4) supersede this.
    worker_dead_ms: int = int(os.environ.get("CBK_WORKER_DEAD_MS", "60000"))
    # A registry row is a DECLARATION, not a liveness signal: `mode` is what the node last
    # said about itself, and nothing ever expires it — a machine powered off months ago
    # still reads `active`. A node silent for longer than this is shown as stale. The
    # worker heartbeats every 10s from its OWN task (never blocked by a long inference,
    # see worker commands.py `_heartbeat_loop`), so this tolerates six consecutive misses
    # before saying anything — a missed heartbeat is transient, sustained silence is not.
    # Called "silent", not "stale": ADR 27 already spends that word on agent-version drift,
    # and the nodes table renders both pills one column apart.
    node_silent_s: float = float(os.environ.get("CBK_NODE_SILENT_S", "60"))
    # At most one wake per capability per this window (coalesce, don't stampede).
    wake_cooldown_s: float = float(os.environ.get("CBK_WAKE_COOLDOWN_S", "60"))
    # How often the escalation engine scans for due waitable jobs.
    escalation_interval_s: float = float(os.environ.get("CBK_ESCALATION_INTERVAL_S", "10"))
    # How often the coordinator probes each capability's model server for `/fleet`'s
    # `health` (model_health.py). It is the sync path's own reachability, which no
    # heartbeat can report. Rounded to whole coordinator ticks, and never faster than one.
    model_probe_s: float = float(os.environ.get("CBK_MODEL_PROBE_S", "30"))
    wol_broadcast: str = os.environ.get("CBK_WOL_BROADCAST", "255.255.255.255")
    wol_port: int = int(os.environ.get("CBK_WOL_PORT", "9"))
    # Reservations: how long before a window opens to wake and pre-load (ADR 17).
    # UNSET is the normal case and does NOT mean "no lead" — it means the lead is derived
    # from each node's MEASURED cold-load time (`stats.load_s`), which is the whole point
    # of measuring it. Defaulting this to 300 made it indistinguishable from an operator
    # explicitly choosing 300, so the measured path could never be reached: the only
    # caller always passed the constant. Set it only to override the measurement.
    warm_lead_s: int | None = (
        int(os.environ["CBK_WARM_LEAD_S"]) if os.environ.get("CBK_WARM_LEAD_S") else None)
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


class ConfigError(ValueError):
    """A setting combination that would silently corrupt job handling."""


def validate(s: Settings) -> None:
    """Refuse to start on a recovery-timing combination that loses or duplicates work.

    These three are a chain, and every link was previously stated in a comment and checked
    by nobody. Both had already lapsed: `reaper_min_idle_ms` EQUALLED the worker's
    inference timeout rather than exceeding it, and `orphan_grace_s > reaper_min_idle_ms`
    held only because nobody had moved either number. A comment is not an assertion, so
    these are assertions.

    Deliberately a hard failure, not a warning. Both violations are silent in production —
    they show up as jobs that ran twice or answers that flipped, hours later and nowhere
    near the setting that caused them — which is exactly the class of thing that should
    cost a restart now rather than a day of debugging later.
    """
    if s.orphan_grace_s * 1000 <= s.reaper_min_idle_ms:
        raise ConfigError(
            f"CBK_ORPHAN_GRACE_S ({s.orphan_grace_s}s) must exceed "
            f"CBK_REAPER_MIN_IDLE_MS ({s.reaper_min_idle_ms}ms = "
            f"{s.reaper_min_idle_ms / 1000:g}s), or the orphan sweep and the reaper race "
            f"the same job: the sweep can terminalise work the reaper is about to requeue."
        )
    if s.max_queue_age_s is not None and s.max_queue_age_s * 1000 <= s.reaper_min_idle_ms:
        raise ConfigError(
            f"CBK_MAX_QUEUE_AGE_S ({s.max_queue_age_s}s) must exceed "
            f"CBK_REAPER_MIN_IDLE_MS ({s.reaper_min_idle_ms / 1000:g}s), or a job is aged "
            f"out while its first delivery is still within the reaper's patience."
        )


settings = Settings()


# Minimum length for CBK_JOIN_PASSWORD. It stands in front of the broker credential, so a
# guessable value is equivalent to publishing that credential on the LAN.
JOIN_PASSWORD_MIN_LEN = 16
