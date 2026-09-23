"""Entrypoint: `cbk-server` / `python -m clusterbuck`."""

from __future__ import annotations

import logging
import os

# The coordinator's default port. 8018 rather than 8000: a commonly-scanned port that
# collides with half the dev tools on a LAN. Must stay in step with the worker's
# CBK_SERVER_URL default (worker/src/cbk_worker/config.py) and with install/ + docs — the
# docs moved to 8018 while these two defaults stayed on 8000, so anyone starting the
# coordinator by hand and following the quickstart curl'd a closed port.
DEFAULT_PORT = 8018

DEFAULT_LOG_LEVEL = "INFO"


_HANDLER_MARK = "_cbk_console"


def configure_logging(level: str | None = None) -> None:
    """Give the coordinator's own loggers a handler, and nobody else's.

    Two separate settings, because they answer different questions.

    A handler goes on the ROOT logger, and root's own level is pinned to WARNING.
    Uvicorn configures handlers only for its own `uvicorn.*` loggers, so without this
    every `clusterbuck.*` record fell through to Python's `lastResort` handler — which
    emits WARNING and above and drops everything below. That silently hid all seventeen
    INFO call sites: wake decisions, reaper requeues, escalation promotions, the orphan
    and stale sweeps, every eval dispatch. Exactly the "what is the coordinator doing?"
    half of the log.

    The INFO level then goes on the `clusterbuck` logger alone, NOT on root. Fifteen
    third-party logger roots are live in this process — httpx, redis, sqlalchemy, openai
    and LiteLLM among them — and several narrate every request at INFO. Lowering root
    would bury the coordinator's own output in their traffic, so they keep root's WARNING
    while ours drops to INFO.

    Deliberately NOT `logging.basicConfig`: that is a no-op when root already has a
    handler, which is true under pytest and under some embeddings. It would silently skip
    both settings and leave the defect in place while appearing to fix it. Handler
    attachment here is explicit and idempotent — repeat calls replace our handler rather
    than stacking, and never touch anyone else's.

    Set `CBK_LOG_LEVEL` to override (DEBUG/INFO/WARNING/ERROR/CRITICAL). An unrecognised
    value falls back to INFO rather than raising: a typo in a unit file should not stop
    the coordinator from starting, and must not silently mute it either.

    This lives in the entrypoint on purpose. A library configuring logging steals a
    decision from whatever embeds it; `python -m clusterbuck` is the application, and the
    tests and e2e scripts deliberately never call it.
    """
    root = logging.getLogger()
    for existing in [h for h in root.handlers if getattr(h, _HANDLER_MARK, False)]:
        root.removeHandler(existing)
    handler = logging.StreamHandler()
    handler.setFormatter(  # systemd already stamps the time and the unit
        logging.Formatter("%(levelname)s [%(name)s] %(message)s"))
    setattr(handler, _HANDLER_MARK, True)
    root.addHandler(handler)
    root.setLevel(logging.WARNING)

    wanted = (level or os.environ.get("CBK_LOG_LEVEL") or DEFAULT_LOG_LEVEL).upper()
    if wanted not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        logging.getLogger("clusterbuck").warning(
            "CBK_LOG_LEVEL=%r is not a log level; using %s", wanted, DEFAULT_LOG_LEVEL)
        wanted = DEFAULT_LOG_LEVEL
    logging.getLogger("clusterbuck").setLevel(wanted)


def main() -> None:
    import sys

    import uvicorn

    from .config import ConfigError, settings, validate

    configure_logging()

    # Before the port is bound, not inside the app factory: a recovery-timing combination
    # that loses or duplicates jobs should stop the deployment here, where the operator is
    # still watching, rather than surface hours later as a job that ran twice. The app
    # factory stays unvalidated so tests can construct deliberately odd settings.
    try:
        validate(settings)
    except ConfigError as e:
        logging.getLogger("clusterbuck").error("refusing to start: %s", e)
        sys.exit(2)

    uvicorn.run(
        "clusterbuck.api:app",
        host=os.environ.get("CBK_HOST", "127.0.0.1"),
        port=int(os.environ.get("CBK_PORT", str(DEFAULT_PORT))),
        reload=False,
    )


if __name__ == "__main__":
    main()
