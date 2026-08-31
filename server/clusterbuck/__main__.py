"""Entrypoint: `cbk-server` / `python -m clusterbuck`."""

from __future__ import annotations

import os

# The coordinator's default port. 8018 rather than 8000: a commonly-scanned port that
# collides with half the dev tools on a LAN. Must stay in step with the worker's
# CBK_SERVER_URL default (worker/src/cbk_worker/config.py) and with install/ + docs — the
# docs moved to 8018 while these two defaults stayed on 8000, so anyone starting the
# coordinator by hand and following the quickstart curl'd a closed port.
DEFAULT_PORT = 8018


def main() -> None:
    import uvicorn

    uvicorn.run(
        "clusterbuck.api:app",
        host=os.environ.get("CBK_HOST", "127.0.0.1"),
        port=int(os.environ.get("CBK_PORT", str(DEFAULT_PORT))),
        reload=False,
    )


if __name__ == "__main__":
    main()
