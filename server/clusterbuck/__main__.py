"""Entrypoint: `cbk-server` / `python -m clusterbuck`."""

from __future__ import annotations

import os


def main() -> None:
    import uvicorn

    uvicorn.run(
        "clusterbuck.api:app",
        host=os.environ.get("CBK_HOST", "127.0.0.1"),
        port=int(os.environ.get("CBK_PORT", "8000")),
        reload=False,
    )


if __name__ == "__main__":
    main()
