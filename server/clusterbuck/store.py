"""SQLite job registry — the durable system of record (Redis stays purely the broker).

Tracks the id → result_key mapping and the last-known lifecycle state so GET /jobs/{id}
can answer even before (or without) a result blob in Redis.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    result_key  TEXT NOT NULL,
    capability  TEXT NOT NULL,
    status      TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
"""


class Store:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        with self._conn() as c:
            c.executescript(_SCHEMA)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def insert(
        self, *, id: str, result_key: str, capability: str, created_at: str
    ) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO jobs (id, result_key, capability, status, created_at) "
                "VALUES (?, ?, ?, 'queued', ?)",
                (id, result_key, capability, created_at),
            )

    def get(self, id: str) -> sqlite3.Row | None:
        with self._conn() as c:
            cur = c.execute("SELECT * FROM jobs WHERE id = ?", (id,))
            return cur.fetchone()

    def set_status(self, id: str, status: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, id))
