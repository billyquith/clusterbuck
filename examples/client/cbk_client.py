"""A reference clusterbuck client: the lifecycle every client needs, written out once.

This is not an SDK. It is a readable account of the behaviour protocols.md §1 asks of a
client, kept runnable so it cannot drift from what the coordinator actually does — the
end-to-end suite (`deploy/e2e/client.sh`) drives it. Copy the shape, not the file.

Two paths, chosen per request by whether a person is waiting:

- **Sync** targets a ready capability. `model` is a capability alias, which binds a
  deployment as it stands now; nothing is queued, woken or retried for you.
- **Durable** (`POST /jobs`) declares an intended outcome and lets the coordinator route,
  wait and wake. The rules that make it safe are the whole point of this file:

  1. Mint one Idempotency-Key per *logical* call, and persist it with the job before
     doing anything else.
  2. A transport failure OR a 5xx on submit is ambiguous (the job may exist either way):
     retry with the **same** key. The coordinator answers a repeat with the existing job.
  3. Poll until a terminal status, and persist the result the first time you see it —
     completions expire from the coordinator after `CBK_RESULT_TTL_S`.
  4. To run the work *again* after any terminal status, use a **fresh** key. A key has no
     TTL, so the old one returns the old terminal job forever.

Failures are told apart by `error.code` (HTTP errors) or `error_code` (a job's terminal
state) — never by status alone, never by message text.

Dependencies: `httpx` and the stock `openai` SDK.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import openai

TERMINAL = {"done", "failed", "expired", "cancelled"}


@dataclass
class ClusterbuckError(Exception):
    """A refusal from the coordinator, reduced to the fields a client branches on."""

    status: int
    code: str | None
    message: str
    retryable: bool | None = None
    body: Any = None

    def __str__(self) -> str:
        return f"HTTP {self.status} {self.code or '<no code>'}: {self.message}"


def error_from_body(status: int, body: Any) -> ClusterbuckError:
    """Read the error envelope. Tolerates a body that has none, so a caller can see that."""
    err = body.get("error") if isinstance(body, dict) else None
    if isinstance(err, dict):
        return ClusterbuckError(status, err.get("code"), str(err.get("message", "")),
                                err.get("retryable"), body)
    detail = body.get("detail") if isinstance(body, dict) else body
    return ClusterbuckError(status, None, str(detail), None, body)


class JobStore:
    """Where a client keeps what it must not lose: a JSON file, keyed by idempotency key.

    Stands in for whatever durable store the client already has. What matters is *when*
    it is written — before the submit returns, and again on the first terminal poll.

    Writes are atomic (temp file + `os.replace`), not `Path.write_text` in place: a crash
    or a kill signal mid-write used to leave a half-written file, and every record in it
    — not just the one being updated — was lost, since the next read raised on the
    truncated JSON. `os.replace` is atomic on both POSIX and Windows, so a reader never
    observes a partial file. A lock serialises concurrent callers IN THIS PROCESS
    (multiple threads sharing one `JobStore`); it does not extend across processes, which
    would need a file lock instead — matching the one-process shape of this example.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()

    def _load(self) -> dict[str, dict]:
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    def put(self, key: str, **fields: Any) -> dict:
        with self._lock:
            records = self._load()
            records.setdefault(key, {}).update(fields)
            fd, tmp_name = tempfile.mkstemp(
                dir=self.path.parent or ".", prefix=f".{self.path.name}.", suffix=".tmp")
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(records, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_name, self.path)
            except BaseException:
                Path(tmp_name).unlink(missing_ok=True)
                raise
            return records[key]

    def get(self, key: str) -> dict | None:
        with self._lock:
            return self._load().get(key)


class Client:
    def __init__(self, base_url: str, api_key: str | None, store: JobStore) -> None:
        self.base_url = base_url.rstrip("/")
        self.store = store
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.http = httpx.Client(base_url=self.base_url, headers=headers, timeout=30)
        # The stock SDK, unmodified: the coordinator accepts its Bearer header, and an
        # error envelope in OpenAI's shape surfaces as `.code` on the raised exception.
        self.openai = openai.OpenAI(base_url=f"{self.base_url}/v1",
                                    api_key=api_key or "unused", max_retries=0)

    # --- discovery ------------------------------------------------------------

    def fleet(self) -> dict:
        return self._json(self.http.get("/fleet"))

    # --- sync -----------------------------------------------------------------

    def complete(self, model: str, messages: list[dict], **params: Any):
        """One interactive completion. Raises ClusterbuckError on any refusal."""
        try:
            return self.openai.chat.completions.create(model=model, messages=messages,
                                                       **params)
        except openai.APIStatusError as e:
            raise error_from_body(e.status_code, _body_of(e.response)) from e

    # --- durable --------------------------------------------------------------

    def new_key(self) -> str:
        return f"call-{uuid.uuid4()}"

    def submit(self, job: dict, key: str, *, attempts: int = 3) -> dict:
        """Submit one logical call. Safe to call again with the same key after a crash."""
        self.store.put(key, request=job, state="submitting")
        for attempt in range(attempts):
            try:
                resp = self.http.post("/jobs", json=job, headers={"Idempotency-Key": key})
            except httpx.TransportError:
                # Ambiguous: the job may or may not exist. The same key makes the retry
                # safe — a repeat returns the existing job and enqueues nothing.
                if attempt == attempts - 1:
                    raise
                time.sleep(0.5 * (attempt + 1))
                continue
            try:
                accepted = self._json(resp)
            except ClusterbuckError as e:
                # A 5xx is exactly as ambiguous as a dropped connection: the row may
                # already be committed even though THIS response says the request
                # failed (Redis timed out after the write, the connection was reset
                # while the reply was in flight). The same key makes retrying safe
                # either way — a repeat returns the existing job rather than a second
                # one. A 4xx is not ambiguous (nothing was accepted) and must not be
                # retried here — see `error_from_body`.
                if e.status >= 500 and attempt < attempts - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise
            return self.store.put(key, job_id=accepted["id"], result_key=accepted["result_key"],
                                  state=accepted["status"],
                                  replayed=resp.headers.get("Idempotency-Replayed") == "true")
        raise AssertionError("unreachable")

    def _job_id(self, key: str) -> str:
        """The job id stored against a key, or a clear error naming exactly what to do —
        never the bare `KeyError`/`TypeError` a missing or half-written record used to
        raise from indexing `None` or a dict with no `job_id` yet."""
        record = self.store.get(key)
        if record is None:
            raise KeyError(f"no stored record for key {key!r} — was submit() ever called "
                           f"with this key?")
        job_id = record.get("job_id")
        if job_id is None:
            raise RuntimeError(
                f"key {key!r} has no job_id yet: submit() started but this client never "
                f"saw a response (a crash, or a kill, between the request and reading "
                f"it) — call submit() again with this SAME key to find out what actually "
                f"happened; the coordinator answers a repeat with the existing job."
            )
        return job_id

    def poll(self, key: str, *, timeout_s: float = 30, interval_s: float = 0.25) -> dict:
        """Poll a stored job to a terminal status and persist what it ended with."""
        job_id = self._job_id(key)
        deadline = time.monotonic() + timeout_s
        while True:
            view = self._json(self.http.get(f"/jobs/{job_id}"))
            if view["status"] in TERMINAL:
                # Persist on first sight: the coordinator keeps a completion only for
                # CBK_RESULT_TTL_S, and keeps no copy of it anywhere durable.
                return self.store.put(key, state=view["status"], result=view.get("result"),
                                      error=view.get("error"),
                                      error_code=view.get("error_code"),
                                      # From the job view directly — the SAME table
                                      # §1c uses for HTTP errors — rather than this
                                      # client keeping its own copy of which codes are
                                      # worth resubmitting and letting it drift.
                                      retryable=view.get("retryable"))
            if time.monotonic() > deadline:
                return self.store.put(key, state=view["status"])
            time.sleep(interval_s)

    def cancel(self, key: str) -> dict:
        job_id = self._job_id(key)
        view = self._json(self.http.delete(f"/jobs/{job_id}"))
        return self.store.put(key, state=view["status"])

    def resubmit(self, key: str) -> tuple[str, dict]:
        """Run a finished call's work again. A fresh key — the old one is spent."""
        record = self.store.get(key)
        if record is None:
            raise KeyError(f"no stored record for key {key!r} — was submit() ever called "
                           f"with this key?")
        fresh = self.new_key()
        self.store.put(key, superseded_by=fresh)
        return fresh, self.submit(record["request"], fresh)

    # --- plumbing -------------------------------------------------------------

    def _json(self, resp: httpx.Response) -> dict:
        body = _body_of(resp)
        if resp.status_code >= 400:
            raise error_from_body(resp.status_code, body)
        return body


def _body_of(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return resp.text
