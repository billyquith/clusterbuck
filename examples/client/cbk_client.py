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
  2. A transport failure on submit is ambiguous (the job may exist): retry with the
     **same** key. The coordinator answers a repeat with the existing job.
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
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import openai

TERMINAL = {"done", "failed", "expired", "cancelled"}

# The codes this client reacts to. A real client keeps a small map like this rather than
# the whole table: most codes only need to be shown to a person, not acted on.
RESUBMIT_WITH_FRESH_KEY = {"job_orphaned", "job_abandoned", "model_server_unreachable",
                           "model_server_timeout", "model_server_error"}


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
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def _load(self) -> dict[str, dict]:
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    def put(self, key: str, **fields: Any) -> dict:
        records = self._load()
        records.setdefault(key, {}).update(fields)
        self.path.write_text(json.dumps(records, indent=2))
        return records[key]

    def get(self, key: str) -> dict | None:
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
            accepted = self._json(resp)
            return self.store.put(key, job_id=accepted["id"], result_key=accepted["result_key"],
                                  state=accepted["status"],
                                  replayed=resp.headers.get("Idempotency-Replayed") == "true")
        raise AssertionError("unreachable")

    def poll(self, key: str, *, timeout_s: float = 30, interval_s: float = 0.25) -> dict:
        """Poll a stored job to a terminal status and persist what it ended with."""
        job_id = self.store.get(key)["job_id"]
        deadline = time.monotonic() + timeout_s
        while True:
            view = self._json(self.http.get(f"/jobs/{job_id}"))
            if view["status"] in TERMINAL:
                # Persist on first sight: the coordinator keeps a completion only for
                # CBK_RESULT_TTL_S, and keeps no copy of it anywhere durable.
                return self.store.put(key, state=view["status"], result=view.get("result"),
                                      error=view.get("error"),
                                      error_code=view.get("error_code"))
            if time.monotonic() > deadline:
                return self.store.put(key, state=view["status"])
            time.sleep(interval_s)

    def cancel(self, key: str) -> dict:
        job_id = self.store.get(key)["job_id"]
        view = self._json(self.http.delete(f"/jobs/{job_id}"))
        return self.store.put(key, state=view["status"])

    def resubmit(self, key: str) -> tuple[str, dict]:
        """Run a finished call's work again. A fresh key — the old one is spent."""
        record = self.store.get(key)
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
