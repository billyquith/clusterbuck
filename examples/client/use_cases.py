"""Client use cases: what a client must be able to observe, one function per case.

Each case is a generic shape of work — an interactive turn, a one-off structured
extraction, a durable batch item, a patient job with a deadline — and asserts only what a
client can see over HTTP. `deploy/e2e/client.sh` brings up a coordinator, model servers
and workers to match the capabilities below, then runs each case.

    python use_cases.py --server URL [--api-key KEY] <case>

Exit status: 0 = the case holds; 2 = an expectation was violated (the coordinator does
not yet behave as a client needs); anything else = the harness itself broke. The e2e
script relies on that split, so a case that is expected to fail cannot pass by crashing.

The capabilities the harness provides:

    live-extract    a working model server, with a worker
    dead-extract    a closed port, with a worker (its model server is dead too)
    broken-extract  a model server that answers every completion with HTTP 500, with a worker
    idle-extract    a working model server, with NO worker — jobs wait
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import openai
from cbk_client import Client, ClusterbuckError, JobStore

MESSAGES = [{"role": "user", "content": "Extract the fields from this text."}]

# Structured output is asked for with a schema. JSON mode (`{"type": "json_object"}`) is
# not accepted by every model server — LM Studio refuses it — and a real schema gets
# better output than "any object" everywhere. Clusterbuck rewrites JSON mode into the
# schema form so older clients still work; new ones should send the schema.
RESPONSE_FORMAT = {"type": "json_schema", "json_schema": {"name": "fields", "schema": {
    "type": "object", "properties": {"fields": {"type": "object"}},
    "required": ["fields"]}}}


class Expectation(Exception):
    """The coordinator did something a client cannot work with."""


def expect(cond: bool, what: str) -> None:
    if not cond:
        raise Expectation(what)


def refused(fn, *args, **kwargs) -> ClusterbuckError:
    try:
        fn(*args, **kwargs)
    except ClusterbuckError as e:
        return e
    raise Expectation(f"{fn.__name__} succeeded; a refusal was expected")


def wait_terminal(c: Client, key: str, timeout_s: float) -> dict:
    record = c.poll(key, timeout_s=timeout_s)
    expect(record["state"] in {"done", "failed", "expired", "cancelled"},
           f"job still {record['state']!r} after {timeout_s}s")
    return record


# --- sync ---------------------------------------------------------------------------


def sync_ok(c: Client, _: argparse.Namespace) -> None:
    """An interactive turn through the stock OpenAI SDK, unmodified."""
    out = c.complete("live-extract", MESSAGES, temperature=0.2, max_tokens=100)
    expect(bool(out.choices[0].message.content), "empty completion")


def sync_unknown_alias(c: Client, _: argparse.Namespace) -> None:
    """A typo'd alias is the client's mistake, and must say so — not look like an outage."""
    try:
        c.openai.chat.completions.create(model="no-such-alias", messages=MESSAGES)
    except openai.APIStatusError as e:
        expect(e.status_code == 404, f"unknown alias gave HTTP {e.status_code}, not 404")
        # Read through the SDK itself: this is what a client actually has in hand.
        expect(e.code == "capability_not_found",
               f"SDK exposes code {e.code!r}, not 'capability_not_found'")
        return
    raise Expectation("an unknown alias was served")


def sync_dead_server(c: Client, _: argparse.Namespace) -> None:
    """The alias exists but its model server does not answer: retryable; async is an option."""
    e = refused(c.complete, "dead-extract", MESSAGES)
    expect(e.code == "model_server_unreachable",
           f"dead model server gave {e.code!r} (HTTP {e.status}), not model_server_unreachable")
    expect(e.retryable is True, "an unreachable model server should be marked retryable")
    # The fallback a client can take: queue it instead of failing the user outright.
    key = c.new_key()
    accepted = c.submit({"capability": "dead-extract", "messages": MESSAGES,
                         "urgency": "waitable", "privacy": "local_only"}, key)
    expect(accepted["state"] == "queued", f"fallback submit answered {accepted['state']!r}")


def sync_structured(c: Client, _: argparse.Namespace) -> None:
    """One-off structured extraction: choose an alias that is ready AND declares JSON output.

    Sync applies no `requires`, so the choice is the client's — and discovery has to
    carry enough to make it.
    """
    caps = c.fleet()["capabilities"]
    usable = [name for name, cap in caps.items()
              if (cap.get("features") or {}).get("json_schema")
              and (cap.get("health") or {}).get("state") == "ready"]
    expect(bool(usable), "/fleet offers no alias that is ready and declares json_schema")
    expect("dead-extract" not in usable, "/fleet offered a dead alias as ready")
    out = c.complete(usable[0], MESSAGES, response_format=RESPONSE_FORMAT)
    try:
        json.loads(out.choices[0].message.content)
    except ValueError as e:
        raise Expectation(f"structured output did not parse: {e}") from e


def discovery_health(c: Client, _: argparse.Namespace) -> None:
    """Discovery must not advertise a capability as usable when its server is dead."""
    deadline = time.monotonic() + 15
    while True:  # the first probe may not have run yet
        caps = c.fleet()["capabilities"]
        states = {n: (cap.get("health") or {}).get("state") for n, cap in caps.items()}
        if "unknown" not in states.values() or time.monotonic() > deadline:
            break
        time.sleep(0.5)
    expect(states.get("dead-extract") == "unreachable",
           f"/fleet reports dead-extract as {states.get('dead-extract')!r}")
    expect(states.get("live-extract") == "ready",
           f"/fleet reports live-extract as {states.get('live-extract')!r}")
    health = caps["dead-extract"]["health"]
    expect(health.get("checked_at") is not None, "no probe time on /fleet health")
    # And when a client sends anyway, the refusal says whether the async plane is worth
    # trying: here a worker is consuming that capability's queue.
    e = refused(c.complete, "dead-extract", MESSAGES)
    err = e.body.get("error") if isinstance(e.body, dict) else None
    expect(isinstance(err, dict) and err.get("use_async") is True,
           "the sync refusal does not say async could serve it")


# --- durable ------------------------------------------------------------------------


def durable_extraction(c: Client, _: argparse.Namespace) -> None:
    """A batch item: idempotent submit, a lost response retried, the result kept."""
    job = {"capability": "live-extract", "messages": MESSAGES,
           "requires": {"json_schema": True},
           "params": {"temperature": 0, "response_format": RESPONSE_FORMAT},
           "urgency": "waitable", "privacy": "local_only",
           "submitter": {"app": "reference-client"}}
    key = c.new_key()
    first = c.submit(job, key)
    # Pretend that response was lost in transit: the client only knows it *tried*.
    again = c.submit(job, key)
    expect(again["replayed"], "a same-key retry was not reported as a replay")
    expect(again["job_id"] == first["job_id"], "a same-key retry minted a second job")
    record = wait_terminal(c, key, 30)
    expect(record["state"] == "done", f"job ended {record['state']!r}: {record.get('error')}")
    stored = c.store.get(key)["result"]
    expect(stored is not None, "the result was not persisted on first terminal poll")
    json.loads(stored["choices"][0]["message"]["content"])


def unsatisfiable(c: Client, _: argparse.Namespace) -> None:
    """Two refusals with different fixes, told apart by code alone."""
    base = {"messages": MESSAGES, "urgency": "waitable", "privacy": "local_only"}
    e = refused(c.submit, {**base, "task_class": "extract", "min_ability": 10}, c.new_key())
    expect(e.code == "ability_unsatisfied", f"ability miss gave {e.code!r}")
    e = refused(c.submit, {**base, "capability": "live-extract",
                           "requires": {"vision": True}}, c.new_key())
    expect(e.code == "requirements_unsatisfied", f"requirement miss gave {e.code!r}")
    e = refused(c.submit, {**base, "capability": "no-such-alias"}, c.new_key())
    expect(e.code == "capability_not_found", f"unknown capability gave {e.code!r}")


def expire(c: Client, _: argparse.Namespace) -> None:
    """A patient job with a deadline nobody can meet ends `expired`, and says so."""
    soon = (datetime.now(UTC) + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    key = c.new_key()
    c.submit({"capability": "idle-extract", "messages": MESSAGES, "urgency": "waitable",
              "privacy": "local_only", "deadline": soon}, key)
    record = wait_terminal(c, key, 20)
    expect(record["state"] == "expired", f"job ended {record['state']!r}")
    expect(record.get("error_code") == "job_expired",
           f"expired job carries error_code {record.get('error_code')!r}")


def orphan(c: Client, args: argparse.Namespace) -> None:
    """A job whose queue entry is lost ends `job_orphaned`; the client reruns it, fresh key."""
    key = c.new_key()
    submitted = c.submit({"capability": "idle-extract", "messages": MESSAGES,
                          "urgency": "waitable", "privacy": "local_only"}, key)
    # HARNESS, not client: simulate the entry being trimmed away. Clients never touch Redis.
    _xdel_job(args.redis_url, "q:idle-extract", submitted["job_id"])
    record = wait_terminal(c, key, 30)
    expect(record["state"] == "failed", f"orphaned job ended {record['state']!r}")
    expect(record.get("error_code") == "job_orphaned",
           f"orphaned job carries error_code {record.get('error_code')!r}")
    _fresh, again = c.resubmit(key)
    expect(not again["replayed"] and again["job_id"] != submitted["job_id"],
           "a resubmit under a fresh key did not create a new job")


def worker_server_dead(c: Client, _: argparse.Namespace) -> None:
    """A worker took the job but could not reach its own model server."""
    key = c.new_key()
    c.submit({"capability": "dead-extract", "messages": MESSAGES, "urgency": "waitable",
              "privacy": "local_only"}, key)
    record = wait_terminal(c, key, 60)
    expect(record["state"] == "failed", f"job ended {record['state']!r}")
    expect(record.get("error_code") == "model_server_unreachable",
           f"failed job carries error_code {record.get('error_code')!r}")


def worker_server_error(c: Client, _: argparse.Namespace) -> None:
    """A worker reached its model server, which failed the request."""
    key = c.new_key()
    c.submit({"capability": "broken-extract", "messages": MESSAGES, "urgency": "waitable",
              "privacy": "local_only"}, key)
    record = wait_terminal(c, key, 30)
    expect(record["state"] == "failed", f"job ended {record['state']!r}")
    expect(record.get("error_code") == "model_server_error",
           f"failed job carries error_code {record.get('error_code')!r}")


def cancel(c: Client, _: argparse.Namespace) -> None:
    """A client that stops waiting withdraws its job, and the job says why it ended."""
    key = c.new_key()
    c.submit({"capability": "idle-extract", "messages": MESSAGES, "urgency": "waitable",
              "privacy": "local_only"}, key)
    record = c.cancel(key)
    expect(record["state"] == "cancelled", f"DELETE answered {record['state']!r}")
    record = wait_terminal(c, key, 5)
    expect(record.get("error_code") == "job_cancelled",
           f"cancelled job carries error_code {record.get('error_code')!r}")


def _xdel_job(redis_url: str, stream: str, job_id: str) -> None:
    import redis

    r = redis.Redis.from_url(redis_url)
    for entry_id, fields in r.xrange(stream):
        if json.loads(fields[b"job"])["id"] == job_id:
            r.xdel(stream, entry_id)
            return
    raise RuntimeError(f"no entry for {job_id} on {stream}")


CASES = {f.__name__.replace("_", "-"): f for f in (
    sync_ok, sync_unknown_alias, sync_dead_server, sync_structured, discovery_health,
    durable_extraction, unsatisfiable, expire, orphan, worker_server_dead,
    worker_server_error, cancel,
)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--api-key")
    ap.add_argument("--redis-url", default="redis://localhost:6379/0")
    ap.add_argument("case", choices=sorted(CASES))
    args = ap.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        client = Client(args.server, args.api_key, JobStore(Path(tmp) / "jobs.json"))
        try:
            CASES[args.case](client, args)
        except (Expectation, ClusterbuckError) as e:
            # A refusal nobody expected is as much a broken promise as a wrong code —
            # except a 401, which means the harness passed the wrong key.
            print(f"{args.case}: {e}", file=sys.stderr)
            return 1 if getattr(e, "status", None) == 401 else 2
    print(f"{args.case}: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
