"""Idempotent submit (protocols.md §1b) — making `POST /jobs` safe to retry.

`POST /jobs` minted a new job on every call, so a client that lost the response to a
submit could not tell "submitted" from "not submitted": retrying risked a duplicate job,
not retrying risked losing the work. An opt-in `Idempotency-Key` closes that, with the
unique index on `jobs.idempotency_key` as the arbiter.

Note what these deliberately do NOT assert: they never touch `submitter.request_id`, which
is documented in five places as identification only and is still never enforced. The two
are different jobs — one describes a retry, the other prevents one.
"""

from __future__ import annotations

import json

import redis
from clusterbuck.queue import stream_key

KEY = "req-4f9c1e70a2"


def _submit(client, *, key: str | None = None, **overrides):
    body = {
        "capability": "8b-extract",
        "messages": [{"role": "user", "content": "hello"}],
        "urgency": "waitable",
        "privacy": "local_only",
    }
    body.update(overrides)
    headers = {"Idempotency-Key": key} if key is not None else {}
    return client.post("/jobs", json=body, headers=headers)


def _stream_len(redis_url: str) -> int:
    conn = redis.from_url(redis_url, decode_responses=True)
    try:
        return len(conn.xrange(stream_key("8b-extract")))
    finally:
        conn.close()


# --- the point of the feature ------------------------------------------------------


def test_a_repeat_returns_the_same_job_and_enqueues_once(client, redis_url):
    first = _submit(client, key=KEY)
    second = _submit(client, key=KEY)

    assert first.status_code == 202
    assert second.status_code == 200, "nothing was accepted for processing this time"
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["result_key"] == first.json()["result_key"]
    assert _stream_len(redis_url) == 1, "the work must not be queued twice"


def test_only_the_replay_is_flagged_as_one(client):
    first = _submit(client, key=KEY)
    second = _submit(client, key=KEY)
    assert "Idempotency-Replayed" not in first.headers
    assert second.headers["Idempotency-Replayed"] == "true"


def test_a_replay_reports_the_jobs_current_status(client, redis_url):
    """The reply is not a canned "queued": a client retrying a submit long after the
    original may find the work already done, and needs to know that."""
    job_id = _submit(client, key=KEY).json()["id"]
    conn = redis.from_url(redis_url, decode_responses=True)
    conn.set(f"res_{job_id[4:]}", json.dumps({
        "job_id": job_id, "status": "done", "worker": "node-a",
        "completed_at": "2026-09-02T00:00:00Z",
        "completion": {"id": "c1", "choices": []},
    }))
    conn.close()
    client.get(f"/jobs/{job_id}")  # the poll is what settles the stored status

    assert _submit(client, key=KEY).json()["status"] == "done"


def test_concurrent_submits_under_one_key_produce_one_job(client, redis_url):
    """The case a check-then-write guard cannot hold, and the reason the unique index
    arbitrates instead. This is also the exact shape of the retry storm the feature
    exists to absorb."""
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: _submit(client, key=KEY), range(8)))

    codes = sorted(r.status_code for r in results)
    assert codes == [200] * 7 + [202], f"exactly one winner, got {codes}"
    assert len({r.json()["id"] for r in results}) == 1
    assert _stream_len(redis_url) == 1


# --- what it must NOT do ------------------------------------------------------------


def test_a_retry_repairs_a_row_committed_but_never_enqueued(client, redis_url):
    """The submit path writes the SQLite row before the XADD. If the coordinator (or
    Redis) dies in exactly that gap, the documented recovery — a same-key retry — used
    to hand back the same stranded `queued` job forever: nothing re-enqueues it, so it
    could only ever become `job_orphaned` once `CBK_ORPHAN_GRACE_S` passed, even though
    the retry carries the exact payload needed to finish the job properly. Simulated by
    inserting the row directly and never calling enqueue — the same state a crash in
    that gap leaves behind."""
    from clusterbuck.ids import new_ids

    job_id, result_key = new_ids()
    client.app.state.store.insert(
        id=job_id, result_key=result_key, capability="8b-extract",
        created_at="2026-09-02T00:00:00Z", urgency="waitable", idempotency_key=KEY,
    )
    assert client.app.state.store.get(job_id).entry_id is None
    assert _stream_len(redis_url) == 0

    retried = _submit(client, key=KEY)

    assert retried.status_code == 200
    assert retried.headers["Idempotency-Replayed"] == "true"
    assert retried.json()["id"] == job_id
    assert retried.json()["status"] == "queued"
    row = client.app.state.store.get(job_id)
    assert row.entry_id is not None, "the retry must actually deliver it this time"
    assert _stream_len(redis_url) == 1


def test_different_keys_are_different_jobs(client, redis_url):
    a = _submit(client, key="key-a")
    b = _submit(client, key="key-b")
    assert (a.status_code, b.status_code) == (202, 202)
    assert a.json()["id"] != b.json()["id"]
    assert _stream_len(redis_url) == 2


def test_no_key_keeps_the_old_behaviour_exactly(client, redis_url):
    """NULLs are distinct in SQLite, so unkeyed submits never collide with each other —
    which is what keeps every pre-existing client working unchanged."""
    a = _submit(client)
    b = _submit(client)
    assert (a.status_code, b.status_code) == (202, 202)
    assert a.json()["id"] != b.json()["id"]
    assert _stream_len(redis_url) == 2


def test_a_rejected_submit_does_not_burn_the_key(client, redis_url):
    """A 422 means no job exists, so the same key must still be usable. Otherwise a
    client that submitted before the fleet could serve its request would be permanently
    unable to retry it."""
    rejected = _submit(client, key=KEY, capability=None,
                       task_class="nothing-measured", min_ability=9)
    assert rejected.status_code == 422

    accepted = _submit(client, key=KEY)
    assert accepted.status_code == 202, "the key was not consumed by the failure"
    assert _stream_len(redis_url) == 1


def test_a_reused_key_with_a_different_payload_returns_the_first_job(client):
    """Documented behaviour, pinned so it cannot drift silently: the key is the client's
    promise that two requests are the same call. There is no request fingerprint (a
    canonicalised-body hash is a deliberate deferral), so the second payload is ignored
    rather than diagnosed."""
    first = _submit(client, key=KEY, messages=[{"role": "user", "content": "one"}])
    second = _submit(client, key=KEY, messages=[{"role": "user", "content": "different"}])
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]


# --- key validation ----------------------------------------------------------------


def test_an_empty_key_is_a_client_error(client):
    assert _submit(client, key="   ").status_code == 400


def test_an_oversized_key_is_a_client_error(client):
    assert _submit(client, key="k" * 256).status_code == 400


def test_a_non_printable_key_is_a_client_error(client):
    assert _submit(client, key="bad\nkey").status_code == 400


def test_a_key_is_trimmed_not_rejected(client):
    first = _submit(client, key=KEY)
    assert _submit(client, key=f"  {KEY}  ").json()["id"] == first.json()["id"]
