"""Submit/poll API behaviour (protocols.md §1b), against a real Redis.

The worker is exercised separately (C# side + the end-to-end script); here we inject the
result blob the way a worker would, to prove the server half of the loop in isolation.
"""

from __future__ import annotations

import json

import redis

from clusterbuck.queue import stream_key


def _submit(client, **overrides):
    body = {
        "capability": "8b-extract",
        "messages": [{"role": "user", "content": "hello"}],
        "urgency": "waitable",
        "privacy": "local_only",
    }
    body.update(overrides)
    return client.post("/jobs", json=body)


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_submit_returns_202_and_enqueues(client, redis_url):
    r = _submit(client)
    assert r.status_code == 202
    payload = r.json()
    assert payload["status"] == "queued"
    assert payload["id"].startswith("job_")
    assert payload["result_key"].startswith("res_")

    # The job landed on the capability stream as a single entry.
    conn = redis.from_url(redis_url, decode_responses=True)
    entries = conn.xrange(stream_key("8b-extract"))
    conn.close()
    assert len(entries) == 1
    job = json.loads(entries[0][1]["job"])
    assert job["id"] == payload["id"]
    assert job["capability"] == "8b-extract"
    assert job["privacy"] == "local_only"


def test_need_shaped_addressing_resolves_capability(client, redis_url):
    r = _submit(client, capability=None, task_class="summarize", min_ability=6)
    assert r.status_code == 202
    conn = redis.from_url(redis_url, decode_responses=True)
    entries = conn.xrange(stream_key("32b-reason"))  # min_ability 6 → 32b-reason
    conn.close()
    assert len(entries) == 1


def test_unknown_capability_rejected_not_parked(client, redis_url):
    """An unknown capability must fail at submit time (422), not queue forever on a
    stream no worker will ever consume."""
    r = _submit(client, capability="70b-genius")
    assert r.status_code == 422
    assert "unknown capability" in r.json()["detail"]

    conn = redis.from_url(redis_url, decode_responses=True)
    entries = conn.xrange(stream_key("70b-genius"))
    conn.close()
    assert entries == []


def test_poll_queued_then_done(client, redis_url):
    submit = _submit(client).json()
    job_id, result_key = submit["id"], submit["result_key"]

    # Before a worker runs: queued, no result.
    got = client.get(f"/jobs/{job_id}").json()
    assert got["status"] == "queued"
    assert got["result"] is None

    # Simulate a worker writing the terminal result blob.
    conn = redis.from_url(redis_url, decode_responses=True)
    conn.set(
        result_key,
        json.dumps(
            {
                "job_id": job_id,
                "status": "done",
                "worker": "node-test",
                "completed_at": "2026-07-24T18:30:04Z",
                "completion": {
                    "choices": [
                        {"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "hi there"}}
                    ]
                },
                "error": None,
            }
        ),
    )
    conn.close()

    got = client.get(f"/jobs/{job_id}").json()
    assert got["status"] == "done"
    assert got["worker"] == "node-test"
    assert got["result"]["choices"][0]["message"]["content"] == "hi there"


def test_unknown_job_404(client):
    assert client.get("/jobs/job_does_not_exist").status_code == 404


def test_bad_submit_rejected(client):
    # Neither messages nor prompt.
    r = client.post("/jobs", json={"capability": "8b-extract", "urgency": "waitable"})
    assert r.status_code == 422
    # No addressing at all.
    r = client.post("/jobs", json={"messages": [{"role": "user", "content": "x"}]})
    assert r.status_code == 422


# --- caller provenance (protocols.md §1b) -------------------------------------------


_SUBMITTER = {
    "app": "nightly-importer",
    "instance": "workstation-2",
    "request_id": "req_4f9c1e70a2",
    "submitted_at": "2026-09-02T00:29:59Z",
}


def test_submitter_reaches_the_queue_payload(client, redis_url):
    """Provenance must ride the wire record, not just the coordinator DB — it is part of
    the job the worker receives."""
    r = _submit(client, submitter=_SUBMITTER)
    assert r.status_code == 202
    conn = redis.from_url(redis_url, decode_responses=True)
    entries = conn.xrange(stream_key("8b-extract"))
    conn.close()
    job = json.loads(entries[0][1]["job"])
    assert job["submitter"] == _SUBMITTER


def test_submitter_is_readable_back_and_ip_is_stamped(client):
    job_id = _submit(client, submitter=_SUBMITTER).json()["id"]
    got = client.get(f"/jobs/{job_id}").json()["submitter"]
    for key, value in _SUBMITTER.items():
        assert got[key] == value
    # Stamped server-side, so it is present even though the client never sent it.
    assert got["observed_ip"]


def test_submitter_rejects_unknown_keys(client):
    r = _submit(client, submitter={**_SUBMITTER, "observed_ip": "10.0.0.9"})
    assert r.status_code == 422, "observed_ip is server-stamped; the body must not set it"


def test_submitter_is_optional_and_client_identity_reads_as_null(client, redis_url):
    """Every pre-existing client submits without provenance and must keep working."""
    job_id = _submit(client).json()["id"]
    conn = redis.from_url(redis_url, decode_responses=True)
    job = json.loads(conn.xrange(stream_key("8b-extract"))[0][1]["job"])
    conn.close()
    assert "submitter" not in job
    got = client.get(f"/jobs/{job_id}").json()
    # An IP is always observed, so provenance is present but carries no client identity.
    assert got["submitter"]["app"] is None
    assert got["submitter"]["request_id"] is None


def test_repeated_request_id_is_recorded_not_rejected(client):
    """The duplicate-vs-repeat distinction only works if the coordinator accepts both:
    two jobs sharing a request_id (one call retried) and two with different ones
    (genuinely repeated work). Neither is an error."""
    shared = {"app": "importer", "instance": "host-1", "request_id": "req_same"}
    retried = [_submit(client, submitter=shared) for _ in range(2)]
    assert [r.status_code for r in retried] == [202, 202]
    ids = [r.json()["id"] for r in retried]
    assert ids[0] != ids[1], "distinct jobs, even though one logical call"
    for job_id in ids:
        assert client.get(f"/jobs/{job_id}").json()["submitter"]["request_id"] == "req_same"

    distinct = [_submit(client, submitter={**shared, "request_id": f"req_{n}"})
                for n in range(2)]
    assert [r.status_code for r in distinct] == [202, 202]
