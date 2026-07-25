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
