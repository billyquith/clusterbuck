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


# --- lifecycle timing, limits and position (protocols.md §1b) -----------------------


_JOB_KEYS = {
    "id", "status", "urgency", "capability",
    "created_at", "started_at", "finished_at",
    "deadline", "escalates_at", "expires_at",
    "queue_position", "result", "usage", "error", "attempts", "worker", "submitter",
}


def test_job_view_key_set_is_stable_in_both_branches(client, redis_url):
    """Pinned deliberately: every other assertion in this file reads individual keys, so
    the endpoint's shape could change — or lose a field a client depends on — without a
    single test failing. This is the guard that makes such a change deliberate."""
    job_id = _submit(client).json()["id"]
    assert set(client.get(f"/jobs/{job_id}").json()) == _JOB_KEYS

    conn = redis.from_url(redis_url, decode_responses=True)
    conn.set(f"res_{job_id[4:]}", json.dumps({
        "job_id": job_id, "status": "done", "worker": "node-a",
        "completed_at": "2026-09-02T00:00:00Z",
        "completion": {"id": "c1", "choices": []},
    }))
    conn.close()
    assert set(client.get(f"/jobs/{job_id}").json()) == _JOB_KEYS


def test_created_at_is_reported_so_a_client_need_not_time_it_locally(client):
    got = client.get(f"/jobs/{_submit(client).json()['id']}").json()
    assert got["created_at"], "server receipt time, authoritative"
    # Not yet observed running, and not finished.
    assert got["started_at"] is None and got["finished_at"] is None


def test_a_job_with_no_bounds_reports_that_nothing_will_give_up_on_it(client):
    """The honest answer to "when does clusterbuck stop trying" for the default shape:
    waitable, no deadline, no escalate_after_min. `null` is the answer, not a gap."""
    got = client.get(f"/jobs/{_submit(client).json()['id']}").json()
    assert got["deadline"] is None
    assert got["escalates_at"] is None
    assert got["expires_at"] is None


def test_the_max_queue_age_backstop_is_reported_as_a_give_up_time(client, monkeypatch):
    """With the backstop configured, a queued job is NOT unbounded — and used to say it was.

    `CBK_MAX_QUEUE_AGE_S` makes the coordinator expire an unclaimed job, but the limits
    view only ever looked at `deadline`. A client polling a job the coordinator was about
    to terminate was told `expires_at: null`, i.e. that nothing would ever give up on it.
    """
    import dataclasses

    from clusterbuck.config import settings
    monkeypatch.setattr("clusterbuck.api.settings",
                        dataclasses.replace(settings, max_queue_age_s=3600))

    got = client.get(f"/jobs/{_submit(client).json()['id']}").json()

    assert got["deadline"] is None, "no deadline was set; the bound is the backstop alone"
    assert got["expires_at"] is not None, "a bounded job must not report itself unbounded"
    assert got["expires_at"] > got["created_at"]


def test_the_earlier_of_deadline_and_backstop_wins(client, monkeypatch):
    """`expires_at` is when clusterbuck stops trying — so it is the first bound to fire,
    not whichever one happens to be implemented."""
    import dataclasses

    from clusterbuck.config import settings
    monkeypatch.setattr("clusterbuck.api.settings",
                        dataclasses.replace(settings, max_queue_age_s=60))

    r = _submit(client, deadline="2030-01-01T00:00:00Z")
    got = client.get(f"/jobs/{r.json()['id']}").json()

    assert got["deadline"].startswith("2030-01-01")
    assert got["expires_at"] < got["deadline"], "the 60s backstop fires long before 2030"


def test_deadline_and_escalation_are_echoed_back(client):
    r = _submit(client, deadline="2030-01-01T00:00:00Z", escalate_after_min=10)
    got = client.get(f"/jobs/{r.json()['id']}").json()
    assert got["deadline"].startswith("2030-01-01")
    assert got["expires_at"] == got["deadline"], "the effective give-up time"
    assert got["escalates_at"], "waitable(10) gains the right to demand capacity"


def test_a_malformed_deadline_is_rejected_not_silently_dropped(client):
    """It used to fall back to "no expiry" with no error, so a client asking for a bound
    got an unbounded job and was never told."""
    r = _submit(client, deadline="next tuesday")
    assert r.status_code == 422
    assert "deadline" in r.json()["detail"]


def test_queue_position_reports_work_ahead_while_nothing_is_running(client):
    """No worker exists in this fixture, so every job stays queued — which is exactly the
    state in which `depth` and `pending` both lie."""
    ids = [_submit(client).json()["id"] for _ in range(3)]
    positions = [client.get(f"/jobs/{i}").json()["queue_position"] for i in ids]
    assert positions == [0, 1, 2]


def test_worker_is_reported_before_the_job_finishes(client, redis_url):
    """`worker` came only from the result blob, so it was null until the job was over —
    which meant a client had no way to tell queued from running. The claim is now read
    from the queue's own pending list.

    The claim and the tick run on a queue built here rather than `app.state.queue`: that
    client is bound to the TestClient's event loop, and driving it from another one fails
    on a cross-loop future. The store is plain SQLite, so it is shared directly.
    """
    import anyio

    from clusterbuck.observe import observe_tick
    from clusterbuck.queue import Queue, stream_key

    job_id = _submit(client).json()["id"]
    store = client.app.state.store

    async def claim_then_observe():
        queue = Queue.from_url(redis_url)
        try:
            await queue.client.xreadgroup(
                "cbk-workers", "node-alpha", {stream_key("8b-extract"): ">"}, count=1)
            await observe_tick(store, queue, group="cbk-workers")
        finally:
            await queue.aclose()

    anyio.run(claim_then_observe)

    got = client.get(f"/jobs/{job_id}").json()
    assert got["status"] == "running"
    assert got["worker"] == "node-alpha", "known before any result exists"
    assert got["started_at"] is not None
    assert got["queue_position"] is None, "not queued any more"


# --- the model that cleared the bar is the model that runs ------------------------------

def _queued_job(redis_url, capability="8b-extract"):
    conn = redis.from_url(redis_url, decode_responses=True)
    entries = conn.xrange(stream_key(capability))
    conn.close()
    return json.loads(entries[-1][1]["job"])


def test_the_resolved_artifact_is_pinned_on_the_job(client, redis_url):
    """A capability is only a queue name — the worker that drains it answers with its own
    CBK_MODEL, for every capability it serves. So the model whose ability cleared the
    floor and the model that ran the job were unrelated, and nothing reconciled them."""
    assert _submit(client).status_code == 202
    assert _queued_job(redis_url)["params"]["model"] == "llama3.2:3b"


def test_a_need_shaped_job_is_pinned_to_the_artifact_that_qualified(client, redis_url):
    r = _submit(client, capability=None, task_class="summarize", min_ability=6)
    assert r.status_code == 202
    job_id = r.json()["id"]
    # The chosen capability is not echoed in the response, so read every tier's stream —
    # then pick out THIS job rather than asserting on whatever else the queues hold.
    conn = redis.from_url(redis_url, decode_responses=True)
    found = [json.loads(e[1]["job"])
             for cap in ("8b-extract", "32b-reason", "70b-reason")
             for e in conn.xrange(stream_key(cap))]
    conn.close()
    job = next(j for j in found if j["id"] == job_id)
    # 6 clears the 32B seed (6.5) and the 70B (7.0); local-then-cheapest picks the 32B.
    assert job["params"]["model"] == "qwen2.5:32b"


def test_a_client_cannot_pin_its_way_past_the_ability_floor(client, redis_url):
    """`params` is forwarded to the model server verbatim, so a client naming its own
    `model` would pick any artifact it liked on whatever tier it asked for — and
    min_ability would enforce nothing at all."""
    r = _submit(client, capability="8b-extract", params={"model": "llama3.1:70b",
                                                         "temperature": 0.1})
    assert r.status_code == 202
    job = _queued_job(redis_url)
    assert job["params"]["model"] == "llama3.2:3b", "client pin overrode the router"
    assert job["params"]["temperature"] == 0.1, "other params must still pass through"




def test_queue_position_for_an_urgent_job_counts_its_own_tier(
    client, redis_url, monkeypatch
):
    """An urgent job's position must be counted on the stream its entry is actually on.

    `_queue_position` called `undelivered` without a `tier`, which defaults to the BASE
    stream — so for a job whose entry had been minted on `q:<cap>:urgent` it compared an
    urgent-stream id against the base stream's last-delivered-id and counted unrelated
    base-tier entries as being ahead. Stream ids are millisecond timestamps, so the
    comparison never raised; it just silently reported a backlog to the one job that had
    jumped that queue. The row records the stream, so the tier was knowable all along.
    """
    monkeypatch.setattr("clusterbuck.api.tiering_ready", lambda *a, **k: True)

    for _ in range(3):
        _submit(client)  # base tier: patient work piling up
    urgent = _submit(client, urgency="urgent").json()["id"]

    row = client.get(f"/jobs/{urgent}").json()
    assert row["queue_position"] == 0, (
        "nothing is ahead of it on the urgent stream; the base-tier backlog is not")


def test_an_oversized_prompt_is_refused_rather_than_written_to_the_broker(client):
    """Redis holds the payload of every queued job in memory, so an unbounded `content`
    was an unbounded write to the broker from a single POST — with no quota, no rate
    limit and no retention sweep behind it. 422 at the edge is the cheap half of that."""
    from clusterbuck.models import MAX_CONTENT_CHARS

    r = _submit(client, messages=[{"role": "user", "content": "x" * (MAX_CONTENT_CHARS + 1)}])
    assert r.status_code == 422

    ok = _submit(client, messages=[{"role": "user", "content": "x" * 1000}])
    assert ok.status_code == 202, "ordinary prompts are untouched"


def test_too_many_messages_is_refused(client):
    from clusterbuck.models import MAX_MESSAGES

    r = _submit(client, messages=[{"role": "user", "content": "hi"}] * (MAX_MESSAGES + 1))
    assert r.status_code == 422


def test_a_job_requiring_an_undeclared_capability_is_refused_at_submit(client):
    """ADR 37 end to end. The fixture fleet's artifacts have no catalog entries, so
    nothing DECLARES vision — and undeclared reads as no. Refusing here beats queuing a
    job that will reach a model which cannot see the image, where the failure looks like
    a model bug rather than a routing one."""
    r = _submit(client, requires={"vision": True})
    assert r.status_code == 422
    assert "vision" in r.json()["detail"]


def test_requires_rides_the_wire_so_the_worker_can_act_on_it(client, redis_url):
    """`json_schema` is the one requirement the worker reads: it licenses forwarding
    `params.response_format`, which is dropped otherwise. That only works if the field
    survives onto the stream, so this asserts the enqueued payload, not just the 202."""
    r = _submit(client, capability="8b-extract", requires={"json_schema": True})
    assert r.status_code == 202, r.text

    conn = redis.from_url(redis_url, decode_responses=True)
    jobs = [json.loads(f["job"])
            for _id, f in conn.xrange(stream_key("8b-extract"))]
    mine = [j for j in jobs if j["id"] == r.json()["id"]]
    assert mine and mine[0]["requires"] == {"json_schema": True}
