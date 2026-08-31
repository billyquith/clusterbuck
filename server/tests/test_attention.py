"""Client attention lease (M4a / §9): promote a client's backlog, demote on expiry."""

from __future__ import annotations

import json

import pytest

from clusterbuck.attention import attention_tick
from clusterbuck.queue import Queue
from clusterbuck.store import Store


def _submit(client, *, client_key, task_class):
    return client.post("/jobs", json={
        "capability": "8b-extract", "task_class": task_class,
        "messages": [{"role": "user", "content": "x"}],
        "urgency": "waitable", "client_key": client_key,
    }).json()


def _urgency(client, job_id):
    return client.get(f"/jobs/{job_id}").json()["urgency"]


def test_attention_promotes_and_scopes(client):
    a = _submit(client, client_key="c1", task_class="summarize")
    b = _submit(client, client_key="c1", task_class="code")
    other = _submit(client, client_key="c2", task_class="summarize")

    # Scoped to summarize: only that client+class promotes.
    r = client.post("/attention", json={"client_key": "c1", "state": "active",
                                         "scope": ["summarize"], "ttl_s": 600}).json()
    assert r["promoted"] == 1
    assert r["lease_expires"] is not None
    assert _urgency(client, a["id"]) == "necessary"
    assert _urgency(client, b["id"]) == "waitable"     # different task class
    assert _urgency(client, other["id"]) == "waitable"  # different client


def test_attention_unscoped_promotes_all_for_client(client):
    a = _submit(client, client_key="cc", task_class="summarize")
    b = _submit(client, client_key="cc", task_class="code")
    r = client.post("/attention", json={"client_key": "cc", "state": "active"}).json()
    assert r["promoted"] == 2
    assert _urgency(client, a["id"]) == "necessary"
    assert _urgency(client, b["id"]) == "necessary"


def test_attention_idle_demotes(client):
    a = _submit(client, client_key="c3", task_class="summarize")
    client.post("/attention", json={"client_key": "c3", "state": "active"})
    assert _urgency(client, a["id"]) == "necessary"

    client.post("/attention", json={"client_key": "c3", "state": "idle"})
    assert _urgency(client, a["id"]) == "waitable"


# --- reconciler expiry (unit) ---

@pytest.fixture()
def store(tmp_path) -> Store:
    return Store(str(tmp_path / "att.db"))


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


async def test_tick_demotes_unstarted_on_expiry(store, queue):
    store.insert(id="j1", result_key="res_j1", capability="8b-extract", created_at="t",
                 client_key="c1", task_class="summarize")
    store.attention_promote("c1", None)
    assert store.get("j1").urgency == "necessary"
    store.upsert_attention_lease("c1", None, expires_at=100.0)

    assert await attention_tick(store, queue, now=200.0) == 1
    row = store.get("j1")
    assert row.urgency == "waitable" and row.promoted_by is None


async def test_tick_keeps_served_jobs(store, queue):
    store.insert(id="j2", result_key="res_j2", capability="8b-extract", created_at="t",
                 client_key="c2")
    store.attention_promote("c2", None)
    store.upsert_attention_lease("c2", None, expires_at=100.0)
    await queue.client.set("res_j2", json.dumps({"job_id": "j2", "status": "done"}))

    assert await attention_tick(store, queue, now=200.0) == 0
    assert store.get("j2").urgency == "necessary"  # served → left promoted
