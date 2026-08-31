"""The admin verbs against the coordinator's REAL response shapes (protocols.md §1b, §10).

These verbs were written against response shapes the coordinator does not serve, and
nothing caught it: the unit suite covered only argument *parsing*, and the one e2e that
ran `cbk fleet` discarded its stderr and its exit code. So the assertions here are about
the shapes themselves — the bodies below are copied from what `GET /fleet` and
`GET /jobs/{id}` actually return, not from what the CLI wished they returned.
"""

from __future__ import annotations

import argparse

import httpx
import pytest

from cbk_worker import commands

# --- the coordinator's real bodies -----------------------------------------------------

# GET /fleet: capabilities is an OBJECT keyed by name; nodes carry the capability mapping.
FLEET = {
    "capabilities": {
        "8b-extract": {"queue": "q:8b-extract", "model": "llama3.2:3b",
                       "model_server": "http://localhost:11434/v1"},
        "32b-reason": {"queue": "q:32b-reason", "model": "qwen2.5:32b",
                       "model_server": "http://localhost:11434/v1"},
    },
    "nodes": [
        {"id": "node-a", "wake": "on-demand", "capabilities": ["8b-extract"]},
        {"id": "node-b", "wake": "opportunistic",
         "capabilities": ["8b-extract", "32b-reason"]},
    ],
}

# POST /jobs → 202: the handle is `id`, not `job_id`.
SUBMITTED = {"id": "job_abc123", "result_key": "res_abc123", "status": "queued"}

# GET /jobs/{id}: FLAT — `result` is the completion itself, worker/error beside it.
POLLED = {
    "id": "job_abc123", "status": "done", "urgency": "waitable",
    "result": {"id": "chatcmpl-1", "model": "llama3.2:3b",
               "choices": [{"message": {"role": "assistant", "content": "42"}}]},
    "error": None, "attempts": 1, "worker": "node-a",
}


@pytest.fixture()
def coordinator(monkeypatch):
    """Patch httpx.AsyncClient so the verbs talk to the bodies above."""
    def handle(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/fleet":
            return httpx.Response(200, json=FLEET)
        if path == "/nodes":
            return httpx.Response(200, json={"nodes": []})
        if path == "/jobs":
            return httpx.Response(202, json=SUBMITTED)
        if path.startswith("/jobs/"):
            return httpx.Response(200, json=POLLED)
        return httpx.Response(404, json={"detail": "not found"})

    real = httpx.AsyncClient

    class Mocked(real):
        def __init__(self, *a, **kw):
            kw["transport"] = httpx.MockTransport(handle)
            super().__init__(*a, **kw)

    monkeypatch.setattr(httpx, "AsyncClient", Mocked)


def _args(**kw) -> argparse.Namespace:
    base = {"server": "http://coordinator:8018", "prompt": "hi", "urgency": "waitable",
            "privacy": "local_only", "capability": "8b-extract", "task_class": None,
            "min_ability": None, "job_id": "job_abc123"}
    return argparse.Namespace(**{**base, **kw})


async def test_submit_prints_the_job_id_not_none(coordinator, capsys):
    """The 202 body names the handle `id`; reading `job_id` printed 'submitted None',
    leaving the user with nothing to pass to `cbk status`."""
    assert await commands.run_submit(_args()) == 0
    out = capsys.readouterr().out
    assert "job_abc123" in out
    assert "None" not in out


async def test_status_prints_the_completion_text(coordinator, capsys):
    """The whole point of the verb. The poll body is flat, so treating `result` as a
    wrapper around `completion` found nothing and printed only the status line."""
    assert await commands.run_status(_args()) == 0
    out = capsys.readouterr().out
    assert "job_abc123: done" in out
    assert "42" in out            # the answer itself
    assert "node-a" in out        # the worker that ran it


async def test_status_surfaces_a_failed_job_error(monkeypatch, capsys):
    """`error` is a sibling of `result`, not nested inside it."""
    failed = {"id": "job_x", "status": "failed", "urgency": "waitable", "result": None,
              "error": "model server refused the connection", "attempts": 3,
              "worker": "cbk-reaper"}

    real = httpx.AsyncClient

    class Mocked(real):
        def __init__(self, *a, **kw):
            kw["transport"] = httpx.MockTransport(
                lambda _r: httpx.Response(200, json=failed))
            super().__init__(*a, **kw)

    monkeypatch.setattr(httpx, "AsyncClient", Mocked)
    assert await commands.run_status(_args(job_id="job_x")) == 0
    captured = capsys.readouterr()
    assert "model server refused" in captured.err
    assert "attempts: 3" in captured.out


async def test_fleet_lists_capabilities_without_crashing(coordinator, capsys):
    """Regression: capabilities arrive as an object keyed by name, so iterating it yielded
    bare strings and `cbk fleet` died with AttributeError on any non-empty fleet."""
    assert await commands.run_fleet(_args()) == 0
    out = capsys.readouterr().out
    assert "8b-extract" in out and "llama3.2:3b" in out
    assert "32b-reason" in out and "qwen2.5:32b" in out
    # The capability→node mapping is inverted out of the node list.
    assert "node-a, node-b" in out
    assert "node-b" in out.splitlines()[2]


async def test_fleet_on_an_empty_coordinator_says_so(monkeypatch, capsys):
    real = httpx.AsyncClient

    class Mocked(real):
        def __init__(self, *a, **kw):
            kw["transport"] = httpx.MockTransport(
                lambda _r: httpx.Response(200, json={"capabilities": {}, "nodes": []}))
            super().__init__(*a, **kw)

    monkeypatch.setattr(httpx, "AsyncClient", Mocked)
    assert await commands.run_fleet(_args()) == 0
    assert "fleet is empty" in capsys.readouterr().out
