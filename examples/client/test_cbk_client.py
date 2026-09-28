"""Reference-client regressions (cbk_client.py) — this is the shape other clients copy,
so a defect here is a defect in every client that follows it.

Run with `cd examples/client && python -m pytest test_cbk_client.py` (or point pytest at
this file directly) — httpx and pytest are the only dependencies, matching cbk_client.py
itself.
"""

from __future__ import annotations

import json
import threading

import httpx
import pytest
from cbk_client import Client, ClusterbuckError, JobStore


def _client_with(handler, tmp_path) -> Client:
    c = Client("http://test", None, JobStore(tmp_path / "jobs.json"))
    c.http = httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler))
    return c


# --- JobStore: atomic, concurrent-safe writes --------------------------------------


def test_concurrent_puts_lose_nothing(tmp_path):
    """Non-atomic `Path.write_text` in place could be interrupted mid-write, and every
    record in the file — not just the one being updated — was lost to the next reader's
    `JSONDecodeError` on a truncated file. Forced here with real concurrent writers."""
    store = JobStore(tmp_path / "jobs.json")

    def worker(n: int) -> None:
        for i in range(20):
            store.put(f"key-{n}-{i}", n=n, i=i)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    records = json.loads((tmp_path / "jobs.json").read_text())
    assert len(records) == 6 * 20, "every put from every thread must have landed"


def test_a_crash_mid_write_leaves_the_previous_version_intact(tmp_path, monkeypatch):
    """The temp-file-then-replace write means a failure DURING the write never touches
    the file readers see — proven by making the write itself fail partway through."""
    store = JobStore(tmp_path / "jobs.json")
    store.put("k1", state="queued")

    real_replace = __import__("os").replace

    def boom(*a, **kw):
        raise OSError("simulated crash mid-write")

    monkeypatch.setattr("os.replace", boom)
    with pytest.raises(OSError):
        store.put("k2", state="queued")
    monkeypatch.setattr("os.replace", real_replace)

    # The file from before the crash is untouched, and no temp file was left behind.
    records = json.loads((tmp_path / "jobs.json").read_text())
    assert records == {"k1": {"state": "queued"}}
    assert list(tmp_path.iterdir()) == [tmp_path / "jobs.json"]


# --- submit(): a 5xx is as ambiguous as a dropped connection -----------------------


def test_submit_retries_a_5xx_with_the_same_key(tmp_path):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(500, json={"error": {"message": "boom"}})
        return httpx.Response(202, json={"id": "job_1", "result_key": "res_1",
                                         "status": "queued"})

    c = _client_with(handler, tmp_path)
    key = c.new_key()
    accepted = c.submit({"capability": "x", "messages": []}, key)

    assert calls["n"] == 2, "the 5xx must have been retried under the SAME key"
    assert accepted["job_id"] == "job_1"


def test_submit_does_not_retry_a_4xx(tmp_path):
    """Not ambiguous: nothing was accepted, so retrying would only repeat the mistake."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(422, json={"error": {"code": "ability_unsatisfied",
                                                    "message": "no artifact clears it"}})

    c = _client_with(handler, tmp_path)
    with pytest.raises(ClusterbuckError) as e:
        c.submit({"capability": "x", "messages": []}, c.new_key())
    assert calls["n"] == 1
    assert e.value.code == "ability_unsatisfied"


# --- poll()/cancel()/resubmit(): clear errors, not KeyError/TypeError ---------------


def test_polling_an_unknown_key_names_the_problem(tmp_path):
    c = _client_with(lambda r: httpx.Response(200, json={}), tmp_path)
    with pytest.raises(KeyError, match="no stored record"):
        c.poll("never-submitted")


def test_polling_a_key_whose_submit_never_got_a_response_says_so(tmp_path):
    """The record left behind by a submit that crashed before reading its response:
    `{request, state: "submitting"}`, no `job_id`. This used to raise a bare `KeyError`
    on the missing dict key, indistinguishable from an unknown key entirely — and gave
    no hint that resubmitting under the SAME key is the documented recovery."""
    c = _client_with(lambda r: httpx.Response(200, json={}), tmp_path)
    c.store.put("half-done", request={"capability": "x"}, state="submitting")
    with pytest.raises(RuntimeError, match="call submit\\(\\) again with this SAME key"):
        c.poll("half-done")
    with pytest.raises(RuntimeError):
        c.cancel("half-done")


def test_resubmitting_an_unknown_key_names_the_problem(tmp_path):
    c = _client_with(lambda r: httpx.Response(200, json={}), tmp_path)
    with pytest.raises(KeyError, match="no stored record"):
        c.resubmit("never-submitted")


# --- poll() persists `retryable` from the job view, not a client-side table ---------


def test_poll_persists_retryable_from_the_job_view(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "job_1", "status": "failed", "result": None,
            "error": "boom", "error_code": "model_server_unreachable", "retryable": True,
        })

    c = _client_with(handler, tmp_path)
    c.store.put("k1", job_id="job_1", result_key="res_1", state="queued")
    record = c.poll("k1")
    assert record["retryable"] is True
