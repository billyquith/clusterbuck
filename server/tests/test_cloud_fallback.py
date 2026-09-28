"""Cloud fallback (design.md §8): an unavailable fleet sends `cloud_ok` work to the cloud,
at submit or by rescue, and what that work costs is recorded honestly.

The failure modes pinned here are the quiet ones. A local tier nobody can serve used to
win over the cloud unconditionally; a rescued job metered under its old capability would
count as AVOIDED spend and never touch the budget; a burst of admissions checked against
the same stale month-to-date could sail past the cap.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest
from clusterbuck.budget import check_cloud_budget, estimate_cost
from clusterbuck.evaluation import SCALE_VERSION, TASK_CLASSES
from clusterbuck.fleet import CapabilitySpec, Fleet
from clusterbuck.ids import new_ids
from clusterbuck.models import JobRecord, Message, Privacy, Urgency
from clusterbuck.queue import URGENT_TIER, Queue, stream_key
from clusterbuck.rescue import rescue_scan
from clusterbuck.routing import resolve
from clusterbuck.store import Store
from clusterbuck.usage import cloud_cost, usage_scan

LOCAL, CLOUD = "32b-reason", "claude"
LOCAL_MODEL, CLOUD_MODEL = "qwen:32b", "anthropic/claude-sonnet-5"
GROUP = "cbk-workers"


def _fleet(**local_extra) -> Fleet:
    return Fleet(capabilities={
        LOCAL: CapabilitySpec(model_server="http://x/v1", model=LOCAL_MODEL,
                              price_in_per_1k=0.001, price_out_per_1k=0.002, **local_extra),
        CLOUD: CapabilitySpec(model=CLOUD_MODEL, cloud=True, api_key_env="CBK_T_KEY"),
    })


@pytest.fixture(autouse=True)
def _provider_key(monkeypatch):
    # A provider account with no key is never a target (routing.can_call), so every test
    # here that expects the cloud to be reachable has to hold one.
    monkeypatch.setenv("CBK_T_KEY", "sk-test")


@pytest.fixture()
def store(tmp_path) -> Store:
    s = Store(str(tmp_path / "fb.db"))
    for artifact in (LOCAL_MODEL, CLOUD_MODEL):
        for tc in TASK_CLASSES:
            s.set_ability(artifact=artifact, task_class=tc, score=7.0,
                          scale_version=SCALE_VERSION, updated_at="t",
                          n_items=10, n_passed=10)
    return s


def _route(store, fleet=None, **kw):
    kw.setdefault("privacy", "cloud_ok")
    kw.setdefault("urgency", "necessary")
    return resolve(fleet or _fleet(), store, capability=kw.pop("capability", None),
                   task_class=None if "capability" in kw else "extract",
                   min_ability=None if "capability" in kw else 5, **kw)


# --- submit-time fallback ---------------------------------------------------------------


def test_an_unavailable_fleet_sends_a_cloud_ok_job_to_the_cloud(store):
    sel = _route(store, unavailable={LOCAL})
    assert (sel.capability, sel.cloud, sel.fell_back_from) == (CLOUD, True, LOCAL)
    assert sel.artifact == CLOUD_MODEL


def test_a_servable_fleet_keeps_the_job_and_records_a_rescue_target(store):
    """Available — asleep-but-wakeable, or up but slow — is not unavailable. Speed never
    promotes cloud over local (§12); only the rescue alternative is remembered."""
    sel = _route(store, unavailable=set())
    assert (sel.capability, sel.cloud, sel.fell_back_from) == (LOCAL, False, None)
    assert (sel.cloud_alternate, sel.cloud_alternate_model) == (CLOUD, CLOUD_MODEL)


def test_local_only_never_falls_back_and_has_no_rescue_target(store):
    sel = _route(store, privacy="local_only", unavailable={LOCAL})
    assert sel.capability == LOCAL
    assert sel.cloud_alternate is None


def test_waitable_does_not_fall_back_but_keeps_a_target_for_after_escalation(store):
    """No cloud rights until promoted (ADR 18) — but promotion must find somewhere to go."""
    sel = _route(store, urgency="waitable", unavailable={LOCAL})
    assert sel.capability == LOCAL
    assert sel.cloud_alternate == CLOUD


def test_an_exhausted_budget_blocks_the_fallback(store):
    store.record_usage(job_id="spent", ts="t", capability=CLOUD, model=None, node=None,
                       venue="cloud", tokens_in=0, tokens_out=0, outcome="done",
                       cost=100.0, day=datetime.now(UTC).strftime("%Y-%m-%d"))
    sel = _route(store, unavailable={LOCAL}, cloud_budget_monthly=10.0)
    assert sel.capability == LOCAL


def test_an_explicit_tier_falls_back_only_to_its_declared_cloud_fallback(store):
    fleet = _fleet(cloud_fallback=CLOUD)
    sel = _route(store, fleet, capability=LOCAL, unavailable={LOCAL})
    assert (sel.capability, sel.fell_back_from) == (CLOUD, LOCAL)
    # No declaration, no fallback: naming a tier is not permission to leave it.
    sel = _route(store, _fleet(), capability=LOCAL, unavailable={LOCAL})
    assert (sel.capability, sel.cloud_alternate) == (LOCAL, None)


def test_cloud_fallback_must_name_a_provider_account():
    with pytest.raises(ValueError, match="not a registered provider account"):
        Fleet(capabilities={
            "a": CapabilitySpec(model_server="http://x/v1", model="m", cloud_fallback="b"),
            "b": CapabilitySpec(model_server="http://y/v1", model="m"),
        })


# --- rescue -----------------------------------------------------------------------------


class _Wake:
    def __init__(self, live: bool = False) -> None:
        self.live = live

    async def has_live_consumer(self, capability: str) -> bool:
        return self.live


@pytest.fixture()
async def queue(redis_url):
    q = Queue.from_url(redis_url)
    yield q
    await q.aclose()


async def _queued(store, queue, *, deadline_in: float | None = 60, privacy="cloud_ok",
                  urgency="necessary", tier=None, created_at=None):
    job_id, result_key = new_ids()
    rec = JobRecord(id=job_id, created_at="t", capability=LOCAL,
                    messages=[Message(role="user", content="hello")],
                    params={"model": LOCAL_MODEL, "max_tokens": 100},
                    urgency=Urgency(urgency), privacy=Privacy(privacy),
                    result_key=result_key)
    store.insert(id=job_id, result_key=result_key, capability=LOCAL,
                 created_at=created_at or datetime.now(UTC).isoformat().replace(
                     "+00:00", "Z"),
                 urgency=urgency, privacy=privacy,
                 deadline_epoch=time.time() + deadline_in if deadline_in else None,
                 cloud_alternate=CLOUD, cloud_alternate_model=CLOUD_MODEL)
    entry = await queue.enqueue(rec.to_wire(), tier=tier)
    store.record_delivery(job_id, stream=stream_key(LOCAL, tier), entry_id=entry)
    return job_id


async def _rescue(store, queue, *, live=False, **kw):
    kw.setdefault("lead_s", 120)
    kw.setdefault("after_s", None)
    kw.setdefault("max_per_tick", 5)
    kw.setdefault("budget_monthly", None)
    return await rescue_scan(store, queue, _Wake(live), _fleet(), group=GROUP,
                             reserve_fraction=0.2, **kw)


async def test_a_due_unserved_job_moves_to_the_cloud_and_its_row_follows(store, queue):
    job = await _queued(store, queue)
    assert await _rescue(store, queue) == [job]
    row = store.get(job)
    # The row's capability is what usage_scan meters by: left as LOCAL, this job's real
    # provider bill would be recorded as avoided spend.
    assert (row.capability, row.rescued_from, row.stream) == (CLOUD, LOCAL, stream_key(CLOUD))
    assert row.est_cost and row.est_cost > 0
    payload = await queue.read_entry(row.stream, row.entry_id)
    assert payload["capability"] == CLOUD
    assert payload["params"]["model"] == CLOUD_MODEL
    assert payload["messages"][0]["content"] == "hello"


async def test_a_rescue_keeps_the_tier(store, queue):
    job = await _queued(store, queue, tier=URGENT_TIER)
    await _rescue(store, queue)
    assert store.get(job).stream == stream_key(CLOUD, URGENT_TIER)


async def test_a_claimed_entry_is_never_moved(store, queue):
    """First-writer-wins protects the answer, not the money."""
    job = await _queued(store, queue)
    assert await queue.read_one(LOCAL, GROUP, "worker-1") is not None
    assert await _rescue(store, queue) == []
    row = store.get(job)
    assert (row.capability, row.rescued_from, row.est_cost) == (LOCAL, None, None)
    assert row.entry_id is not None


@pytest.mark.parametrize("case", ["live", "not_due", "local_only", "waitable", "past"])
async def test_jobs_that_must_stay_where_they_are(store, queue, case):
    job = await _queued(
        store, queue,
        deadline_in={"not_due": 3600, "past": -5}.get(case, 60),
        privacy="local_only" if case == "local_only" else "cloud_ok",
        urgency="waitable" if case == "waitable" else "necessary")
    assert await _rescue(store, queue, live=case == "live") == []
    assert store.get(job).capability == LOCAL


async def test_no_deadline_is_rescued_only_when_opted_in(store, queue):
    job = await _queued(store, queue, deadline_in=None, created_at="2020-01-01T00:00:00Z")
    assert await _rescue(store, queue) == []
    assert await _rescue(store, queue, after_s=600) == [job]


async def test_the_budget_is_checked_per_rescue_with_estimates_committed(store, queue):
    """A paced allowance that covers exactly one estimate admits exactly one rescue: the
    second is checked against the first's COMMITTED estimate, not the stale $0 that the
    usage table still shows while the first is in flight."""
    import calendar

    a = await _queued(store, queue)
    b = await _queued(store, queue)
    payload = await queue.read_entry(store.get(a).stream, store.get(a).entry_id)
    payload["params"]["model"] = CLOUD_MODEL
    one = estimate_cost(_fleet(), CLOUD, JobRecord.from_wire(payload))
    assert one > 0
    today = datetime.now(UTC)
    elapsed = today.day / calendar.monthrange(today.year, today.month)[1]
    # Paced allowance for `necessary` today: most of one estimate — clear of the float
    # boundary, so the first fits (0 < 0.9) and the second does not (1.0 >= 0.9).
    cap = 0.9 * one / (0.8 * elapsed)
    moved = await _rescue(store, queue, budget_monthly=cap)
    assert len(moved) == 1 and moved[0] in (a, b)


async def test_the_per_tick_cap_holds(store, queue):
    for _ in range(4):
        await _queued(store, queue)
    assert len(await _rescue(store, queue, max_per_tick=2)) == 2


async def test_an_interrupted_rescue_is_undone_by_the_orphan_sweep(store, queue):
    """Row pointed at the cloud, entry never moved: the sweep finds it on the old stream."""
    from clusterbuck.backstop import backstop_scan

    job = await _queued(store, queue)
    row = store.get(job)
    assert store.begin_rescue(job, entry_id=row.entry_id, to_capability=CLOUD, est_cost=1.0)
    out = await backstop_scan(store, queue, _fleet(), orphan_grace_s=0,
                              max_queue_age_s=None, group=GROUP, now=time.time() + 5)
    assert out["adopted"] == 1
    row = store.get(job)
    assert (row.capability, row.rescued_from, row.est_cost) == (LOCAL, None, None)
    assert row.stream == stream_key(LOCAL)


# --- budget -----------------------------------------------------------------------------


def test_committed_estimates_count_against_the_budget(store):
    month = datetime.now(UTC).strftime("%Y-%m")
    store.insert(id="inflight", result_key="r", capability=CLOUD,
                 created_at=f"{month}-01T00:00:00Z", est_cost=50.0)
    assert not check_cloud_budget(store, monthly_cap=40.0, urgency="urgent").allowed
    # Once metered, the estimate gives way to the real figure.
    store.record_usage(job_id="inflight", ts="t", capability=CLOUD, model=None, node=None,
                       venue="cloud", tokens_in=0, tokens_out=0, outcome="done",
                       cost=1.0, day=f"{month}-01")
    assert check_cloud_budget(store, monthly_cap=40.0, urgency="urgent").allowed


# --- cost -------------------------------------------------------------------------------


def _completion(model="claude-sonnet-5", cached=0):
    usage = {"prompt_tokens": 1000, "completion_tokens": 1000, "total_tokens": 2000}
    if cached:
        usage["prompt_tokens_details"] = {"cached_tokens": cached}
        usage["cache_read_input_tokens"] = cached
    return {"id": "x", "object": "chat.completion", "created": 0, "model": model,
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": "hi"}}],
            "usage": usage}


def test_a_mapped_model_is_priced_by_litellm_and_cache_reads_are_cheaper():
    full, src = cloud_cost(_fleet(), CLOUD, _completion(), 1000, 1000)
    cached, _ = cloud_cost(_fleet(), CLOUD, _completion(cached=800), 1000, 1000)
    assert src == "litellm" and full > 0
    assert cached < full


def test_an_unmapped_model_falls_back_to_the_fleet_price_and_says_so():
    fleet = Fleet(capabilities={CLOUD: CapabilitySpec(
        model="anthropic/not-a-real-model", cloud=True,
        price_in_per_1k=0.01, price_out_per_1k=0.02)})
    cost, src = cloud_cost(fleet, CLOUD, _completion("not-a-real-model"), 1000, 1000)
    assert (round(cost, 6), src) == (0.03, "fleet")


async def test_usage_scan_meters_cloud_work_at_the_billed_price(store, queue):
    store.insert(id="c1", result_key="r:c1", capability=CLOUD, created_at="t")
    await queue.write_result("r:c1", {"job_id": "c1", "status": "done",
                                      "worker": "cloud:anthropic",
                                      "completion": _completion(cached=500),
                                      "usage": _completion(cached=500)["usage"]})
    await usage_scan(store, queue, _fleet())
    row = store.recent_usage()[0]
    assert (row.venue, row.cost_source, row.tokens_cached_in) == ("cloud", "litellm", 500)
    assert row.cost == pytest.approx(
        cloud_cost(_fleet(), CLOUD, _completion(cached=500), 1000, 1000)[0])


# --- the submit path --------------------------------------------------------------------


@pytest.fixture()
def fallback_client(redis_url, tmp_path, monkeypatch):
    from clusterbuck.api import create_app
    from fastapi.testclient import TestClient

    monkeypatch.setenv("CBK_T_KEY", "sk-test")
    fleet = tmp_path / "fleet.yaml"
    fleet.write_text(
        "capabilities:\n"
        f"  {LOCAL}:\n    model_server: 'http://127.0.0.1:9/v1'\n    model: '{LOCAL_MODEL}'\n"
        f"  {CLOUD}:\n    model: '{CLOUD_MODEL}'\n    cloud: true\n"
        "    api_key_env: CBK_T_KEY\n")
    app = create_app(redis_url=redis_url, db_path=str(tmp_path / "api.db"),
                     fleet_path=str(fleet), start_scheduler=False)
    with TestClient(app) as c:
        for artifact in (LOCAL_MODEL, CLOUD_MODEL):
            c.app.state.store.set_ability(artifact=artifact, task_class="extract",
                                          score=7.0, scale_version=SCALE_VERSION,
                                          updated_at="t", n_items=10, n_passed=10)
        yield c


def _submit(c, **kw):
    body = {"task_class": "extract", "min_ability": 5, "urgency": "necessary",
            "privacy": "cloud_ok", "messages": [{"role": "user", "content": "hi"}]}
    body.update(kw)
    r = c.post("/jobs", json=body)
    assert r.status_code == 202, r.text
    return c.app.state.store.get(r.json()["id"])


def test_submit_to_a_dark_fleet_goes_to_the_cloud_and_commits_its_estimate(fallback_client):
    """No node enrolled, nothing to wake: this job's only way to be answered is the cloud."""
    row = _submit(fallback_client)
    assert (row.capability, row.privacy) == (CLOUD, "cloud_ok")
    assert row.stream in (stream_key(CLOUD), stream_key(CLOUD, URGENT_TIER))
    assert row.est_cost and row.est_cost > 0


def test_submit_that_stays_local_records_where_it_may_be_rescued_to(fallback_client):
    for privacy, urgency, alt in (("local_only", "necessary", None),
                                  ("cloud_ok", "waitable", CLOUD)):
        row = _submit(fallback_client, privacy=privacy, urgency=urgency)
        assert (row.capability, row.cloud_alternate, row.est_cost) == (LOCAL, alt, None)


def test_a_model_the_sync_router_registered_at_zero_is_still_unpriced(monkeypatch):
    """Found end to end, not by reasoning: building the sync Router registers each
    deployment in LiteLLM's global price map, an unknown one at $0 — after which
    `completion_cost` stopped raising for it and returned 0 labelled as a real price."""
    from clusterbuck.sync import build_router

    monkeypatch.setenv("CBK_T_KEY", "sk-test")
    fleet = Fleet(capabilities={CLOUD: CapabilitySpec(
        model="openai/e2e-unmapped-model", cloud=True, api_key_env="CBK_T_KEY",
        price_in_per_1k=0.01, price_out_per_1k=0.02)})
    build_router(fleet)
    cost, src = cloud_cost(fleet, CLOUD, _completion("e2e-unmapped-model"), 1000, 1000)
    assert (round(cost, 6), src) == (0.03, "fleet")


def test_unpriced_cloud_tiers_are_still_ranked_by_what_they_cost(store):
    """Leaving cloud prices to LiteLLM must not turn cheapest-first into alphabetical."""
    for m in ("openai/gpt-5.6-sol", "openai/gpt-5.6-luna"):
        store.set_ability(artifact=m, task_class="extract", score=7.0,
                          scale_version=SCALE_VERSION, updated_at="t", n_items=10, n_passed=10)
    fleet = Fleet(capabilities={
        LOCAL: CapabilitySpec(model_server="http://x/v1", model=LOCAL_MODEL),
        # "a-" sorts first by name, and is the dearer of the two by LiteLLM's list price.
        "a-dear": CapabilitySpec(model="openai/gpt-5.6-sol", cloud=True,
                                 api_key_env="CBK_T_KEY"),
        "b-cheap": CapabilitySpec(model="openai/gpt-5.6-luna", cloud=True,
                                  api_key_env="CBK_T_KEY"),
    })
    assert _route(store, fleet, unavailable={LOCAL}).capability == "b-cheap"
    assert _route(store, fleet, unavailable=set()).cloud_alternate == "b-cheap"


async def test_an_account_with_no_key_is_never_a_target(store, queue, monkeypatch):
    """Found in review: `cloud_fallback` skips ability scoring, so nothing stopped a job
    being sent — at submit or by rescue — to an account the executor can only fail."""
    job = await _queued(store, queue)  # its alternate was chosen while the key was set
    monkeypatch.delenv("CBK_T_KEY")
    sel = _route(store, _fleet(cloud_fallback=CLOUD), capability=LOCAL, unavailable={LOCAL})
    assert (sel.capability, sel.cloud_alternate) == (LOCAL, None)
    sel = _route(store, unavailable={LOCAL})
    assert (sel.capability, sel.cloud_alternate) == (LOCAL, None)
    assert await _rescue(store, queue) == []
    assert store.get(job).capability == LOCAL


async def test_the_nearest_deadline_is_rescued_first(store, queue):
    later = await _queued(store, queue, deadline_in=100)
    sooner = await _queued(store, queue, deadline_in=10)
    assert await _rescue(store, queue, max_per_tick=1) == [sooner]
    assert store.get(later).capability == LOCAL


def test_a_servable_local_tier_beats_one_nobody_can_serve(store):
    """Both have no ETA; only one of them has a way to be answered."""
    fleet = Fleet(capabilities={
        # The cheaper tier is the unavailable one, so price alone would pick it.
        "cheap-dark": CapabilitySpec(model_server="http://x/v1", model=LOCAL_MODEL,
                                     price_in_per_1k=0.0001, price_out_per_1k=0.0001),
        "dear-asleep": CapabilitySpec(model_server="http://y/v1", model=LOCAL_MODEL,
                                      price_in_per_1k=0.01, price_out_per_1k=0.01),
    })
    assert _route(store, fleet, unavailable={"cheap-dark"}).capability == "dear-asleep"


def test_fleet_view_shows_where_each_tier_falls_back_to(redis_url, tmp_path):
    """A client deciding whether to send `cloud_ok` work to a tier can see where it may go."""
    from clusterbuck.api import create_app
    from fastapi.testclient import TestClient

    fleet = tmp_path / "fleet.yaml"
    fleet.write_text(
        "capabilities:\n"
        f"  {LOCAL}:\n    model_server: 'http://127.0.0.1:9/v1'\n    model: '{LOCAL_MODEL}'\n"
        f"    cloud_fallback: {CLOUD}\n"
        f"  {CLOUD}:\n    model: '{CLOUD_MODEL}'\n    cloud: true\n"
        "    api_key_env: CBK_T_KEY\n")
    app = create_app(redis_url=redis_url, db_path=str(tmp_path / "f.db"),
                     fleet_path=str(fleet), start_scheduler=False)
    with TestClient(app) as c:
        caps = c.get("/fleet").json()["capabilities"]
    assert caps[LOCAL]["cloud_fallback"] == CLOUD
    assert caps[CLOUD]["cloud_fallback"] is None
