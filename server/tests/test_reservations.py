"""Reservations (protocols.md §8): admission, the reconciler-tick lifecycle, and the API.

The lifecycle proof is the tick driven directly through now = warm_by → starts → ends,
asserting the state sequence and that the wake side-effect fires exactly once.
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from clusterbuck.api import create_app
from clusterbuck.fleet import Fleet, NodeSpec, CapabilitySpec
from clusterbuck.reservations import admit, reservation_tick, window_start_epoch
from clusterbuck.evaluation import SCALE_VERSION
from clusterbuck.store import Store

CAP = "8b-extract"


def _fleet() -> Fleet:
    return Fleet(
        nodes=[NodeSpec(id="node-a", mac="aa:bb:cc:dd:ee:ff", capabilities=[CAP])],
        capabilities={CAP: CapabilitySpec(
            queue="q:8b-extract", model_server="http://127.0.0.1:1/v1", model="m")},
    )


@pytest.fixture()
def store(tmp_path) -> Store:
    s = Store(str(tmp_path / "rsv.db"))
    # Admission resolves through the ability matrix, so the test fleet's artifact needs a
    # measured score — you cannot book capacity for an ability nobody has measured.
    s.set_ability(artifact="m", task_class="x", score=5.0,
                  scale_version=SCALE_VERSION, updated_at="t")
    return s


class SpyWake:
    """Duck-typed WakeCoordinator that records maybe_wake calls (no Redis, no packets)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def maybe_wake(self, capability: str, *, reason: str) -> list[str]:
        self.calls.append((capability, reason))
        return []


# --- admission ---------------------------------------------------------------

def test_admit_confirmed_when_capable_node_exists(store):
    a = admit(_fleet(), store, task_class="x", min_ability=4, window_start="asap",
              duration_min=30, now=1000.0, lead_s=300)
    assert a.status == "confirmed"
    assert a.capability == CAP and a.node == "node-a" and a.artifact == "m"
    assert a.warm_by == 1000.0 and a.starts == 1000.0
    assert a.ends == 1000.0 + 30 * 60


def test_admit_declined_without_fleet(store):
    assert admit(None, store, task_class="x", min_ability=4, window_start="asap",
                 duration_min=30).status == "declined"


def test_admit_declined_when_capability_unserved(store):
    # Nothing in the test fleet reaches ability 9, so the booking is declined outright
    # rather than confirmed against a weaker artifact.
    a = admit(_fleet(), store, task_class="x", min_ability=9, window_start="asap",
              duration_min=30)
    assert a.status == "declined"
    assert "ability 9" in (a.reason or "")


def test_window_start_epoch():
    assert window_start_epoch("asap", 1000.0) == 1000.0
    now = time.time()
    nxt = window_start_epoch("03:30", now)
    assert nxt > now and nxt <= now + 24 * 3600


# --- reconciler-tick lifecycle ----------------------------------------------

def _seed(store: Store, *, warm_by, starts, ends, state="scheduled"):
    store.insert_reservation(
        id="rsv1", status="confirmed", state=state, task_class="x", min_ability=4,
        capability=CAP, node="node-a", artifact="m", priority="medium",
        privacy="local_only", load="light", duration_min=30, est_jobs=None,
        warm_by=warm_by, starts=starts, ends=ends, created_at="t",
    )


async def test_lifecycle_progresses_and_wakes_once(tmp_path):
    store = Store(str(tmp_path / "r.db"))
    _seed(store, warm_by=100, starts=200, ends=300)
    spy = SpyWake()

    await reservation_tick(store, spy, now=50)   # before warm_by
    assert store.get_reservation("rsv1").state == "scheduled"
    assert spy.calls == []

    await reservation_tick(store, spy, now=150)  # ≥ warm_by → warming (+ wake once)
    assert store.get_reservation("rsv1").state == "warming"
    assert len(spy.calls) == 1

    await reservation_tick(store, spy, now=160)  # still warming, before starts
    assert store.get_reservation("rsv1").state == "warming"
    assert len(spy.calls) == 1                   # idempotent — no second wake

    await reservation_tick(store, spy, now=250)  # ≥ starts → open
    assert store.get_reservation("rsv1").state == "open"

    await reservation_tick(store, spy, now=350)  # ≥ ends → draining
    assert store.get_reservation("rsv1").state == "draining"

    await reservation_tick(store, spy, now=360)  # draining → closed
    assert store.get_reservation("rsv1").state == "closed"
    assert len(spy.calls) == 1


async def test_lifecycle_missed_window_closes_without_waking(tmp_path):
    store = Store(str(tmp_path / "r.db"))
    _seed(store, warm_by=100, starts=200, ends=300)
    spy = SpyWake()
    await reservation_tick(store, spy, now=500)  # whole window already past
    assert store.get_reservation("rsv1").state == "closed"
    assert spy.calls == []


async def test_cancelled_is_ignored_by_tick(tmp_path):
    store = Store(str(tmp_path / "r.db"))
    _seed(store, warm_by=100, starts=200, ends=300)
    assert store.cancel_reservation("rsv1") is True
    assert store.get_reservation("rsv1").state == "cancelled"
    spy = SpyWake()
    await reservation_tick(store, spy, now=150)  # would warm if active
    assert store.get_reservation("rsv1").state == "cancelled"
    assert spy.calls == []


# --- API ---------------------------------------------------------------------

@pytest.fixture()
def rsv_client(redis_url, tmp_path):
    fleet = tmp_path / "fleet.yaml"
    fleet.write_text(
        "nodes:\n"
        "  - id: node-a\n"
        "    mac: 'aa:bb:cc:dd:ee:ff'\n"
        "    capabilities: [8b-extract]\n"
        "capabilities:\n"
        "  8b-extract:\n"
        "    queue: 'q:8b-extract'\n"
        "    model_server: 'http://127.0.0.1:1/v1'\n"
        "    model: 'm'\n"
    )
    db = str(tmp_path / "t.db")
    seed = Store(db)
    for task_class in ("x", "summarize"):
        seed.set_ability(artifact="m", task_class=task_class, score=5.0,
                         scale_version=SCALE_VERSION, updated_at="t")
    app = create_app(redis_url=redis_url, db_path=db,
                     fleet_path=str(fleet), start_scheduler=False)
    with TestClient(app) as c:
        yield c


def test_post_reservation_confirmed(rsv_client):
    r = rsv_client.post("/reservations", json={
        "task_class": "summarize", "min_ability": 4,
        "window": {"start": "asap"}, "duration_min": 30,
    })
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == "confirmed"
    assert body["state"] == "scheduled"
    assert body["plan"]["node"] == "node-a"
    assert body["id"].startswith("rsv_")


def test_the_api_actually_reaches_the_measured_warm_lead(rsv_client, monkeypatch):
    """The wiring, not the arithmetic — and the wiring was the half that was missing.

    `admit` derives the lead from measured cold-load time when `lead_s is None`, and
    `Store.warm_lead_for` was unit-tested. But the only production caller always passed
    `settings.warm_lead_s`, which defaulted to 300, so the measured branch was dead in
    production while CLAUDE.md claimed the lead "comes from each node's measured
    cold-load time rather than a hardcoded five minutes".

    Asserts on the call itself: a node measuring a 90s cold load must widen the lead past
    the 300s default (90 x WARM_LEAD_SAFETY is smaller, so a bare value would not prove
    anything — what matters is that the measurement is consulted at all).
    """
    from clusterbuck import api as api_mod
    from clusterbuck.config import settings

    assert settings.warm_lead_s is None, (
        "CBK_WARM_LEAD_S must be unset by default, or the measured path is unreachable")

    seen = {}
    real_admit = api_mod.admit

    def spy(*a, **kw):
        seen.update(kw)
        return real_admit(*a, **kw)

    monkeypatch.setattr(api_mod, "admit", spy)
    r = rsv_client.post("/reservations", json={
        "task_class": "summarize", "min_ability": 4, "window": {"start": "asap"},
    })

    assert r.status_code == 201, r.text
    assert "lead_s" in seen, "admit was not called with an explicit lead_s"
    assert seen["lead_s"] is None, (
        f"api passed lead_s={seen['lead_s']!r}; anything but None skips the measurement")


def test_post_reservation_declined(rsv_client):
    r = rsv_client.post("/reservations", json={
        "task_class": "x", "min_ability": 9, "window": {"start": "asap"},
    })
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "declined"
    assert body["state"] is None
    assert body["plan"] is None


def test_reservation_get_and_delete(rsv_client):
    rid = rsv_client.post("/reservations", json={
        "task_class": "x", "min_ability": 4, "window": {"start": "asap"},
    }).json()["id"]

    assert rsv_client.get(f"/reservations/{rid}").json()["status"] == "confirmed"
    assert rsv_client.get("/reservations/rsv_missing").status_code == 404

    deleted = rsv_client.delete(f"/reservations/{rid}").json()
    assert deleted["state"] == "cancelled"


def test_recurring_reservation_rejected(rsv_client):
    r = rsv_client.post("/reservations", json={
        "task_class": "x", "min_ability": 4, "window": {"start": "02:00", "recur": "daily"},
    })
    assert r.status_code == 422


def test_job_can_reference_confirmed_reservation(rsv_client):
    rid = rsv_client.post("/reservations", json={
        "task_class": "x", "min_ability": 4, "window": {"start": "asap"},
    }).json()["id"]

    ok = rsv_client.post("/jobs", json={
        "capability": CAP, "messages": [{"role": "user", "content": "hi"}],
        "reservation": rid,
    })
    assert ok.status_code == 202

    bad = rsv_client.post("/jobs", json={
        "capability": CAP, "messages": [{"role": "user", "content": "hi"}],
        "reservation": "rsv_nope",
    })
    assert bad.status_code == 400


# --- the warm lead is measured, not a constant -----------------------------------------

def test_warm_lead_uses_the_slowest_measured_cold_load(tmp_path):
    """A reservation does not get to pick which node takes its window, so the lead has to
    cover the slowest one. `DEFAULT_WARM_LEAD_S` was a hardcoded five minutes standing in
    for exactly this number."""
    import json as _json

    from clusterbuck.orm.node import Node
    from clusterbuck.reservations import DEFAULT_WARM_LEAD_S

    s = Store(str(tmp_path / "lead.db"))
    rows = [Node(node_id="quick", node_key="k", capabilities=_json.dumps(["c"]),
                 load_s=8.0, enrolled_at="t"),
            Node(node_id="slow", node_key="k", capabilities=_json.dumps(["c"]),
                 load_s=90.0, enrolled_at="t")]
    s.list_nodes = lambda: rows          # type: ignore[method-assign]
    assert s.warm_lead_for("c", DEFAULT_WARM_LEAD_S) == 90.0


def test_warm_lead_falls_back_when_nothing_measured(tmp_path):
    """A model server that cannot report which models are resident yields no samples at
    all — coldness has to be proven — so such a fleet keeps the constant."""
    from clusterbuck.reservations import DEFAULT_WARM_LEAD_S

    s = Store(str(tmp_path / "lead2.db"))
    assert s.warm_lead_for("c", DEFAULT_WARM_LEAD_S) == DEFAULT_WARM_LEAD_S


def test_a_measured_fleet_warms_earlier_than_its_load_time(tmp_path):
    """A window that opens onto a model still loading has failed at the one thing
    pre-warming exists to do, so the measured median is padded."""
    import json as _json

    from clusterbuck.orm.node import Node
    from clusterbuck.reservations import WARM_LEAD_SAFETY

    s = Store(str(tmp_path / "lead3.db"))
    s.list_nodes = lambda: [Node(node_id="n", node_key="k",  # type: ignore[method-assign]
                                 capabilities=_json.dumps(["c"]), load_s=40.0,
                                 enrolled_at="t")]
    assert WARM_LEAD_SAFETY * s.warm_lead_for("c", 300) > 40.0
