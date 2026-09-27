"""The coordinator's measurement of each capability's model server (model_health.py)."""

from __future__ import annotations

import asyncio

import httpx
from clusterbuck.fleet import CapabilitySpec, Fleet
from clusterbuck.model_health import ModelHealth


def _fleet(**servers: str) -> Fleet:
    caps = {name: CapabilitySpec(queue=f"q:{name}", model="fake", model_server=url)
            for name, url in servers.items()}
    caps["provider"] = CapabilitySpec(queue="q:provider", model="anthropic/x", cloud=True)
    return Fleet(capabilities=caps)


def _probed(fleet: Fleet, handler) -> tuple[ModelHealth, list[str]]:
    seen: list[str] = []

    def record(request: httpx.Request):
        seen.append(str(request.url))
        return handler(request)

    health = ModelHealth(fleet, transport=httpx.MockTransport(record))
    asyncio.run(health.probe_all())
    return health, seen


def _models(*ids: str) -> httpx.Response:
    return httpx.Response(200, json={"object": "list", "data": [{"id": i} for i in ids]})


def test_ready_when_reachable_and_listing_the_model():
    health, _ = _probed(_fleet(a="http://s/v1"), lambda r: _models("fake:latest"))
    view = health.view("a")
    assert view["state"] == "ready"  # the `:latest` spelling is the same artifact
    assert view["checked_at"] and view["last_ok_at"] and view["last_error"] is None


def test_degraded_when_the_model_is_not_there_or_the_server_errors():
    health, _ = _probed(_fleet(a="http://s/v1"), lambda r: _models("something-else"))
    assert health.view("a")["state"] == "degraded"
    assert "fake" in health.view("a")["last_error"]
    health, _ = _probed(_fleet(a="http://s/v1"), lambda r: httpx.Response(503))
    assert health.view("a")["state"] == "degraded"


def test_unreachable_when_nothing_answers():
    def refuse(request):
        raise httpx.ConnectError("refused", request=request)

    health, _ = _probed(_fleet(a="http://s/v1"), refuse)
    view = health.view("a")
    assert view["state"] == "unreachable"
    assert view["last_ok_at"] is None, "never been up"
    assert "ConnectError" in view["last_error"]


def test_last_ok_survives_an_outage():
    """The point of `last_ok_at`: how long it has been down, not merely that it is."""
    up = {"v": True}

    def flaky(request):
        if up["v"]:
            return _models("fake")
        raise httpx.ConnectError("gone", request=request)

    health = ModelHealth(_fleet(a="http://s/v1"), transport=httpx.MockTransport(flaky))
    asyncio.run(health.probe_all())
    first_ok = health.view("a")["last_ok_at"]
    up["v"] = False
    asyncio.run(health.probe_all())
    assert health.view("a")["state"] == "unreachable"
    assert health.view("a")["last_ok_at"] == first_ok


def test_one_probe_per_server_and_none_for_a_provider_account():
    health, seen = _probed(_fleet(a="http://s/v1", b="http://s/v1", c="http://t/v1"),
                           lambda r: _models("fake"))
    assert sorted(seen) == ["http://s/v1/models", "http://t/v1/models"]
    assert health.view("provider")["state"] == "unknown"
    assert ModelHealth(_fleet(a="http://s/v1")).view("a")["state"] == "unknown"


def test_fleet_shows_health_and_declared_features(client):
    caps = client.get("/fleet").json()["capabilities"]
    assert caps["8b-extract"]["health"]["state"] == "unknown"  # no scheduler in tests
    features = caps["8b-extract"]["features"]
    assert set(features) == {"context_tokens", "tools", "json_schema", "vision"}
    # A provider account declares its own on fleet.yaml; the same lookup routing uses.
    assert "features" in caps["claude-sonnet"]


def test_fleet_features_are_the_ones_requires_is_checked_against(client):
    """Declare vision for the tier's model, and both /fleet and routing agree at once."""
    msg = [{"role": "user", "content": "x"}]
    job = {"capability": "8b-extract", "messages": msg, "requires": {"vision": True}}
    assert client.post("/jobs", json=job).status_code == 422
    assert not client.get("/fleet").json()["capabilities"]["8b-extract"]["features"][
        "vision"]

    client.post("/catalog", json={"artifact": "llama3.2:3b", "registry_ref": "llama3.2:3b",
                                  "size_gb": 2, "min_ram_gb": 4,
                                  "supports_vision": True})
    assert client.get("/fleet").json()["capabilities"]["8b-extract"]["features"][
        "vision"] is True
    assert client.post("/jobs", json=job).status_code == 202
