"""Handing the machine back when the owner pauses (ADR 10, design.md → "the owner always wins").

`cbk pause` stopped this node CLAIMING work and did nothing else, while the documentation
promised eviction that was "instant and lossless: stops pulling, unloads". Neither of the
other two halves existed: a 30B generation started a minute earlier held the machine for
minutes more, and the weights stayed resident indefinitely afterwards. On a shared 16 GB
box those are the only two things the owner can actually feel.
"""

from __future__ import annotations

import httpx
import pytest

from cbk_worker.commands import owner_took_the_machine, yields_to_a_person
from cbk_worker.model_manager import ModelManager

OLLAMA = "http://127.0.0.1:11434"


def _manager(handler, manager: str = "ollama") -> tuple[httpx.AsyncClient, ModelManager]:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client, ModelManager(client, OLLAMA, manager)


# --- when to hand it back -------------------------------------------------------------


@pytest.mark.parametrize(("previous", "effective", "expected"), [
    ("active", "paused", True),          # the owner just paused
    ("away", "paused", True),
    (None, "paused", True),              # already paused when the worker started
    ("paused", "paused", False),         # a pause that is merely continuing
    ("paused", "active", False),         # resuming
    ("away", "active", False),           # ladder descent is a different thing entirely
    ("active", "away", False),
])
def test_only_the_transition_into_a_pause_releases_the_machine(previous, effective, expected):
    """A transition, not a state: unloading on every beat of a long pause would ask the
    model server ten times a minute to drop models it dropped at the start."""
    assert owner_took_the_machine(previous, effective) is expected


def test_a_quarantine_is_not_the_owner_pausing():
    """`loop.paused` is set from two independent sources and only one of them is a person
    reaching for their own laptop. This predicate reads the LADDER's mode, so a
    coordinator quarantine — which also pauses claiming — cannot abort a running job."""
    assert owner_took_the_machine("active", "active") is False


# --- unloading -------------------------------------------------------------------------


async def test_unload_asks_ollama_to_drop_the_model_immediately():
    """Ollama has no unload endpoint; `keep_alive: 0` on an empty generate is the
    documented mechanism, and with no prompt "when it is done" is now."""
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = request.read().decode()
        return httpx.Response(200, json={"done": True})

    http, manager = _manager(handler)
    assert await manager.unload("qwen2.5:32b") == (True, None)
    assert seen["url"] == f"{OLLAMA}/api/generate"
    assert '"keep_alive": 0' in seen["body"] or '"keep_alive":0' in seen["body"]
    assert "qwen2.5:32b" in seen["body"]
    await http.aclose()


async def test_unload_does_not_delete_the_weights():
    """The distinction that keeps a coffee break from costing a multi-gigabyte
    re-download: unload frees RAM, remove frees disk."""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200, json={})

    http, manager = _manager(handler)
    await manager.unload("qwen2.5:32b")
    assert all("/api/delete" not in url for url in calls)
    await http.aclose()


async def test_a_server_that_cannot_unload_says_so_rather_than_pretending():
    """llama.cpp and vLLM hold one model for the life of the process. The honest answer
    is that the weights stay resident, not a silent success."""
    http, manager = _manager(lambda r: httpx.Response(200), manager="none")
    ok, error = await manager.unload("qwen2.5:32b")
    assert ok is False
    assert "stays resident" in error
    await http.aclose()


async def test_an_unload_failure_is_reported_not_raised():
    """Releasing the machine runs inside the heartbeat beat; a model server that is
    unhappy must not take the beat down with it."""
    http, manager = _manager(lambda r: httpx.Response(500, text="model not found"))
    ok, error = await manager.unload("ghost")
    assert ok is False and "500" in error
    await http.aclose()


# --- the wiring ------------------------------------------------------------------------
#
# Everything above can be perfect and never run. `_heartbeat_loop` is where the release is
# actually triggered, and it had no test of any kind — so this is the one that proves
# `cbk pause` reaches `evict` and `unload` rather than just being able to.


class _FakeLoop:
    paused = False
    jobs_done = 0
    tps = None
    load_s = None
    broker_ok = True

    def __init__(self) -> None:
        self.evicted = 0
        self.capabilities: list[str] = []

    def evict(self) -> bool:
        self.evicted += 1
        return True

    async def set_capabilities(self, caps) -> None:
        self.capabilities = list(caps)

    def set_installed(self, artifacts) -> None: ...
    def set_resident(self, loaded) -> None: ...

    def set_presence(self, mode, profile) -> None:
        self.presence = (mode, profile)


class _FakeInventory:
    def __init__(self, loaded: list[str]) -> None:
        self._loaded = loaded

    async def installed(self) -> list[str]:
        return ["qwen2.5:32b"]

    async def loaded(self) -> list[str]:
        return list(self._loaded)

    async def digests(self) -> dict:
        return {}


class _FakeManager:
    def __init__(self) -> None:
        self.unloaded: list[str] = []

    async def unload(self, artifact: str):
        self.unloaded.append(artifact)
        return (True, None)


class _FakeResponse:
    fitness = None
    update = None
    action = None


class _FakeRegistry:
    def __init__(self, stop) -> None:
        self.beats = 0
        self._stop = stop

    async def heartbeat(self, node_id, node_key, req):
        self.beats += 1
        self.mode = req.mode
        self.queues = list(req.queues)
        if self.beats >= 2:          # one beat paused, then let the loop exit
            self._stop.set()
        return _FakeResponse()


async def _run_one_pause(tmp_path, *, loaded: list[str], profile: str = "shared"):
    import asyncio

    from cbk_worker.commands import _heartbeat_loop
    from cbk_worker.config import WorkerConfig
    from cbk_worker.presence import PresenceLadder
    from cbk_worker.registry import NodeState, save_state

    state_path = tmp_path / "node.json"
    save_state(state_path, NodeState(
        node_id="node-a", node_key="k", server="http://127.0.0.1:1",
        capabilities=["8b-extract"], ladder=None, mode="paused", profile=profile,
    ))
    cfg = WorkerConfig(worker_id="node-a", consumer_group="cbk-workers",
                       capabilities=("8b-extract",), heartbeat_s=0.01)
    loop, inventory, manager = _FakeLoop(), _FakeInventory(loaded), _FakeManager()
    ladder = PresenceLadder(None, ["8b-extract"])
    stop = asyncio.Event()
    await _heartbeat_loop(_FakeRegistry(stop), loop, ladder, inventory, manager,
                          None, state_path, cfg, stop)
    return loop, manager


async def test_pausing_actually_reaches_evict_and_unload(tmp_path):
    """`cbk pause` writes `mode=paused`; the running worker re-reads it on its next beat.
    From there the owner must get BOTH halves back — the GPU and the RAM."""
    loop, manager = await _run_one_pause(tmp_path, loaded=["qwen2.5:32b", "llama3.2:3b"])

    assert loop.paused is True
    assert loop.evicted == 1, "the in-flight job must be stopped, not just un-claimed"
    assert manager.unloaded == ["qwen2.5:32b", "llama3.2:3b"]


async def test_the_release_happens_once_not_on_every_beat(tmp_path):
    """Two beats, one release. A long pause must not re-ask the model server to drop
    models it dropped at the start, ten times a minute."""
    loop, manager = await _run_one_pause(tmp_path, loaded=["qwen2.5:32b"])

    assert loop.evicted == 1
    assert manager.unloaded == ["qwen2.5:32b"]


async def test_a_server_that_cannot_report_residency_still_gets_evicted(tmp_path):
    """LM Studio and llama.cpp report nothing loaded, which means UNKNOWN, not empty. The
    RAM cannot be reclaimed there — but stopping the running job still can be, and is."""
    loop, manager = await _run_one_pause(tmp_path, loaded=[])

    assert loop.evicted == 1, "eviction does not depend on the inventory adapter"
    assert manager.unloaded == []


# --- who the machine belongs to --------------------------------------------------------


@pytest.mark.parametrize(("profile", "yields"), [
    ("shared", True),          # somebody's laptop; their processes need the resources
    ("background", True),
    (None, True),              # enrolled before the profile was persisted
    ("dedicated", False),      # exists to serve; optimises for availability
])
def test_only_a_machine_somebody_owns_is_handed_back(profile, yields):
    assert yields_to_a_person(profile) is yields


async def test_a_dedicated_node_drains_instead_of_evicting(tmp_path):
    """Availability, not resource saving.

    Both halves of an eviction are pure loss on a machine that exists to serve: dropping
    the weights makes the next job pay a cold load while the node sits idle holding
    nothing, and killing the running generation throws away GPU time already spent to
    re-run the same work elsewhere. Pausing one is an operator taking it out of rotation,
    and the right shape for that is a drain — claim nothing further, finish what is in
    hand, stay warm.
    """
    loop, manager = await _run_one_pause(
        tmp_path, loaded=["qwen2.5:32b"], profile="dedicated")

    assert loop.paused is True, "it must still stop CLAIMING — that is the drain"
    assert loop.evicted == 0, "the job in hand finishes; nobody is waiting for the machine"
    assert manager.unloaded == [], "staying warm is the point of a dedicated node"


async def test_a_shared_node_gives_back_both_the_gpu_and_the_ram(tmp_path):
    """The other side of the same rule: the machine is somebody's and they want it now."""
    loop, manager = await _run_one_pause(
        tmp_path, loaded=["qwen2.5:32b"], profile="shared")

    assert loop.evicted == 1
    assert manager.unloaded == ["qwen2.5:32b"]


async def test_a_node_enrolled_before_profiles_were_persisted_still_yields(tmp_path):
    """Unknown yields, deliberately: somebody has just typed `cbk pause` on it. Being
    wrong costs one cold load and one re-run; being wrong the other way leaves a person
    in front of their own laptop without it."""
    loop, manager = await _run_one_pause(
        tmp_path, loaded=["qwen2.5:32b"], profile=None)

    assert loop.evicted == 1
    assert manager.unloaded == ["qwen2.5:32b"]


# --- telling the coordinator the broker is gone ---------------------------------------


async def _beat_with(tmp_path, *, broker_ok: bool):
    """One heartbeat from a node whose broker is up or down, returning what it reported."""
    import asyncio

    from cbk_worker.commands import _heartbeat_loop
    from cbk_worker.config import WorkerConfig
    from cbk_worker.presence import PresenceLadder
    from cbk_worker.registry import NodeState, save_state

    state_path = tmp_path / "node.json"
    save_state(state_path, NodeState(
        node_id="node-a", node_key="k", server="http://127.0.0.1:1",
        capabilities=["8b-extract"], ladder=None, mode="active", profile="shared",
    ))
    cfg = WorkerConfig(worker_id="node-a", consumer_group="cbk-workers",
                       capabilities=("8b-extract",), heartbeat_s=0.01)
    loop = _FakeLoop()
    loop.broker_ok = broker_ok
    stop = asyncio.Event()
    registry = _FakeRegistry(stop)
    await _heartbeat_loop(registry, loop, PresenceLadder(None, ["8b-extract"]),
                          _FakeInventory([]), _FakeManager(), None, state_path, cfg, stop)
    return registry


async def test_a_node_that_can_reach_the_broker_reports_its_queues(tmp_path):
    registry = await _beat_with(tmp_path, broker_ok=True)
    assert "q:8b-extract" in registry.queues


async def test_a_node_cut_off_from_the_broker_reports_no_queues(tmp_path):
    """The condition that lets the work loop survive an outage instead of exiting.

    `queues` means "the streams I am claiming from", and a node that cannot reach Redis
    is claiming from none — so this is the literal truth, not a signal smuggled through a
    field that means something else. The coordinator already reads an empty `queues` as
    "not serving" (server wake.py, `nodes_serving`), so the wake a queued job is owed
    still fires instead of being suppressed by a node that cannot answer it.
    """
    registry = await _beat_with(tmp_path, broker_ok=False)
    assert registry.queues == []
    assert registry.mode == "active", (
        "it is not paused and must not claim to be — the owner has not taken it, "
        "it simply cannot reach the broker"
    )


# --- a quarantine holds between beats ------------------------------------------------


class _WatchingInventory(_FakeInventory):
    """Records `loop.paused` at the one point in a beat where the loop is waiting on I/O
    before the heartbeat reply — which is exactly when the work loop gets to poll."""

    def __init__(self, loop) -> None:
        super().__init__([])
        self._loop = loop
        self.paused_seen: list[bool] = []

    async def installed(self) -> list[str]:
        self.paused_seen.append(self._loop.paused)
        return await super().installed()


class _QuarantiningRegistry(_FakeRegistry):
    async def heartbeat(self, node_id, node_key, req):
        from cbk_worker.models import Fitness
        self.beats += 1
        if self.beats >= 3:
            self._stop.set()
        resp = _FakeResponse()
        resp.fitness = Fitness(status="quarantine", reason="blocked")
        return resp


async def test_a_quarantined_worker_stays_paused_through_the_next_beat(tmp_path):
    """The verdict arrives on the heartbeat REPLY, so it must survive the start of the next
    beat. Recomputing `paused` from the ladder alone there un-quarantined the node for the
    whole inventory-and-heartbeat round trip, every beat — long enough for the work loop to
    claim, which e2e version.sh caught as a block-listed build still draining its queue."""
    import asyncio

    from cbk_worker.commands import _heartbeat_loop
    from cbk_worker.config import WorkerConfig
    from cbk_worker.presence import PresenceLadder
    from cbk_worker.registry import NodeState, save_state

    state_path = tmp_path / "node.json"
    save_state(state_path, NodeState(
        node_id="node-a", node_key="k", server="http://127.0.0.1:1",
        capabilities=["8b-extract"], ladder=None, mode="active", profile="shared",
    ))
    cfg = WorkerConfig(worker_id="node-a", consumer_group="cbk-workers",
                       capabilities=("8b-extract",), heartbeat_s=0.01)
    loop = _FakeLoop()
    inventory = _WatchingInventory(loop)
    stop = asyncio.Event()
    await _heartbeat_loop(_QuarantiningRegistry(stop), loop,
                          PresenceLadder(None, ["8b-extract"]), inventory, _FakeManager(),
                          None, state_path, cfg, stop)

    # Beat 1 has not heard a verdict yet; every beat after it has.
    assert inventory.paused_seen[1:] == [True, True]
    assert loop.paused is True
