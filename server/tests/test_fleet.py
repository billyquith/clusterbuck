"""fleet.yaml registry loader (protocols.md §5)."""

from __future__ import annotations

from pathlib import Path

import pytest

from clusterbuck.fleet import Fleet, load_fleet

SEED = Path(__file__).resolve().parents[1] / "fleet.yaml"


def test_seed_loads():
    fleet = load_fleet(SEED)
    assert "8b-extract" in fleet.capabilities
    cap = fleet.capabilities["8b-extract"]
    assert cap.queue == "q:8b-extract"
    assert cap.model_server.startswith("http")
    assert cap.model


def test_nodes_for_capability():
    fleet = load_fleet(SEED)
    ids = {n.id for n in fleet.nodes_for("70b-reason")}
    assert ids == {"node-b"}  # only node-b advertises 70b-reason


def test_rejects_unknown_field(tmp_path):
    bad = tmp_path / "fleet.yaml"
    bad.write_text("nodes: []\ncapabilities: {}\nbogus: 1\n")
    with pytest.raises(Exception):
        load_fleet(bad)


def test_empty_fleet_is_valid():
    assert Fleet().capabilities == {}
