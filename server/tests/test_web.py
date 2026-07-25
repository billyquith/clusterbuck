"""The htmx dashboard (M3b): page + fragments render, vendored asset is served."""

from __future__ import annotations


def test_dashboard_page(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "clusterbuck" in r.text
    assert "/static/htmx.min.js" in r.text  # vendored, not a CDN URL


def test_vendored_htmx_served(client):
    r = client.get("/static/htmx.min.js")
    assert r.status_code == 200
    assert "htmx" in r.text.lower()


def test_headline_fragment(client):
    r = client.get("/ui/headline")
    assert r.status_code == 200
    assert "avoided cloud spend" in r.text


def test_queues_fragment(client):
    assert client.get("/ui/queues").status_code == 200


def test_fleet_fragment_renders_capabilities(client):
    # The conftest client runs from server/, so it loads the seed fleet.yaml.
    r = client.get("/ui/fleet")
    assert r.status_code == 200
    assert "8b-extract" in r.text


def test_reservations_fragment(client):
    r = client.get("/ui/reservations")
    assert r.status_code == 200
    assert "Reservations" in r.text
