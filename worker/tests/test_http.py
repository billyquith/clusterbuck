"""HTTP connection settings (`http.py`) — the sibling of broker.py's, and why.

The incident these encode: a node's model server kept its listening socket but stopped
accepting, so every connection sat in SYN_SENT. The inference client was
`AsyncClient(timeout=600.0)`, and one float in httpx sets EVERY timeout — connect
included. Each job therefore spent ten minutes failing to open a TCP connection that a
healthy peer answers in two milliseconds, while the node heartbeated "fine" on a separate
connection and kept claiming work it could not do.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from cbk_worker import http as cbk_http


def test_connecting_is_bounded_far_below_the_read_budget():
    """The whole bug in one assertion. Reading may take ten minutes because generation
    does; CONNECTING may not, because a model server is on loopback or the LAN."""
    c = cbk_http.inference_client(600.0)
    t = c.timeout
    assert t.read == 600.0, "a long generation must still be allowed to finish"
    assert t.connect == 5.0
    assert t.pool == 5.0
    assert t.connect < t.read / 50, "connect must not scale with the read budget"


def test_a_single_float_would_have_set_them_all():
    """Pins the httpx behaviour the bug depended on, so nobody 'simplifies' the Timeout
    back into one number without seeing what it does."""
    naive = httpx.Timeout(600.0)
    assert naive.connect == 600.0 and naive.read == 600.0


def test_idle_pooled_connections_are_not_trusted_across_a_suspend():
    """httpx has no validate-before-reuse hook, so the analogue of the broker's
    `health_check_interval` is to expire idle connections instead. The work loop polls
    every second, so a connection idle this long means the machine was not running."""
    c = cbk_http.inference_client(600.0)
    assert c._transport._pool._keepalive_expiry == 30.0


def test_connection_failures_retry_but_requests_never_replay():
    """Transport-level retries in httpx cover connection establishment ONLY. That is what
    makes them safe for inference: reconnecting costs nothing, whereas replaying a
    completion could spend the GPU twice and produce two answers to one job."""
    c = cbk_http.inference_client(600.0)
    assert c._transport._pool._retries == 3


def test_control_clients_get_the_same_hardening_with_their_own_read_budget():
    """Heartbeat and model-management cross the same network on the same sleeping
    machine. Only the read budget differs — a beat should be quick, a multi-GB pull
    legitimately is not."""
    beat, mgr = cbk_http.control_client(30.0), cbk_http.control_client(3600.0)
    assert (beat.timeout.read, mgr.timeout.read) == (30.0, 3600.0)
    for c in (beat, mgr):
        assert c.timeout.connect == 5.0 and c.timeout.pool == 5.0


async def test_a_peer_that_never_answers_a_handshake_fails_in_seconds_not_minutes():
    """End to end, in the failure mode that caused the outage: packets to the peer vanish,
    so the handshake sits in SYN_SENT forever. Before this, such a peer cost the full READ
    timeout per job — ten minutes — because one float had set connect to 600s too.

    192.0.2.0/24 is TEST-NET-1 (RFC 5737): reserved for documentation and routed nowhere,
    which is the portable way to get a black hole rather than a refusal. Some networks
    answer it with an immediate unreachable instead of dropping; that fails in
    milliseconds and has not created the condition under test, so it is skipped rather
    than passed.
    """
    blackhole = "http://192.0.2.1:9/v1/models"

    async with cbk_http.inference_client(600.0) as c:     # production read budget
        started = time.monotonic()
        with pytest.raises((httpx.ConnectTimeout, httpx.ConnectError)):
            # Bounded independently of the client, so a regression is a failure rather
            # than a hung suite.
            await asyncio.wait_for(c.get(blackhole), timeout=90)
        elapsed = time.monotonic() - started

    if elapsed < 1.0:
        pytest.skip(f"network refused TEST-NET in {elapsed:.2f}s — no black hole to test")
    # 5s connect x (1 + 3 retries) is the ceiling. The point is that it is bounded by the
    # CONNECT budget and nowhere near the 600s read budget that used to apply.
    assert elapsed < 60, f"took {elapsed:.1f}s — the connect bound did not apply"
