"""How this worker makes HTTP connections, and the settings that make sleep survivable.

The sibling of `broker.py`, for the same reason and against the same failure. That module
explains why a machine which suspends leaves TCP connections dead but not reset, so a read
on one blocks until the kernel gives up — never, without a timeout. Redis was hardened
against that; HTTP was not, and it is the same socket on the same sleeping machine.

The gap cost a real outage. A node's model server stopped accepting connections while
still holding its listening socket, so every claim sat in `SYN_SENT`. The inference client
was built as `AsyncClient(timeout=600.0)`, and a single float in httpx sets **every**
timeout — connect included. So each job spent ten minutes failing to open a TCP connection
that a healthy peer answers in two milliseconds, then returned `ConnectTimeout`. The node
heartbeated "fine" throughout, because the heartbeat is a different connection: it claimed
work it could not do, ten minutes at a time, and the tier looked busy rather than broken.

The fix is not one number, because the timeouts here are not one concern:

* **Connecting** is fast or it is broken. A model server is on loopback or the LAN; if the
  handshake has not completed in a few seconds nothing is listening that will answer.
* **Reading** is slow by nature. A 70B generating a long answer legitimately takes minutes,
  and this is the one timeout that must stay generous — shortening it would abandon work
  that was about to succeed, which is the mistake in the other direction.

Conflating the two is what turned a dead peer into a ten-minute stall per job. They are
separated here, and every value is stated rather than inherited, because a client
library's defaults are not a contract (`broker.py`).
"""

from __future__ import annotations

import httpx

# Establishing a connection, as opposed to using one. The model server is loopback or a
# LAN address; a handshake that has not completed in five seconds is not slow, it is a
# peer that is not accepting. Same number and same reasoning as the broker's.
CONNECT_TIMEOUT_S = 5.0

# Waiting for a free connection out of the pool, rather than for the peer. Bounded for the
# same reason: a pool exhausted by requests that are themselves stuck must surface as an
# error, not as an unbounded wait behind them.
POOL_TIMEOUT_S = 5.0

# Sending the request body. A prompt is small and goes to a nearby host, so this is a
# transport-health bound, not a content one.
WRITE_TIMEOUT_S = 30.0

# How long an idle pooled connection may be reused before it is discarded and remade.
#
# This is the analogue of the broker's `health_check_interval`, and it is aimed squarely
# at suspend/resume. httpx has no "validate before reuse" hook, so the equivalent is to
# stop trusting an idle connection at all: the work loop polls every second, so a
# connection is only ever idle this long because the machine was not running. After a wake
# the pool is therefore rebuilt rather than a real request being posted into a socket the
# peer forgot about and waiting out the read timeout to find out.
KEEPALIVE_EXPIRY_S = 30.0

# Retries at the TRANSPORT layer, which in httpx means connection failures only — a
# request that reached the server is never replayed. That distinction is what makes this
# safe here: re-establishing a connection costs nothing, whereas re-sending a completion
# that may already be generating would spend the GPU twice and risk two answers to one
# job. So a flapping peer is ridden out, and a model server that returned an error is
# still the caller's problem to report.
CONNECT_RETRIES = 3


def _transport() -> httpx.AsyncHTTPTransport:
    """The shared transport settings.

    `limits` goes HERE, not on the AsyncClient. httpx applies a client-level `limits`
    only to the transport it builds itself, so passing both an explicit `transport=` and
    `limits=` silently drops the latter and leaves the pool on its defaults — which is a
    failure with no symptom until the thing you configured it for happens. A test asserts
    the value that actually reaches the pool, rather than the one that was passed in.
    """
    return httpx.AsyncHTTPTransport(
        retries=CONNECT_RETRIES,
        limits=httpx.Limits(keepalive_expiry=KEEPALIVE_EXPIRY_S),
    )


def inference_client(read_timeout_s: float) -> httpx.AsyncClient:
    """The client used for completions: patient about answers, not about connecting."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(
            read_timeout_s,            # generation genuinely takes this long
            connect=CONNECT_TIMEOUT_S,
            write=WRITE_TIMEOUT_S,
            pool=POOL_TIMEOUT_S,
        ),
        transport=_transport(),
    )


def control_client(read_timeout_s: float, **kwargs) -> httpx.AsyncClient:
    """Everything that is not inference — heartbeat, inventory, model management.

    Same hardening, because these cross the same network on the same sleeping machine.
    Only the read budget differs: a heartbeat should be quick, while pulling a multi-GB
    model legitimately is not, and the caller knows which it is doing.
    """
    return httpx.AsyncClient(
        timeout=httpx.Timeout(
            read_timeout_s,
            connect=CONNECT_TIMEOUT_S,
            write=WRITE_TIMEOUT_S,
            pool=POOL_TIMEOUT_S,
        ),
        transport=_transport(),
        **kwargs,
    )
