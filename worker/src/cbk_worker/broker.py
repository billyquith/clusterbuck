"""How this worker connects to the broker, and the settings that make sleep survivable.

Every value here is stated rather than inherited, and that is the whole point of the
module. `Redis.from_url(url, decode_responses=True)` produced a *working* client on the
redis-py we happen to vendor, because its modern defaults (a 5s socket timeout, keepalive
on, ten retries over connection errors) are close to what a sleeping fleet needs. On
redis-py 5.x — which `redis>=5.0` permitted — the same call gives `socket_timeout=None`,
`socket_keepalive=False` and retries effectively off.

That difference is not cosmetic, and the failure it produces is the worst kind. A machine
that suspends does not close its TCP connections; on wake the socket is dead but not
reset, so a read on it blocks until the kernel gives up, which without a socket timeout is
never. The worker would then sit alive and silent: claiming nothing, and crashing nowhere,
so neither launchd's `KeepAlive` nor systemd's `Restart=always` would notice. A zombie is
strictly worse than a crash — a crash is recovered in seconds.

For a component whose headline use case is laptops that sleep, the behaviour that makes
that work belongs at the call site with its reasons, not in a dependency's changelog.
"""

from __future__ import annotations

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from .config import normalise_redis_url

# Cap on any single command. The one number that turns "hangs forever after a wake" into
# "fails in five seconds and reconnects", so it must never be None. Comfortably above a
# LAN round trip to Redis, and nothing on the pull path is a blocking read — `XREADGROUP`
# is called without BLOCK precisely so no normal operation can outlast this.
SOCKET_TIMEOUT_S = 5.0

# Establishing a connection, as opposed to using one. Same value; a broker that cannot be
# reached in five seconds is down as far as this worker is concerned, and the retry below
# is what decides how patient to be about that.
CONNECT_TIMEOUT_S = 5.0

# PING a pooled connection that has been idle longer than this before reusing it. This is
# the setting aimed squarely at suspend/resume: the work loop polls every second, so a
# connection is only ever idle this long because the machine was not running. The first
# command after a wake therefore validates the socket and reconnects, instead of sending a
# real claim into a dead one and waiting out the timeout above to discover it.
HEALTH_CHECK_INTERVAL_S = 30.0

# TCP keepalive, so a connection the peer forgot about (a coordinator reboot while this
# node slept) is detected by the kernel rather than on next use.
KEEPALIVE = True

# Retries for a connection-level failure, with exponential backoff capped at a second, so
# a wake storm does not become a reconnect storm. Ten attempts over roughly ten seconds:
# long enough to ride out a broker restart or a Wi-Fi reassociation, short enough that a
# genuinely absent broker still surfaces as an error the loop can log and back off from.
RETRIES = 10
BACKOFF_CAP_S = 1.0
BACKOFF_BASE_S = 0.01

# Spelled out rather than left to `Retry`'s default, which is currently this exact tuple.
# The default is the thing this module exists to stop depending on.
RETRY_ON = (RedisConnectionError, RedisTimeoutError)


def connect(url: str) -> Redis:
    """The worker's broker client, configured for a machine that sleeps."""
    return Redis.from_url(
        normalise_redis_url(url),
        decode_responses=True,
        socket_timeout=SOCKET_TIMEOUT_S,
        socket_connect_timeout=CONNECT_TIMEOUT_S,
        socket_keepalive=KEEPALIVE,
        health_check_interval=HEALTH_CHECK_INTERVAL_S,
        retry=Retry(
            ExponentialBackoff(cap=BACKOFF_CAP_S, base=BACKOFF_BASE_S),
            RETRIES,
            supported_errors=RETRY_ON,
        ),
    )
