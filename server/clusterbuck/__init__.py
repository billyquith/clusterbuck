"""clusterbuck server (cbk-server) — the always-on half: job API + coordinator.

M0 scope: the async plane's submit/poll API over a Redis Streams queue, with SQLite
as the durable job registry. Sync plane (LiteLLM), coordinator (WoL), and the
self-managing-fleet layer arrive in later milestones.
"""

__version__ = "0.0.1"
