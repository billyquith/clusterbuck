"""cbk — the clusterbuck worker agent.

Meets the coordinator only at the documented seams — the Redis queue contract and HTTP —
and is held to the same JSON Schema in `contract/` by a conformance suite, so it cannot
drift from the wire (ADR 22). Ships as one platform-independent `py3-none-any` zipapp
instead of six per-platform binaries (ADR 29).
"""

from .config import AGENT_FLAVOUR, AGENT_VERSION, PROTOCOL_VERSION

__all__ = ["AGENT_FLAVOUR", "AGENT_VERSION", "PROTOCOL_VERSION"]
__version__ = AGENT_VERSION
