"""cbk — the clusterbuck worker agent, Python implementation.

One of two interchangeable workers. The C# worker in `worker/dotnet` is the reference
implementation, not a constraint (ADR 7): both meet the coordinator only at the documented
seams — the Redis queue contract and HTTP — and both are held to the same JSON Schema in
`contract/` by their own conformance suites, so neither can drift from the wire (ADR 22).

This one exists because it is one platform-independent artifact instead of six per-platform
binaries, and a fraction of the size (ADR 29).
"""

from .config import AGENT_FLAVOUR, AGENT_VERSION, PROTOCOL_VERSION

__all__ = ["AGENT_FLAVOUR", "AGENT_VERSION", "PROTOCOL_VERSION"]
__version__ = AGENT_VERSION
