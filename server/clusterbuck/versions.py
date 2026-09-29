"""Worker version governance: is this build fit to run jobs?

Protocol compatibility is necessary but **not sufficient**. A worker can speak the queue
contract perfectly and still carry bugs that produce plausible-looking wrong results — the
concrete example from this codebase's own history: a worker predating the `params.model`
requirement ignores the artifact pin, so eval jobs get measured on whatever model that node
defaults to and ability scores are attributed to the wrong artifact, silently, corrupting
routing. Nothing about the protocol version would catch that.

So the coordinator judges **fitness** from the worker's build version, against three policy
knobs:

* `current` — what the fleet should be running. Anything older is `stale` (usable, flagged).
* `minimum` — the supported floor. Below it, `quarantine`: stop claiming jobs.
* `blocked` — specific releases known to be broken. **Bugs are not monotonic**: 1.4.2 can be
  broken while 1.4.1 and 1.4.3 are fine, which a floor alone cannot express. This is the knob
  that actually answers "there might be bugs in a worker version".

Unset policy ⇒ everything is `ok`, so a fleet works before an operator has opinions.

**Enforcement is cooperative.** Workers claim straight from Redis (ADR 2, pull-based), so the
coordinator cannot hard-block one that ignores a quarantine — it can only refuse to hand it
work it controls, and tell it to stop. Real enforcement would need per-node Redis credentials
issued at enrollment and revocable here; see ADR 27.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

_log = logging.getLogger("clusterbuck.versions")

# The queue-contract protocol this coordinator speaks. Bump only for a genuinely breaking
# change to the Redis seam, since every older worker is quarantined by it.
PROTOCOL_VERSION = 1


def parse_version(v: str | None) -> tuple[int, ...] | None:
    """'1.4.2' -> (1, 4, 2). Returns None for anything unparseable, never raises."""
    if not v:
        return None
    core = v.strip().split("+")[0].split("-")[0]  # drop pre-release / build metadata
    parts = core.split(".")
    try:
        return tuple(int(p) for p in parts)
    except ValueError:
        return None


def _lt(a: tuple[int, ...], b: tuple[int, ...]) -> bool:
    """Compare padded to equal length, so 1.4 < 1.4.1."""
    n = max(len(a), len(b))
    return a + (0,) * (n - len(a)) < b + (0,) * (n - len(b))


def version_gt(a: tuple[int, ...], b: tuple[int, ...]) -> bool:
    """Strictly newer. The public form of `_lt`, for the update-offer gate.

    Exposed because "is this build newer than that one" is now a decision two places
    make — the fitness check here, and `_build_update_for`, which must never offer a node
    a version below the one it runs. Comparing the parsed tuples rather than the strings
    is the whole point: lexically, "0.9.0" sorts above "0.10.0".
    """
    return _lt(b, a)


@dataclass(frozen=True)
class VersionPolicy:
    current: str | None = None
    minimum: str | None = None
    blocked: frozenset[str] = frozenset()

    @property
    def configured(self) -> bool:
        return bool(self.current or self.minimum or self.blocked)


@dataclass(frozen=True)
class Fitness:
    status: str            # ok | stale | quarantine
    reason: str | None = None
    current_version: str | None = None

    def to_wire(self) -> dict:
        return {"status": self.status, "reason": self.reason,
                "current_version": self.current_version}


def assess(policy: VersionPolicy, *, agent_version: str | None,
           protocol_version: int | None) -> Fitness:
    """Decide whether a worker reporting these versions may run jobs."""
    # Skew first: a worker on an older queue contract is unfit regardless of its build,
    # because it may mishandle the payloads themselves (protocols.md §7).
    if protocol_version is not None and protocol_version < PROTOCOL_VERSION:
        return Fitness("quarantine", current_version=policy.current, reason=(
            f"speaks queue-contract protocol {protocol_version}, this coordinator requires "
            f"{PROTOCOL_VERSION}"))

    if not policy.configured:
        return Fitness("ok")

    if agent_version in policy.blocked:
        return Fitness("quarantine", current_version=policy.current, reason=(
            f"version {agent_version} is explicitly blocked as known-bad"))

    parsed = parse_version(agent_version)
    if parsed is None:
        # Cannot be shown to meet a declared floor, so refuse it rather than assume.
        if policy.minimum:
            return Fitness("quarantine", current_version=policy.current, reason=(
                f"reports no usable version ({agent_version!r}) and cannot be shown to meet "
                f"the {policy.minimum} floor"))
        return Fitness("stale", current_version=policy.current,
                       reason=f"reports no usable version ({agent_version!r})")

    minimum = parse_version(policy.minimum)
    if minimum and _lt(parsed, minimum):
        return Fitness("quarantine", current_version=policy.current, reason=(
            f"version {agent_version} is below the supported floor {policy.minimum}"))

    current = parse_version(policy.current)
    if current and _lt(parsed, current):
        return Fitness("stale", current_version=policy.current, reason=(
            f"version {agent_version} is behind the current release {policy.current}"))

    return Fitness("ok", current_version=policy.current)


def policy_from_settings(settings, release_path: str | None = None) -> VersionPolicy:
    from .release import released_version

    blocked = {
        v.strip() for v in (settings.worker_blocked_versions or "").split(",") if v.strip()
    }
    return VersionPolicy(
        # Unset: whatever is released. The two used to be set separately and drifted, and
        # a node judged against a version nobody was offering it could never be `ok`.
        current=(settings.worker_current_version
                 or released_version(release_path or settings.update_release)),
        minimum=settings.worker_min_version,
        blocked=frozenset(blocked),
    )
