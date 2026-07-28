"""Presence-mode model ladder (ADR 10).

A shared machine runs a small model while its user is `active` and swaps in the large ones
when `away`. Climbing the ladder (active → away) is damped by hysteresis because cold-loading
a big model is expensive; descending (away → active) and pausing are immediate, because
eviction is cheap and the owner's machine is the owner's.

This is the ladder *logic*, driven by a supplied desired mode. Detecting presence from the OS
(screen lock / input idle) is deferred; the mode is set manually (env / CLI / `cbk pause`),
which is also the honest fallback for machines without a clean signal.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence


class PresenceLadder:
    def __init__(self, ladder: dict[str, list[str]] | None,
                 all_capabilities: Sequence[str],
                 hysteresis_s: float = 120.0,
                 clock: Callable[[], float] | None = None) -> None:
        self._ladder = ladder or {}
        self._all = list(all_capabilities)
        self._hysteresis_s = hysteresis_s
        self._clock = clock or time.monotonic
        self._effective = "active"
        self._away_pending_since: float | None = None

    @property
    def effective_mode(self) -> str:
        return self._effective

    def update(self, desired: str) -> str:
        """Feed the desired mode; returns the effective mode after hysteresis."""
        if desired in ("paused", "active"):
            self._effective = desired          # descend / pause immediately
            self._away_pending_since = None
        elif desired == "away":
            if self._effective == "away":
                return self._effective
            now = self._clock()
            if self._away_pending_since is None:
                self._away_pending_since = now
            if now - self._away_pending_since >= self._hysteresis_s:
                self._effective = "away"       # climbed after staying away long enough
                self._away_pending_since = None
        return self._effective

    def capabilities(self) -> list[str]:
        """Capabilities to serve in the current effective mode (empty when paused)."""
        if self._effective == "paused":
            return []
        if self._effective in self._ladder:
            return list(self._ladder[self._effective])
        return list(self._all)      # no ladder entry ⇒ serve everything this node can
