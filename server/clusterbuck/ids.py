"""Opaque id generation. Job and result ids share a suffix so one implies the other."""

from __future__ import annotations

import uuid


def new_ids() -> tuple[str, str]:
    """Return (job_id, result_key) sharing a suffix."""
    suffix = uuid.uuid4().hex
    return f"job_{suffix}", f"res_{suffix}"
