"""Opaque id generation. Job and result ids share a suffix so one implies the other."""

from __future__ import annotations

import secrets
import uuid


def new_ids() -> tuple[str, str]:
    """Return (job_id, result_key) sharing a suffix."""
    suffix = uuid.uuid4().hex
    return f"job_{suffix}", f"res_{suffix}"


def new_reservation_id() -> str:
    return f"rsv_{uuid.uuid4().hex}"


def new_node_id() -> str:
    return f"node-{uuid.uuid4().hex[:8]}"


def new_node_key() -> str:
    return f"nk_{secrets.token_urlsafe(24)}"


def new_join_token() -> str:
    return f"jt_{secrets.token_urlsafe(18)}"
