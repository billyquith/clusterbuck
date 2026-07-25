"""Model catalog & upgrade proposals (fleet-management.md → Workload awareness / Model
catalog & upgrade watching).

The coordinator decides, the worker executes (M6c). Everything here produces **proposals**,
never silent changes, because multi-GB weights must not appear on someone's laptop
unannounced. Three gates, in order:

1. **Fits** — the candidate's `min_ram_gb` fits the node and its `size_gb` fits the owner's
   disk quota (the machine profile's contract, ADR 10).
2. **Eval gate** — the candidate must beat the incumbent. `catalog.expected_ability` is an
   admin-curated *ranking hint* only; the authoritative number is **measured** ability
   (ADR 15), which for a not-yet-installed artifact doesn't exist yet — so an approved
   install triggers re-measurement (M6c) rather than inheriting the hint.
3. **Approval** — a human decides, unless the node has explicitly opted into
   `auto_approve`.

Proposal kinds:
- `upgrade` — a catalog artifact that fits and would raise ability for a task class.
- `reeval`  — an installed artifact's digest changed upstream: same name, new artifact, so
  its stored ability is stale and must be re-measured, not inherited.
- `reclaim` — an installed model that hasn't served a job in a while: give the owner their
  disk back. Install and reclaim ship together, or clusterbuck stops being a good guest.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone

from .evaluation import SCALE_VERSION, TASK_CLASSES
from .store import Store

_log = logging.getLogger("clusterbuck.catalog")

# Owner's default storage contract by profile (GB). Overridable per node.
PROFILE_DISK_QUOTA_GB = {"dedicated": 200.0, "shared": 50.0, "background": 20.0}

# A model unused for this long is a reclaim candidate.
RECLAIM_UNUSED_DAYS = 30

# Generic seed catalog: widely-available open models, described only by their public
# metadata. Sizes/RAM floors are approximate 4-bit-class figures for planning, not promises.
SEED_CATALOG = [
    {"artifact": "llama3.2:3b", "family": "llama3.2", "params_b": 3.0, "quant": "Q4_K_M",
     "size_gb": 2.0, "min_ram_gb": 8.0, "source": "ollama", "registry_ref": "llama3.2:3b",
     "expected_ability": 4.0},
    {"artifact": "llama3.1:8b", "family": "llama3.1", "params_b": 8.0, "quant": "Q4_K_M",
     "size_gb": 4.7, "min_ram_gb": 16.0, "source": "ollama", "registry_ref": "llama3.1:8b",
     "expected_ability": 5.0},
    {"artifact": "qwen2.5:14b", "family": "qwen2.5", "params_b": 14.0, "quant": "Q4_K_M",
     "size_gb": 9.0, "min_ram_gb": 24.0, "source": "ollama", "registry_ref": "qwen2.5:14b",
     "expected_ability": 6.0},
    {"artifact": "qwen2.5:32b", "family": "qwen2.5", "params_b": 32.0, "quant": "Q4_K_M",
     "size_gb": 20.0, "min_ram_gb": 48.0, "source": "ollama", "registry_ref": "qwen2.5:32b",
     "expected_ability": 7.0},
    {"artifact": "llama3.1:70b", "family": "llama3.1", "params_b": 70.0, "quant": "Q4_K_M",
     "size_gb": 40.0, "min_ram_gb": 64.0, "source": "ollama", "registry_ref": "llama3.1:70b",
     "expected_ability": 8.0},
]


def seed_catalog(store: Store, *, now: str) -> int:
    """Populate the catalog with generic defaults if empty. Returns rows written."""
    if store.catalog_count() > 0:
        return 0
    for entry in SEED_CATALOG:
        store.upsert_catalog(added_at=now, **entry)
    return len(SEED_CATALOG)


def quota_for(profile: str | None, explicit: float | None) -> float:
    if explicit is not None:
        return explicit
    return PROFILE_DISK_QUOTA_GB.get(profile or "shared", 50.0)


def fits(candidate, node, quota_gb: float) -> tuple[bool, str]:
    """Gate 1: hardware + the owner's disk contract."""
    ram = node["ram_gb"] or 0
    if candidate["min_ram_gb"] > ram:
        return False, f"needs {candidate['min_ram_gb']:g} GB RAM, node has {ram:g}"
    if candidate["size_gb"] > quota_gb:
        return False, f"{candidate['size_gb']:g} GB exceeds {quota_gb:g} GB disk quota"
    return True, "fits"


def _new_id() -> str:
    return f"prop_{uuid.uuid4().hex[:12]}"


def scan_node(store: Store, node, *, now: str, scale_version: str = SCALE_VERSION) -> list[str]:
    """Generate proposals for one node. Returns the ids created."""
    import json as _json

    created: list[str] = []
    node_id = node["node_id"]
    installed = set(_json.loads(node["installed"] or "[]"))
    quota = quota_for(node["profile"], node["disk_quota_gb"])
    auto = bool(node["auto_approve"])
    initial_status = "approved" if auto else "pending"

    # --- upgrade: a fitting catalog artifact that would raise ability somewhere ---
    for cand in store.list_catalog():
        artifact = cand["artifact"]
        if artifact in installed:
            continue
        ok, why = fits(cand, node, quota)
        if not ok:
            continue
        hint = cand["expected_ability"]
        if hint is None:
            continue
        # Best measured ability the node can currently reach, per task class.
        best_gain: tuple[float, str, float] | None = None  # (gain, task_class, incumbent)
        for task_class in TASK_CLASSES:
            incumbent_best = 0.0
            for have in installed:
                score = store.get_ability(have, task_class, scale_version)
                if score is not None:
                    incumbent_best = max(incumbent_best, score)
            gain = hint - incumbent_best
            if gain > 0 and (best_gain is None or gain > best_gain[0]):
                best_gain = (gain, task_class, incumbent_best)
        if best_gain is None:
            continue
        if store.open_proposal_exists(kind="upgrade", node_id=node_id, artifact=artifact):
            continue
        gain, task_class, incumbent_best = best_gain
        pid = _new_id()
        store.insert_proposal(
            id=pid, kind="upgrade", node_id=node_id, artifact=artifact,
            incumbent=None, task_class=task_class, status=initial_status, created_at=now,
            rationale=(
                f"{artifact} ({cand['size_gb']:g} GB, needs {cand['min_ram_gb']:g} GB RAM) "
                f"{why}; expected ability {hint:g} for {task_class} vs best installed "
                f"{incumbent_best:g} (+{gain:g}). Ability is re-measured after install."
            ),
        )
        created.append(pid)

    # --- reclaim: installed models that haven't earned their disk lately ---
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RECLAIM_UNUSED_DAYS)).strftime("%Y-%m-%d")
    used = store.models_used_since(cutoff)
    for artifact in sorted(installed):
        if artifact in used:
            continue
        if store.open_proposal_exists(kind="reclaim", node_id=node_id, artifact=artifact):
            continue
        pid = _new_id()
        store.insert_proposal(
            id=pid, kind="reclaim", node_id=node_id, artifact=artifact, incumbent=None,
            task_class=None, status="pending",  # never auto-delete an owner's data
            created_at=now,
            rationale=(f"{artifact} has served no jobs in {RECLAIM_UNUSED_DAYS} days; "
                       f"removing it would return disk to the owner."),
        )
        created.append(pid)

    return created


def propose_reeval(store: Store, node_id: str, artifact: str, old: str | None,
                   new: str | None, *, now: str) -> str | None:
    """A digest change means a new artifact — its stored ability is stale (ADR 15)."""
    if store.open_proposal_exists(kind="reeval", node_id=node_id, artifact=artifact):
        return None
    pid = _new_id()
    store.insert_proposal(
        id=pid, kind="reeval", node_id=node_id, artifact=artifact, incumbent=None,
        task_class=None, status="pending", created_at=now,
        rationale=(f"{artifact} changed upstream ({(old or '?')[:19]}… → {(new or '?')[:19]}…). "
                   f"Ability is pinned to an artifact, so the stored score is stale and must "
                   f"be re-measured rather than inherited."),
    )
    _log.info("reeval proposed for %s on %s (digest changed)", artifact, node_id)
    return pid


def scan_all(store: Store, *, now: str) -> list[str]:
    created: list[str] = []
    for node in store.list_nodes():
        created += scan_node(store, node, now=now)
    return created
