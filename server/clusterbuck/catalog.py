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
    # Candidates a fresh install may propose. Sizes are the summed layer bytes from the
    # Ollama registry manifest, not estimates from the parameter count — `size_gb` gates
    # against the owner's disk quota, so guessing it high silently excludes a model
    # everywhere and guessing low proposes a pull the node cannot hold.
    #
    # `min_ram_gb` keeps the ~2.4x-of-size convention the original seed used. What matters
    # is that the ratio stays consistent across entries, since it is a relative gate.
    #
    # `expected_ability` only RANKS candidates for the planner (ADR 15). Routing uses
    # measured ability, which a not-yet-installed artifact does not have — so these are
    # ordering hints, and a wrong one costs an eval, not a bad route.
    #
    # The capability fields, by contrast, ARE load-bearing at routing time (ADR 37): they
    # are hard yes/no facts no ability score can express, and a job requiring one is
    # refused rather than served by an artifact that does not declare it. `context_tokens`
    # is the model's published window, not whatever a given server happens to be
    # configured with — a runtime that was started with a smaller one will error, which is
    # visible, whereas routing a long document to a model that cannot hold it is not.
    #
    # Families are deliberately mixed. A catalog drawn from one vendor leaves a fleet with
    # nowhere to go when that line stalls or a tag is pulled.
    #
    # NOTE: this list only seeds an EMPTY table, so it is the floor for a new install and
    # never an update path. Curate a running fleet through `POST /catalog`.

    # A sub-1B entry matters on RAM-tight nodes, where nothing larger fits at all.
    {"artifact": "qwen3:0.6b", "family": "qwen3", "params_b": 0.6, "quant": "Q4_K_M",
     "size_gb": 0.5, "min_ram_gb": 2.0, "source": "ollama", "registry_ref": "qwen3:0.6b",
     "expected_ability": 2.5,
     "context_tokens": 32768, "supports_tools": True,
     "supports_json_schema": True, "supports_vision": False},
    # The example fleet in fleet.yaml serves this one; keeping it in the catalog is what
    # lets reeval and reclaim reason about it.
    {"artifact": "llama3.2:3b", "family": "llama3.2", "params_b": 3.0, "quant": "Q4_K_M",
     "size_gb": 2.0, "min_ram_gb": 8.0, "source": "ollama", "registry_ref": "llama3.2:3b",
     "expected_ability": 4.0,
     "context_tokens": 131072, "supports_tools": True,
     "supports_json_schema": True, "supports_vision": False},
    {"artifact": "gemma3:4b", "family": "gemma3", "params_b": 4.0, "quant": "Q4_K_M",
     "size_gb": 3.3, "min_ram_gb": 8.0, "source": "ollama", "registry_ref": "gemma3:4b",
     "expected_ability": 4.5,
     "context_tokens": 131072, "supports_tools": False,
     "supports_json_schema": True, "supports_vision": True},
    {"artifact": "qwen3:8b", "family": "qwen3", "params_b": 8.0, "quant": "Q4_K_M",
     "size_gb": 5.2, "min_ram_gb": 16.0, "source": "ollama", "registry_ref": "qwen3:8b",
     "expected_ability": 5.5,
     "context_tokens": 131072, "supports_tools": True,
     "supports_json_schema": True, "supports_vision": False},
    {"artifact": "qwen3:14b", "family": "qwen3", "params_b": 14.0, "quant": "Q4_K_M",
     "size_gb": 9.3, "min_ram_gb": 24.0, "source": "ollama", "registry_ref": "qwen3:14b",
     "expected_ability": 6.5,
     "context_tokens": 131072, "supports_tools": True,
     "supports_json_schema": True, "supports_vision": False},
    {"artifact": "mistral-small3.2:24b", "family": "mistral-small3.2", "params_b": 24.0,
     "quant": "Q4_K_M", "size_gb": 15.2, "min_ram_gb": 32.0, "source": "ollama",
     "registry_ref": "mistral-small3.2:24b", "expected_ability": 6.8,
     "context_tokens": 131072, "supports_tools": True,
     "supports_json_schema": True, "supports_vision": True},
    # Mixture-of-experts: 30B total, ~3B active per token. `active_params_b` is what lets
    # the fits gate see that overflowing VRAM is not the penalty here that it would be for
    # a dense model of the same size — measured on real hardware, a partially-resident
    # 30B-A3B beat the same artifact fully resident on a slower accelerator (ADR 39).
    {"artifact": "qwen3:30b-a3b", "family": "qwen3", "params_b": 30.0,
     "active_params_b": 3.0, "quant": "Q4_K_M",
     "size_gb": 18.6, "min_ram_gb": 40.0, "source": "ollama",
     "registry_ref": "qwen3:30b-a3b", "expected_ability": 7.0,
     "context_tokens": 131072, "supports_tools": True,
     "supports_json_schema": True, "supports_vision": False},
    {"artifact": "qwen2.5:32b", "family": "qwen2.5", "params_b": 32.0, "quant": "Q4_K_M",
     "size_gb": 20.0, "min_ram_gb": 48.0, "source": "ollama", "registry_ref": "qwen2.5:32b",
     "expected_ability": 7.0,
     "context_tokens": 32768, "supports_tools": True,
     "supports_json_schema": True, "supports_vision": False},
    {"artifact": "llama3.1:70b", "family": "llama3.1", "params_b": 70.0, "quant": "Q4_K_M",
     "size_gb": 40.0, "min_ram_gb": 64.0, "source": "ollama", "registry_ref": "llama3.1:70b",
     "expected_ability": 8.0,
     "context_tokens": 131072, "supports_tools": True,
     "supports_json_schema": True, "supports_vision": False},
    {"artifact": "llama3.3:70b", "family": "llama3.3", "params_b": 70.0, "quant": "Q4_K_M",
     "size_gb": 42.5, "min_ram_gb": 64.0, "source": "ollama", "registry_ref": "llama3.3:70b",
     "expected_ability": 8.2,
     "context_tokens": 131072, "supports_tools": True,
     "supports_json_schema": True, "supports_vision": False},
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


# Weights are not the whole footprint: the KV cache, activations and the runtime's own
# buffers have to live in the same memory. A model whose file exactly equals the card's
# capacity does not fit in it. This headroom is deliberately modest — it decides only
# whether a candidate is proposed as FAST or as one that will spill, never whether it is
# proposed at all.
VRAM_HEADROOM = 1.2

# How much smaller the active set must be before an artifact counts as a mixture-of-experts
# for the fits gate. A model activating most of itself per token behaves like a dense one,
# whatever its architecture is called, so the threshold is about behaviour rather than
# nomenclature.
MOE_ACTIVE_FRACTION = 0.5


def _is_moe(candidate) -> bool:
    """Whether this artifact activates materially fewer parameters than it holds.

    Requires BOTH figures and a real gap: an artifact that merely omits `active_params_b`
    is treated as dense, because assuming otherwise would excuse a genuinely oversized
    dense model from a gate that exists to catch exactly that.
    """
    total = getattr(candidate, "params_b", None)
    active = getattr(candidate, "active_params_b", None)
    return bool(total and active and active < total * MOE_ACTIVE_FRACTION)


def fits(candidate, node, quota_gb: float) -> tuple[str, str]:
    """Gate 1: hardware + the owner's disk contract.

    Returns a verdict, not a boolean, because "will it run" and "will it run well" are
    different questions and only the second one is worth acting on:

    * `"fast"`     — fits in the accelerator's memory; runs at device speed.
    * `"degraded"` — fits in system RAM but overflows the accelerator, so layers cross the
      bus every token. It runs, often an order of magnitude slower. Worth proposing only
      when nothing better exists, and never without saying so.
    * `"ok"`       — fits in RAM on a node with no accelerator (or none we could measure),
      which is simply what that node's speed is. Not a degradation.
    * `"no"`       — does not fit, or breaks the owner's disk contract.

    A boolean could not tell `fast` from `degraded`, so the gate approved a 70B onto a
    workstation with an 8 GB card exactly as readily as onto one that could hold it.
    """
    ram = node.ram_gb or 0
    if candidate.min_ram_gb > ram:
        return "no", f"needs {candidate.min_ram_gb:g} GB RAM, node has {ram:g}"
    if candidate.size_gb > quota_gb:
        return "no", f"{candidate.size_gb:g} GB exceeds {quota_gb:g} GB disk quota"

    vram = getattr(node, "vram_gb", None)
    # None means "no accelerator, or we could not measure one" — NOT zero. Judging such a
    # node by RAM is right; calling it degraded would libel every CPU node in the fleet.
    if not vram:
        return "ok", "fits"
    needed = candidate.size_gb * VRAM_HEADROOM
    if needed <= vram:
        return "fast", f"fits in {vram:g} GB VRAM"

    # Overflowing the accelerator is only reliably a penalty for a DENSE model, where every
    # parameter is read for every token and the spilled share crosses the bus each time. A
    # mixture-of-experts reads only its active parameters, so most of what sits in system
    # RAM is untouched per token.
    #
    # This is measured, not assumed. A 30B-A3B (~3B active) held 54% on a 12 GB card ran at
    # 71 tok/s, while the same artifact fully resident in 48 GB on another node managed 52.
    # The partially-resident node was the FASTER one. Calling that "degraded" told the
    # operator to avoid the best configuration in the fleet (ADR 39).
    if _is_moe(candidate):
        return "ok", (
            f"{candidate.size_gb:g} GB exceeds {vram:g} GB VRAM, but only "
            f"~{candidate.active_params_b:g}B of {candidate.params_b:g}B parameters are "
            f"active per token, so partial residency is not reliably a penalty — measured "
            f"throughput decides"
        )
    return "degraded", (
        f"{candidate.size_gb:g} GB needs ~{needed:.1f} GB with overhead but the "
        f"accelerator has {vram:g} GB — it will run from system RAM and be far slower"
    )


def _new_id() -> str:
    return f"prop_{uuid.uuid4().hex[:12]}"


def scan_node(store: Store, node, *, now: str, scale_version: str = SCALE_VERSION) -> list[str]:
    """Generate proposals for one node. Returns the ids created."""
    import json as _json

    created: list[str] = []
    node_id = node.node_id
    installed = set(_json.loads(node.installed or "[]"))
    quota = quota_for(node.profile, node.disk_quota_gb)
    auto = bool(node.auto_approve)
    initial_status = "approved" if auto else "pending"

    # --- upgrade: a fitting catalog artifact that would raise ability somewhere ---
    for cand in store.list_catalog():
        artifact = cand.artifact
        if artifact in installed:
            continue
        verdict, why = fits(cand, node, quota)
        if verdict == "no":
            continue
        hint = cand.expected_ability
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
        # A candidate that will spill off the accelerator is still worth OFFERING — it may
        # be the only thing that raises ability on this node — but never silently. The
        # warning leads the rationale so the human approving sees it before the numbers.
        warning = "WILL RUN SLOWLY: " if verdict == "degraded" else ""
        # `auto_approve` is consent to routine upgrades, not to spending a multi-GB
        # download and the owner's disk on something that will then crawl. That trade is a
        # judgement call, so it goes to a human even on an opted-in node — the same reason
        # reclaim is never auto-approved.
        status = "pending" if verdict == "degraded" else initial_status
        store.insert_proposal(
            id=pid, kind="upgrade", node_id=node_id, artifact=artifact,
            incumbent=None, task_class=task_class, status=status, created_at=now,
            rationale=(
                f"{warning}{artifact} ({cand.size_gb:g} GB, needs {cand.min_ram_gb:g} GB "
                f"RAM) {why}; expected ability {hint:g} for {task_class} vs best installed "
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
    """Flag an artifact as needing measurement (ADR 15).

    Two callers: a digest change (same name, new artifact ⇒ stored score is stale), and a
    fresh install (never measured here). Both mean "do not trust an inherited number".
    """
    if store.open_proposal_exists(kind="reeval", node_id=node_id, artifact=artifact):
        return None
    if old and new:
        why = (f"{artifact} changed upstream ({old[:19]}… → {new[:19]}…). Ability is pinned "
               f"to an artifact, so the stored score is stale and must be re-measured "
               f"rather than inherited.")
    else:
        why = (f"{artifact} was just installed and has no measured ability on this scale; "
               f"run the task-class suite before routing depends on it.")
    pid = _new_id()
    store.insert_proposal(
        id=pid, kind="reeval", node_id=node_id, artifact=artifact, incumbent=None,
        task_class=None, status="pending", created_at=now, rationale=why,
    )
    _log.info("reeval proposed for %s on %s", artifact, node_id)
    return pid


def install_allowed(mode: str, profile: str | None) -> bool:
    """Whether a multi-GB transfer is acceptable right now.

    A dedicated machine exists to serve, so anytime. On a machine someone uses, pull only
    while they're `away` — saturating the network/disk under an active owner is exactly the
    rudeness ADR 10 exists to prevent. `paused` means the owner opted out: nothing.
    """
    if profile == "dedicated":
        return mode != "paused"
    return mode == "away"


def next_action(store: Store, node, *, mode: str) -> dict | None:
    """The next approved action this node should carry out, if any is permitted now."""
    node_id = node.node_id

    # Removals are cheap and free disk — allowed in any non-paused mode.
    if mode != "paused":
        for prop in store.approved_proposals_for_node(node_id, "reclaim"):
            return {"proposal_id": prop.id, "kind": "remove",
                    "artifact": prop.artifact, "source": "ollama"}

    if not install_allowed(mode, node.profile):
        return None
    for prop in store.approved_proposals_for_node(node_id, "upgrade"):
        cand = next((c for c in store.list_catalog() if c.artifact == prop.artifact), None)
        if cand is None:
            continue
        return {"proposal_id": prop.id, "kind": "install",
                "artifact": prop.artifact, "registry_ref": cand.registry_ref,
                "source": cand.source}
    return None


def apply_action_result(store: Store, node_id: str, result, *, now: str) -> list[str]:
    """Record an action's outcome. A successful install leaves the artifact's ability
    UNKNOWN, so it earns a re-eval proposal rather than inheriting anyone's score."""
    notes: list[str] = []
    prop = store.get_proposal(result.proposal_id)
    if prop is None:
        return notes
    # A node may only report on its OWN actions. The heartbeat proves which node is
    # speaking (node_key), but the proposal id is just a string in the body — unchecked, any
    # enrolled node could mark another node's install `applied` (recording an install that
    # never happened, and triggering a re-eval proposal for it) or `failed` (cancelling one
    # that was approved). Nodes are not adversaries here, but a confused or restored-from-
    # backup node reporting a stale proposal_id is entirely ordinary.
    if prop.node_id != node_id:
        _log.warning("node %s reported on proposal %s, which belongs to %s — ignored",
                     node_id, prop.id, prop.node_id)
        return notes
    if not result.ok:
        store.set_proposal_status(prop.id, "failed")
        notes.append(f"{prop.artifact}: {prop.kind} failed — {result.error}")
        _log.warning("action %s failed on %s: %s", prop.id, node_id, result.error)
        return notes

    store.set_proposal_status(prop.id, "applied")
    notes.append(f"{prop.artifact}: {prop.kind} applied")
    if prop.kind == "upgrade":
        # Newly installed ⇒ unmeasured on this scale. Flag it for measurement; running the
        # tier-1 suite as ordinary jobs (model-evaluation.md) is the next step.
        if propose_reeval(store, node_id, prop.artifact, None, None, now=now):
            notes.append(f"{prop.artifact}: ability unmeasured — evaluation proposed")
    return notes


def scan_all(store: Store, *, now: str) -> list[str]:
    created: list[str] = []
    for node in store.list_nodes():
        created += scan_node(store, node, now=now)
    return created
