"""Which models each node should hold — advice, never action.

The planner in `catalog.py` answers a narrow question once per candidate: "would this raise
ability on this node in SOME task class?" It writes the answer down and never revisits it,
so a proposal made before anything was measured keeps saying "vs best installed 0" long
after the node holds a measured model that beats the candidate. And it ranks nothing: every
pending upgrade is presented as equally worth approving.

This module answers the question an operator actually has — "what should be on this
machine, given what clients send and what we know about each model?" — and recomputes it
on every read from live data, so it cannot go stale:

* **Demand-weighted.** A model is valued by its ability in the classes clients actually
  ask for, blended with an even prior (`DEMAND_PRIOR_JOBS`). Early traffic is thin and
  skewed; the prior keeps a few dozen extract jobs from declaring code worthless, and
  fades as real volume grows. The mix is recomputed every read, so the advice moves as the
  workload does — this is meant to be reviewed weekly, not settled once.
* **Evidence over promises.** Ability belongs to the artifact, not the node, so a score
  measured anywhere counts everywhere. A catalog hint is clamped to the instrument ceiling
  before it is compared with a measurement: the checker cannot record above
  `TIER1_MAX_ABILITY`, so an unclamped 8 would "win" by a margin nothing can verify.
* **A slate, not a single pick.** A node holds up to `SLATE_SIZE` models: a primary for
  the demand mix, a model covering a class the primary is weak at, and — on a machine
  someone else uses — a light model that serves while its owner is busy.
* **"Keep what you have" is a result.** A candidate must beat the incumbent by
  `MIN_GAIN` before it is recommended. With every measured model near the ceiling, most
  differences are noise, and recommending a 40 GB download for 0.1 of a point would be
  worse than recommending nothing.

It reads the store and routes nothing. Approval still goes through proposals.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from .catalog import fits, quota_for
from .evaluation import TASK_CLASSES, TIER1_MAX_ABILITY

# Real demand is read over the same window reclaim uses, so "unused" and "wanted" are
# judged against the same stretch of history.
DEMAND_WINDOW_DAYS = 30

# Pseudo-jobs spread evenly across the task classes before real demand is added. At 25 per
# class, 100 real jobs move the mix halfway from even; at 1,000 the prior is noise. Low
# enough that genuine demand shows through within a week of real use, high enough that a
# development-stage trickle cannot declare a class irrelevant.
DEMAND_PRIOR_JOBS = 25

# The smallest demand-weighted gain worth a download. Tier-1 scores move in half points,
# so anything under one step is inside the instrument's own granularity.
MIN_GAIN = 0.5

SLATE_SIZE = 3

# A light model must be at most this share of the primary's size to earn a slot: the
# point of it is to stay usable while the owner is on the machine, which a near-copy of
# the primary is not.
LIGHT_MAX_SIZE_FRACTION = 0.5

# ...and may give up at most this much demand-weighted ability to do so. Any further and
# it is not a lighter way to serve the same work, it is a different, worse model.
LIGHT_MAX_LOSS = 1.5

# Profiles whose owner uses the machine for something else. A dedicated node has no owner
# to make room for, so the light slot would only spend its disk.
_OWNER_PROFILES = {"shared", "background"}

_FIT_RANK = {"fast": 0, "ok": 1, "degraded": 2}


@dataclass
class Candidate:
    artifact: str
    installed: bool
    scores: dict[str, float]
    evidence: str               # "measured" | "hint" | "seed"
    value: float                # demand-weighted ability
    fit: str                    # fast | ok | degraded | "running" (installed, uncurated)
    fit_reason: str
    size_gb: float | None
    active_params_b: float | None
    measured_on: list[str] = field(default_factory=list)
    tiers: list[str] = field(default_factory=list)  # tiers whose registered model this is


@dataclass
class Pick:
    role: str                   # primary | coverage | light
    candidate: Candidate
    why: str

    @property
    def action(self) -> str:
        return "keep" if self.candidate.installed else "install"


@dataclass
class NodeAdvice:
    node_id: str
    name: str
    profile: str | None
    slate: list[Pick]
    ranked: list[Candidate]     # every viable candidate, best first
    incumbent: Candidate | None
    unused: list[str]           # installed models the slate does not keep
    summary: str
    offline_s: float | None = None      # set when silent past OFFLINE_AFTER_S
    last_known: list[str] = field(default_factory=list)  # what it last reported holding


def demand_weights(by_class: dict[str, int]) -> dict[str, float]:
    total = sum(by_class.get(tc, 0) for tc in TASK_CLASSES)
    denom = total + DEMAND_PRIOR_JOBS * len(TASK_CLASSES)
    return {tc: (by_class.get(tc, 0) + DEMAND_PRIOR_JOBS) / denom for tc in TASK_CLASSES}


def _speed_key(c: Candidate) -> float:
    """Higher is faster: fewer active parameters per token, which is what bounds decode
    speed on memory-bandwidth-limited hardware (ADR 39). Unknown sorts last.

    Not the throughput derived from job timestamps. That span runs from claim to finish,
    so it carries cold loads and prefill: on the live fleet it put a 3B at 2 tok/s and a
    30B-A3B at 26 — the opposite of their real decode speeds. Ranking on it would prefer
    whichever model happened to be warm."""
    if c.active_params_b:
        return 1.0 / c.active_params_b
    return 0.0


def _rank_key(c: Candidate, band_top: float):
    # Within MIN_GAIN of the best, value differences are noise; what separates the band is,
    # in order: evidence we have over a promise, running on the accelerator, speed, then
    # the smaller download.
    in_band = c.value >= band_top - MIN_GAIN
    return (
        0 if in_band else 1,
        -c.value if not in_band else 0.0,
        0 if c.evidence == "measured" else 1,
        _FIT_RANK.get(c.fit, 1),
        -_speed_key(c),
        c.size_gb if c.size_gb is not None else 1e9,
    )


# How much faster a challenger must be to displace an incumbent of equal ability. Active
# parameters are a proxy for decode speed, so only a margin this wide counts as real.
SPEED_MARGIN = 1.5


def displaces(challenger: Candidate, incumbent: Candidate) -> str | None:
    """Why `challenger` should replace `incumbent` on a node, or None to keep it.

    Ability first: a real gain wins. But tier-1 scores stop at the instrument ceiling and
    a model not yet installed has only an estimate, so on ability alone a newer model could
    never replace an older one that already maxes the checker. What the operator wants
    from an upgrade at equal ability is speed — so running on the accelerator where the
    incumbent spills, or activating far fewer parameters per token, is reason enough too.
    """
    gain = challenger.value - incumbent.value
    if gain >= MIN_GAIN:
        return f"+{gain:.1f} for the demand mix over {incumbent.artifact}"
    if gain <= -MIN_GAIN:
        return None
    if _FIT_RANK.get(challenger.fit, 1) < _FIT_RANK.get(incumbent.fit, 1):
        return (f"as capable as {incumbent.artifact}, and it runs on the accelerator "
                f"where {incumbent.artifact} does not")
    if (challenger.active_params_b and incumbent.active_params_b
            and incumbent.active_params_b >= challenger.active_params_b * SPEED_MARGIN):
        return (f"as capable as {incumbent.artifact}, with "
                f"{incumbent.active_params_b / challenger.active_params_b:.0f}x fewer "
                f"active parameters per token, so it should decode faster")
    return None


def _abilities(ability_rows) -> tuple[dict[str, dict[str, float]], dict[str, str]]:
    scores: dict[str, dict[str, float]] = {}
    provenance: dict[str, str] = {}
    for a in ability_rows:
        scores.setdefault(a.artifact, {})[a.task_class] = a.score
        # One seed cell makes the whole artifact seed-grade evidence.
        if a.provenance == "seed" or provenance.get(a.artifact) == "seed":
            provenance[a.artifact] = "seed"
        else:
            provenance[a.artifact] = "measured"
    return scores, provenance


def _candidate(artifact, *, node, installed: bool, cat, scores, provenance,
               weights, measured_on, tiers) -> Candidate | None:
    s = scores.get(artifact)
    if s and all(tc in s for tc in TASK_CLASSES):
        evidence = provenance.get(artifact, "measured")
        per_class = {tc: min(s[tc], TIER1_MAX_ABILITY) for tc in TASK_CLASSES}
    elif cat is not None and cat.expected_ability is not None:
        evidence = "hint"
        hint = min(cat.expected_ability, TIER1_MAX_ABILITY)
        per_class = dict.fromkeys(TASK_CLASSES, hint)
    else:
        return None  # nothing to judge it on; recommending it would be a guess

    if cat is not None:
        verdict, reason = fits(cat, node, quota_for(node.profile, node.disk_quota_gb))
        if verdict == "no":
            if not installed:
                return None
            # It is demonstrably running, whatever the gate predicts; judge it as such.
            verdict, reason = "running", f"installed here, though the catalog says {reason}"
    elif installed:
        verdict, reason = "running", "installed here, but not in the catalog"
    else:
        return None  # size and RAM unknown: cannot say it fits

    return Candidate(
        artifact=artifact, installed=installed, scores=per_class, evidence=evidence,
        value=sum(weights[tc] * per_class[tc] for tc in TASK_CLASSES),
        fit=verdict, fit_reason=reason,
        size_gb=getattr(cat, "size_gb", None),
        active_params_b=(getattr(cat, "active_params_b", None)
                         or getattr(cat, "params_b", None)),
        measured_on=measured_on.get(artifact, []),
        tiers=tiers.get(artifact, []),
    )


def _fmt_evidence(c: Candidate, names: dict[str, str]) -> str:
    if c.evidence == "measured":
        where = ", ".join(names.get(n, n) for n in c.measured_on[:2])
        return f"measured{' on ' + where if where else ''}"
    if c.evidence == "seed":
        return "a shipped placeholder, not a measurement"
    return "catalog estimate, not yet measured"


def _slate(ranked: list[Candidate], node, weights, quota: float,
           names: dict[str, str]) -> list[Pick]:
    if not ranked:
        return []
    picks: list[Pick] = []
    used = 0.0

    def room(c: Candidate) -> bool:
        return c.installed or c.size_gb is None or used + c.size_gb <= quota

    primary = ranked[0]
    picks.append(Pick("primary", primary, (
        f"best for the demand mix ({primary.value:.1f}); "
        f"{_fmt_evidence(primary, names)}; {primary.fit_reason}; {_tier_note(primary)}")))
    used += primary.size_gb or 0

    # Coverage: the candidate that most raises the slate's best score in the classes the
    # primary is weak at, weighted by how much those classes are asked for.
    def coverage_gain(c: Candidate) -> tuple[float, str | None]:
        gain, weakest = 0.0, None
        for tc in TASK_CLASSES:
            best = max(p.candidate.scores[tc] for p in picks)
            g = weights[tc] * max(0.0, c.scores[tc] - best)
            if g > 0 and (weakest is None or g > gain):
                weakest = tc
            gain += g
        return gain, weakest

    best_cov = None
    for c in ranked[1:]:
        if not room(c):
            continue
        g, tc = coverage_gain(c)
        # Weighted gain, so the bar scales with how much the class is asked for.
        if g * len(TASK_CLASSES) >= MIN_GAIN and (best_cov is None or g > best_cov[1]):
            best_cov = (c, g, tc)
    if best_cov:
        c, g, tc = best_cov
        picks.append(Pick("coverage", c, (
            f"covers {tc}, where {primary.artifact} scores "
            f"{primary.scores[tc]:g} and this scores {c.scores[tc]:g}; "
            f"{_fmt_evidence(c, names)}; {_tier_note(c)}")))
        used += c.size_gb or 0

    if (node.profile or "shared") in _OWNER_PROFILES and len(picks) < SLATE_SIZE \
            and primary.size_gb:
        chosen = {p.candidate.artifact for p in picks}
        # An installed model with no catalog entry has no known size, but it is already on
        # disk, so it costs nothing to keep — preferring a download over it would be
        # advice to fetch what is, for this purpose, already there.
        light = [c for c in ranked
                 if c.artifact not in chosen
                 and ((c.size_gb and c.size_gb <= primary.size_gb * LIGHT_MAX_SIZE_FRACTION)
                      or (c.installed and c.size_gb is None))
                 and primary.value - c.value <= LIGHT_MAX_LOSS and room(c)]
        light.sort(key=lambda c: (not c.installed, -c.value, c.size_gb))
        if light:
            c = light[0]
            size = (f"{c.size_gb:g} GB" if c.size_gb else "size unknown (not in the catalog)")
            picks.append(Pick("light", c, (
                f"{size} against the primary's {primary.size_gb:g} GB — for the "
                f"active rung of this node's presence ladder, while the owner is using "
                f"the machine; {c.value:.1f} for the mix; {_fmt_evidence(c, names)}; "
                f"{_tier_note(c)}")))
    return picks[:SLATE_SIZE]


def _tier_note(c: Candidate) -> str:
    """A model serves a job only when a tier names it: a job is pinned to its tier's
    registered model, and a worker refuses one it cannot honour. Installed is not enough."""
    if c.tiers:
        return f"serves {', '.join(c.tiers)}"
    return "no tier names this model yet, so it serves no jobs until one does"


def advise(store, *, now: datetime | None = None, scale_version: str,
           tier_models: dict[str, list[str]] | None = None) -> dict:
    """Per-node slates plus the demand they were computed from.

    `tier_models` maps an artifact to the tiers registered to serve it (fleet.yaml). A
    slate model no tier names is still worth holding — but it serves nothing until one
    does, and the advice has to say so rather than imply the node will use it.
    """
    tier_models = tier_models or {}
    now = now or datetime.now(UTC)
    since = (now - timedelta(days=DEMAND_WINDOW_DAYS)).isoformat().replace("+00:00", "Z")
    demand = store.real_demand(since)
    weights = demand_weights(demand["by_class"])
    scores, provenance = _abilities(store.ability_matrix(scale_version))
    catalog = {c.artifact: c for c in store.list_catalog()}
    throughput = store.model_throughput()
    nodes = store.list_nodes()
    names = {n.node_id: (n.hostname or n.node_id) for n in nodes}

    # Where each artifact has actually been exercised — "measured on" is only honest when
    # it names a node that has run it, not every node that happens to list it.
    measured_on: dict[str, list[str]] = {}
    for (node_id, artifact) in throughput:
        measured_on.setdefault(artifact, []).append(node_id)

    from .wake import heartbeat_age_s

    out = []
    for node in nodes:
        installed = set(json.loads(node.installed or "[]"))
        # A node silent for a day is judged on its last report — and an offline node's
        # model server often reports nothing at all, so "installed: []" would turn into
        # advice to download what it already has. Fall back to the models it was last
        # seen holding, and label the whole entry as a report, not a reading.
        age = heartbeat_age_s(node, now=now)
        offline = age is not None and age > OFFLINE_AFTER_S
        last_known: list[str] = []
        if offline and not installed and hasattr(store, "node_models"):
            seen = store.node_models(node.node_id)
            if seen:
                # Only what it held at its last report — its history includes models
                # it dropped months ago.
                latest = max(m.last_seen for m in seen)
                cutoff = (datetime.fromisoformat(latest.replace("Z", "+00:00"))
                          - timedelta(days=1)).isoformat().replace("+00:00", "Z")
                last_known = sorted(m.artifact for m in seen if m.last_seen >= cutoff)
                installed = set(last_known)
        pool = installed | set(catalog)
        cands = [c for a in sorted(pool) if (c := _candidate(
            a, node=node, installed=a in installed, cat=catalog.get(a), scores=scores,
            provenance=provenance, weights=weights, measured_on=measured_on,
            tiers=tier_models)) is not None]
        if not cands:
            out.append(NodeAdvice(node.node_id, names[node.node_id], node.profile, [], [],
                                  None, [], "nothing in the catalog fits this machine",
                                  age if offline else None, last_known))
            continue
        top = max(c.value for c in cands)
        ranked = sorted(cands, key=lambda c: _rank_key(c, top))
        have = [c for c in ranked if c.installed]
        incumbent = have[0] if have else None

        # The incumbent keeps the primary slot unless something displaces it — a download
        # is not free, and inside MIN_GAIN the ability difference is not measurable. The
        # first displacing candidate in rank order takes the slot, with its reason.
        displace_why = None
        if incumbent:
            for c in ranked:
                if c is not incumbent and (displace_why := displaces(c, incumbent)):
                    ranked.remove(c)
                    ranked.insert(0, c)
                    break
            else:
                ranked.remove(incumbent)
                ranked.insert(0, incumbent)

        quota = quota_for(node.profile, node.disk_quota_gb)
        slate = _slate(ranked, node, weights, quota, names)
        if slate and displace_why and not slate[0].candidate.installed:
            slate[0].why = f"{displace_why}; {slate[0].why}"
        kept = {p.candidate.artifact for p in slate}
        unused = sorted(a for a in installed if a not in kept and a in scores)

        primary = slate[0].candidate if slate else None
        if primary is None:
            summary = "no recommendation"
        elif primary.installed:
            summary = (f"keep {primary.artifact} — nothing that fits is more capable by "
                       f"{MIN_GAIN:g}, or as capable and clearly faster")
        elif incumbent:
            summary = f"upgrade to {primary.artifact}: {displace_why}"
        else:
            summary = (f"install {primary.artifact} — nothing measurable is installed "
                       f"here yet")
        if offline:
            summary = f"offline {_ago(age)} — from its last report: {summary}"
        out.append(NodeAdvice(node.node_id, names[node.node_id], node.profile, slate,
                              ranked, incumbent, unused, summary,
                              age if offline else None, last_known))

    return {"nodes": out, "weights": weights, "demand": demand,
            "window_days": DEMAND_WINDOW_DAYS, "prior": DEMAND_PRIOR_JOBS}


# --- gaps: what the fleet is missing, stated with its evidence -------------------------

# A tier carrying at least this share of real demand is load-bearing; one node serving it
# is a single point of failure worth saying out loud.
BUSY_TIER_SHARE = 0.5

# A local tier below this share of real demand is idle in practice.
IDLE_TIER_SHARE = 0.05

# Silence long enough that the node's capacity should be counted as offline, not as a
# missed heartbeat. Deliberately far past `node_silent_s`: the dashboard's "silent" pill
# is about liveness right now, this is about planning.
OFFLINE_AFTER_S = 24 * 3600


@dataclass
class Gap:
    key: str        # stable identifier, for tests and for anyone scripting against it
    severity: str   # "warn" | "info"
    title: str
    detail: str


def _ago(seconds: float) -> str:
    days = seconds / 86400
    if days >= 1.5:
        return f"{days:.0f} days ago"
    return f"{seconds / 3600:.0f} hours ago"


def gaps(store, fleet, *, advice: dict | None = None, now: datetime | None = None,
         scale_version: str) -> list[Gap]:
    """Rules over live data, each one naming the evidence that triggered it.

    Each rule is here because the thing it catches looked healthy from every other angle —
    a tier that receives nothing still heartbeats green, and a model with no catalog entry
    still answers jobs. None of them change behaviour; they say what to look at.
    """
    from .wake import heartbeat_age_s

    now = now or datetime.now(UTC)
    advice = advice or advise(store, now=now, scale_version=scale_version)
    demand = advice["demand"]
    total = demand["total"]
    caps = fleet.capabilities if fleet else {}
    nodes = store.list_nodes()
    catalog = {c.artifact: c for c in store.list_catalog()}
    name = {n.node_id: (n.hostname or n.node_id) for n in nodes}
    advertised: dict[str, list] = {}
    for n in nodes:
        for c in json.loads(n.capabilities or "[]"):
            advertised.setdefault(c, []).append(n)
    window = f"the last {advice['window_days']} days"
    out: list[Gap] = []

    for cap, count in sorted(demand["by_capability"].items(), key=lambda kv: -kv[1]):
        if cap not in caps:
            out.append(Gap("undefined-tier", "warn",
                           f"Jobs sent to {cap}, which is not defined",
                           f"{count} real job(s) in {window} addressed {cap}, but fleet.yaml "
                           f"has no such tier, so none of them could run."))
            continue
        share = count / total if total else 0
        servers = advertised.get(cap, [])
        if share >= BUSY_TIER_SHARE and len(servers) == 1:
            out.append(Gap("single-node-tier", "warn",
                           f"{cap} depends on one machine",
                           f"{cap} took {share:.0%} of real jobs in {window} and only "
                           f"{name[servers[0].node_id]} serves it. When that machine is off, "
                           f"this work waits. A second node serving the tier's model "
                           f"({caps[cap].model}) would remove the single point."))

    for cap, spec in caps.items():
        if spec.cloud or cap not in advertised:
            continue
        count = demand["by_capability"].get(cap, 0)
        if total and count / total < IDLE_TIER_SHARE:
            out.append(Gap("idle-tier", "info", f"{cap} is barely used",
                           f"{count} of {total} real jobs in {window}. A job addressed by "
                           f"task_class + min_ability goes to the cheapest tier that clears "
                           f"the bar — speed is not weighed — so this tier only receives "
                           f"jobs that name it, even while its machines are idle."))

    for model, count in sorted(demand["by_model"].items(), key=lambda kv: -kv[1]):
        cloud_models = {s.model for s in caps.values() if s.cloud}
        if model in catalog or model in cloud_models:
            continue
        biggest = demand["max_tokens_in"]
        out.append(Gap("uncurated-model", "warn", f"{model} has no catalog entry",
                       f"It ran {count} of {total} real jobs in {window}, but its context "
                       f"window, tool calling and JSON-schema support are unrecorded"
                       f"{f' (largest prompt so far: {biggest:,} tokens)' if biggest else ''}. "
                       f"A job that requires any of them skips it, and it cannot be "
                       f"recommended for another node. Add it with POST /catalog."))

    for n in nodes:
        age = heartbeat_age_s(n, now=now)
        served = json.loads(n.capabilities or "[]")
        if age is not None and age > OFFLINE_AFTER_S and served:
            out.append(Gap("offline-node", "warn", f"{name[n.node_id]} has been offline",
                           f"Last heartbeat {_ago(age)}. Its {', '.join(served)} capacity "
                           f"is not available."))
        elif served and not json.loads(n.installed or "[]"):
            out.append(Gap("empty-node", "warn", f"{name[n.node_id]} reports no models",
                           f"It advertises {', '.join(served)} but has nothing installed, "
                           f"so any job it claims fails."))

    fleet_artifacts = {a for n in nodes for a in json.loads(n.installed or "[]")}
    for key, feature, label in (("tools", "supports_tools", "tool calling"),
                                ("vision", "supports_vision", "image input"),
                                ("json-schema", "supports_json_schema", "JSON-schema output")):
        local = any(getattr(catalog.get(a), feature, None) for a in fleet_artifacts)
        cloud = any(getattr(s, feature, None) for s in caps.values() if s.cloud)
        if not (local or cloud):
            out.append(Gap(f"no-{key}", "info", f"Nothing on the fleet declares {label}",
                           f"No installed model's catalog entry, and no cloud tier, says it "
                           f"supports {label}, so a job that requires it cannot run."))

    if not any(s.cloud for s in caps.values()):
        out.append(Gap("no-cloud", "info", "No cloud tier",
                       "A job no local model can serve fails or waits, rather than falling "
                       "back to a cloud model."))

    return out
