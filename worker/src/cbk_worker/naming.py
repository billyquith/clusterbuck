"""Artifact identity: which spellings name the same model.

Its own module because both the pull loop and the HTTP client need it, and putting it in
either would make them import each other. The coordinator mirrors this logic in
`server/clusterbuck/fleet.py`; the two MUST agree, or a pin one side believes valid is
refused by the other and the job fails for no real reason.
"""

from __future__ import annotations


def artifact_aliases(name: str) -> set[str]:
    """Spellings that mean the same artifact to a model server.

    Ollama reports an explicit tag on `/v1/models` while a registry commonly omits it, so
    `llama3.2` and `llama3.2:latest` are one artifact. Anything beyond that is left alone:
    guessing that `qwen/qwen3.5-9b` and `qwen3.5:9b` are the same weights would be exactly
    the confident-but-unfounded inference this system keeps removing — they are different
    artifacts to the servers that hold them, and ability is pinned per artifact.
    """
    name = (name or "").strip()
    base = name.split(":", 1)[0]
    return {name, base, f"{base}:latest"}
