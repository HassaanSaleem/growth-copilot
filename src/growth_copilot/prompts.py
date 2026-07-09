"""Prompt loading. Prompts are files, not string literals: reviewable,
diffable, and editable without touching code."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=8)
def load_prompt(name: str) -> str:
    path = Path(__file__).resolve().parent / "resources" / "prompts" / f"{name}.md"
    return path.read_text()
