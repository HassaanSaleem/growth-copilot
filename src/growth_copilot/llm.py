"""LLM access in one place.

Every stage gets its model from config (default: claude-opus-4-8) so the
whole pipeline can be re-pointed without touching code. When no API key is
present, `get_chat` returns None and callers fall back to deterministic
behavior — the copilot degrades to recipe mode instead of crashing.
"""

from __future__ import annotations

from functools import lru_cache

from growth_copilot.config import get_settings


@lru_cache(maxsize=8)
def _chat(model: str, max_tokens: int):
    from langchain_anthropic import ChatAnthropic

    return ChatAnthropic(model=model, max_tokens=max_tokens, timeout=120, max_retries=2)


def get_chat(stage: str, max_tokens: int = 4096):
    """Return a ChatAnthropic for `stage`, or None when running offline."""
    settings = get_settings()
    if not settings.api_key:
        return None
    return _chat(settings.model_for(stage), max_tokens)
