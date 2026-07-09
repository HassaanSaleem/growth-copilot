"""Environment-driven configuration.

No secrets or endpoints are ever hardcoded; everything resolves from the
process environment with safe local defaults. `ANTHROPIC_API_KEY` is the
only credential, and it is optional — without it the copilot runs in
deterministic recipe mode.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DEFAULT_MODEL = "claude-opus-4-8"


def _env(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value or default


@dataclass(frozen=True)
class Settings:
    db_path: Path = field(default_factory=lambda: Path(_env("GROWTH_COPILOT_DB", "data/warehouse.duckdb")))
    checkpoint_path: Path = field(
        default_factory=lambda: Path(_env("GROWTH_COPILOT_CHECKPOINTS", "data/checkpoints.sqlite"))
    )
    model: str = field(default_factory=lambda: _env("GROWTH_COPILOT_MODEL", DEFAULT_MODEL))
    triage_model: str = field(default_factory=lambda: _env("GROWTH_COPILOT_TRIAGE_MODEL", ""))
    planner_model: str = field(default_factory=lambda: _env("GROWTH_COPILOT_PLANNER_MODEL", ""))
    synthesis_model: str = field(default_factory=lambda: _env("GROWTH_COPILOT_SYNTHESIS_MODEL", ""))
    max_parallel_tasks: int = field(default_factory=lambda: int(_env("GROWTH_COPILOT_MAX_PARALLEL", "8")))
    # Per-dependency character budget when formatting upstream results into
    # the synthesis prompt. A context-budget contract between stages: one
    # oversized task result must never crowd out its siblings.
    dep_char_budget: int = field(default_factory=lambda: int(_env("GROWTH_COPILOT_DEP_BUDGET", "8000")))

    @property
    def api_key(self) -> str | None:
        return os.getenv("ANTHROPIC_API_KEY") or None

    def model_for(self, stage: str) -> str:
        override = {
            "triage": self.triage_model,
            "planner": self.planner_model,
            "synthesis": self.synthesis_model,
        }.get(stage, "")
        return override or self.model


def get_settings() -> Settings:
    return Settings()
