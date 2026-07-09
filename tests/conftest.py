"""Shared fixtures: one session-scoped seeded warehouse, forced-offline env.

Every test runs offline — ANTHROPIC_API_KEY is removed so the graph
exercises its deterministic fallbacks, and GROWTH_COPILOT_DB points at a
throwaway seeded DuckDB file (settings re-read the environment on every
`get_settings()` call, so setting os.environ in a fixture is sufficient).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

SEED_USERS = 800
SEED_DAYS = 60
SEED_SEED = 7


@pytest.fixture(scope="session")
def seeded_db(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict[str, Any]]:
    """Seed the deterministic Relay warehouse once for the whole session."""
    from growth_copilot.warehouse import seed

    db_path = tmp_path_factory.mktemp("warehouse") / "relay-test.duckdb"
    stats = seed(db_path, users=SEED_USERS, days=SEED_DAYS, seed=SEED_SEED)
    return db_path, stats


@pytest.fixture(scope="session", autouse=True)
def offline_env(seeded_db: tuple[Path, dict[str, Any]]):
    """Point the app at the test warehouse and force offline (no-LLM) mode."""
    db_path, _ = seeded_db
    previous_db = os.environ.get("GROWTH_COPILOT_DB")
    previous_key = os.environ.pop("ANTHROPIC_API_KEY", None)
    os.environ["GROWTH_COPILOT_DB"] = str(db_path)
    yield
    if previous_db is None:
        os.environ.pop("GROWTH_COPILOT_DB", None)
    else:
        os.environ["GROWTH_COPILOT_DB"] = previous_db
    if previous_key is not None:
        os.environ["ANTHROPIC_API_KEY"] = previous_key


@pytest.fixture()
def con(seeded_db: tuple[Path, dict[str, Any]]):
    """The process-cached connection to the seeded test warehouse."""
    from growth_copilot.warehouse import get_connection

    return get_connection(seeded_db[0])
