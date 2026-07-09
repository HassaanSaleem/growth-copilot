"""Embedded DuckDB warehouse: seeding, metadata, tool execution, comparisons.

The copilot's privacy contract lives here: every tool returns aggregates
only — user-level rows never leave the warehouse.
"""

from growth_copilot.warehouse.analytics import execute_tool, exported_segment_names
from growth_copilot.warehouse.compare import compare_discoveries, compare_profiles
from growth_copilot.warehouse.db import get_connection
from growth_copilot.warehouse.metadata import USER_PROPERTIES, metadata_catalog
from growth_copilot.warehouse.repository import WarehouseRepository
from growth_copilot.warehouse.seed import EVENTS, PLANTED_EFFECTS, seed

__all__ = [
    "EVENTS",
    "PLANTED_EFFECTS",
    "USER_PROPERTIES",
    "WarehouseRepository",
    "compare_discoveries",
    "compare_profiles",
    "execute_tool",
    "exported_segment_names",
    "get_connection",
    "metadata_catalog",
    "seed",
]
