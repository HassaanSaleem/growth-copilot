"""The warehouse seam, spelled out as a Protocol.

`WarehouseRepository` is everything the graph asks of a warehouse and
nothing else. The module-level DuckDB implementation in this package
satisfies it today (`isinstance(growth_copilot.warehouse, WarehouseRepository)`
holds — modules are valid protocol implementers). A client-server backend
is one adapter away: implement these four members and nothing in
`graph/nodes.py` changes, because the graph never sees SQL.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class WarehouseRepository(Protocol):
    def get_connection(self, db_path: Path) -> Any:
        """Open (or reuse) a connection handle for the warehouse at `db_path`."""
        ...

    def execute_tool(self, con: Any, tool: str, args: dict[str, Any] | None) -> dict[str, Any]:
        """Run one catalog tool. Returns aggregates only — never row-level data."""
        ...

    def metadata_catalog(self, con: Any) -> dict[str, Any]:
        """Actual events, properties, and property values — grounding's source of truth."""
        ...

    def exported_segment_names(self, tool: str, args: dict[str, Any]) -> list[str]:
        """Segment names a task would export, for plan-time collision checks."""
        ...
