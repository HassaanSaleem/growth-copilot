"""DuckDB connection lifecycle.

One process-cached connection per database file; every tool execution takes
a fresh cursor from it (`con.cursor()` opens an isolated session), which is
what keeps parallel LangGraph `Send` branches safe against each other.
"""

from __future__ import annotations

import threading
from pathlib import Path

import duckdb

_CONNECTIONS: dict[str, duckdb.DuckDBPyConnection] = {}
_LOCK = threading.Lock()


def get_connection(db_path: Path | str) -> duckdb.DuckDBPyConnection:
    """Return the process-cached connection for `db_path`, creating it on first use."""
    resolved = Path(db_path).expanduser().resolve()
    key = str(resolved)
    with _LOCK:
        con = _CONNECTIONS.get(key)
        if con is None:
            resolved.parent.mkdir(parents=True, exist_ok=True)
            con = duckdb.connect(key)
            _CONNECTIONS[key] = con
        return con
