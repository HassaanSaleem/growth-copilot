"""Warehouse metadata catalog — the ground truth the planner and grounding
stages check names against.

Queried fresh on every call (a handful of cheap aggregates), so it can never
go stale after a re-seed. The `summary` string is injected verbatim into LLM
prompts.
"""

from __future__ import annotations

from typing import Any

import duckdb

USER_PROPERTIES = ["country", "device", "channel", "plan", "company_size"]


def _tables_exist(cur: duckdb.DuckDBPyConnection) -> bool:
    row = cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('users', 'events')"
    ).fetchone()
    return bool(row and row[0] >= 2)


def metadata_catalog(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Return {events, properties, property_values, summary} for the warehouse."""
    cur = con.cursor()
    try:
        if not _tables_exist(cur):
            return {
                "events": [],
                "properties": USER_PROPERTIES,
                "property_values": {},
                "summary": "The warehouse is empty — run `growth-copilot seed` first.",
            }
        event_rows = cur.execute(
            "SELECT event, COUNT(*) AS n, COUNT(DISTINCT user_id) AS u FROM events GROUP BY event ORDER BY event"
        ).fetchall()
        property_values = {
            prop: [
                row[0]
                for row in cur.execute(
                    f"SELECT DISTINCT {prop} FROM users WHERE {prop} IS NOT NULL ORDER BY 1"
                ).fetchall()
            ]
            for prop in USER_PROPERTIES
        }
    finally:
        cur.close()

    lines = ["## Events"]
    for event, n, u in event_rows:
        lines.append(f"- `{event}` — {n:,} events · {u:,} users")
    lines.append("")
    lines.append("## User properties")
    for prop in USER_PROPERTIES:
        lines.append(f"- `{prop}`: {', '.join(property_values.get(prop, []))}")

    return {
        "events": [row[0] for row in event_rows],
        "properties": USER_PROPERTIES,
        "property_values": property_values,
        "summary": "\n".join(lines),
    }
