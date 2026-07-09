"""The tool catalog: the SME-editable contract between planner and executor.

Tools are declared in resources/catalog.json — descriptions double as
affordance specs for the planner prompt (what each tool imports/exports),
so adding a tool is a JSON edit plus a warehouse function, never a prompt
rewrite.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


@lru_cache(maxsize=1)
def load_catalog() -> dict[str, dict[str, Any]]:
    path = Path(__file__).resolve().parent / "resources" / "catalog.json"
    data = json.loads(path.read_text())
    return {tool["name"]: tool for tool in data["tools"]}


def tool_names() -> list[str]:
    return list(load_catalog().keys())


def catalog_for_prompt() -> str:
    """Render the catalog as compact markdown for the planner prompt."""
    lines: list[str] = []
    for tool in load_catalog().values():
        lines.append(f"### {tool['name']}")
        lines.append(tool["description"])
        lines.append("Args: " + json.dumps(tool["args"]))
        if tool.get("exports"):
            lines.append(f"Exports: {tool['exports']}")
        lines.append("Example args: " + json.dumps(tool["example"]))
        lines.append("")
    return "\n".join(lines)
