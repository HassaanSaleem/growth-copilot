"""Recursive bottleneck drill-down: tree discovery bounded by max_depth."""

from __future__ import annotations

from typing import Any

from growth_copilot.graph.drilldown import build_drilldown_graph

SOLO_FUNNEL = ["workspace_created", "file_uploaded", "link_shared"]


def _run(max_depth: int) -> dict[str, Any]:
    return build_drilldown_graph().invoke(
        {"steps": SOLO_FUNNEL, "timeframe_days": 60, "max_depth": max_depth},
        {"recursion_limit": 100},
    )


def test_drilldown_discovers_root_and_reports():
    final = _run(1)
    nodes = final["nodes"]
    assert nodes
    roots = [n for n in nodes if n["filters"] == {}]
    assert len(roots) == 1
    assert roots[0]["depth"] == 0
    assert roots[0]["parent"] == ""
    assert all(n["depth"] <= 1 for n in nodes)

    report = final["report"]
    assert isinstance(report, str) and report.strip()
    assert "Bottleneck drill-down" in report


def test_max_depth_zero_yields_no_children():
    final = _run(0)
    assert len(final["nodes"]) == 1
    assert all(n["depth"] == 0 for n in final["nodes"])
