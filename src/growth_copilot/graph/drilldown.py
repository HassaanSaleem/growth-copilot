"""Recursive bottleneck drill-down as a LangGraph work queue.

The shape that "breaks" static graph frameworks: the analysis tree isn't
known until execution — each cohort's funnel decides whether and where to
recurse. Expressed here as a frontier/collect loop:

    START → init → (route) ─┬→ [Send("expand_cohort", item) ...] → collect → (route)
                            └→ report → END

Each `expand_cohort` runs the cohort's funnel, finds the worst step, mines
property-value blockers, and *proposes* child cohorts. `collect` dedups
proposals by filter signature (order-independent) and promotes the novel
ones to the next frontier. Expansion is bounded by depth, an adaptive
cohort-size threshold, and signature dedup — the cost controls this shape
needs to survive production, expressed in ~150 lines of graph.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, TypedDict

from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from growth_copilot import warehouse
from growth_copilot.config import get_settings
from growth_copilot.domain.state import append_list, merge_results

MIN_COHORT_FLOOR = 200  # never expand cohorts smaller than this
ADAPTIVE_FRACTION = 0.10  # ... or smaller than 10% of the parent's lost users
MAX_BLOCKERS_PER_NODE = 3


class DrilldownState(TypedDict, total=False):
    steps: list[str]
    timeframe_days: int
    max_depth: int
    frontier: list[dict[str, Any]]
    # proposals merge by filter signature across parallel Send branches,
    # which makes cross-branch dedup a property of the reducer
    proposals: Annotated[dict[str, dict[str, Any]], merge_results]
    seen: Annotated[dict[str, bool], merge_results]
    nodes: Annotated[list[dict[str, Any]], append_list]
    report: str


class CohortItem(TypedDict):
    filters: dict[str, Any]
    depth: int
    parent_signature: str
    steps: list[str]
    timeframe_days: int
    max_depth: int


def filter_signature(filters: dict[str, Any]) -> str:
    return json.dumps(filters, sort_keys=True)


def init(state: DrilldownState) -> dict[str, Any]:
    root_sig = filter_signature({})
    return {"frontier": [{"filters": {}, "depth": 0, "parent_signature": ""}], "seen": {root_sig: True}}


def route_frontier(state: DrilldownState) -> list[Send] | str:
    frontier = state.get("frontier") or []
    if not frontier:
        return "report"
    return [
        Send(
            "expand_cohort",
            CohortItem(
                filters=item["filters"],
                depth=item["depth"],
                parent_signature=item["parent_signature"],
                steps=state["steps"],
                timeframe_days=state.get("timeframe_days", 90),
                max_depth=state.get("max_depth", 3),
            ),
        )
        for item in frontier
    ]


def expand_cohort(item: CohortItem) -> dict[str, Any]:
    writer = get_stream_writer()
    con = warehouse.get_connection(get_settings().db_path)
    signature = filter_signature(item["filters"])
    writer({"type": "drilldown_expand", "filters": item["filters"], "depth": item["depth"]})

    funnel = warehouse.execute_tool(
        con,
        "funnel_analysis",
        {"steps": item["steps"], "timeframe_days": item["timeframe_days"], "filters": item["filters"]},
    )
    steps = funnel["data"]["steps"]
    if len(steps) < 2 or steps[0]["users"] == 0:
        node = {"signature": signature, "filters": item["filters"], "depth": item["depth"],
                "parent": item["parent_signature"], "funnel": steps, "blockers": [], "note": "empty cohort"}
        return {"nodes": [node]}

    # Worst step = largest absolute user loss between consecutive steps.
    losses = [
        (i, steps[i]["users"] - steps[i + 1]["users"]) for i in range(len(steps) - 1)
    ]
    worst_index, lost_users = max(losses, key=lambda pair: pair[1])

    node: dict[str, Any] = {
        "signature": signature,
        "filters": item["filters"],
        "depth": item["depth"],
        "parent": item["parent_signature"],
        "funnel": steps,
        "worst_step": {
            "from": steps[worst_index]["event"],
            "to": steps[worst_index + 1]["event"],
            "lost_users": lost_users,
        },
        "blockers": [],
    }

    # Adaptive expansion threshold: don't chase cohorts too small to matter.
    threshold = max(MIN_COHORT_FLOOR, int(lost_users * ADAPTIVE_FRACTION))
    proposals: dict[str, dict[str, Any]] = {}
    if item["depth"] < item["max_depth"] and lost_users >= MIN_COHORT_FLOOR:
        breakdown = warehouse.execute_tool(
            con,
            "funnel_breakdown",
            {
                "steps": [steps[worst_index]["event"], steps[worst_index + 1]["event"]],
                "timeframe_days": item["timeframe_days"],
                "filters": item["filters"],
            },
        )
        blockers = breakdown["data"].get("blockers", [])[:MAX_BLOCKERS_PER_NODE]
        node["blockers"] = blockers
        for blocker in blockers:
            prop, value = blocker["property"], blocker["value"]
            if prop in item["filters"]:
                continue  # already conditioned on this property along this branch
            child_filters = {**item["filters"], prop: value}
            if blocker.get("affected_users", 0) < threshold:
                continue
            proposals[filter_signature(child_filters)] = {
                "filters": child_filters,
                "depth": item["depth"] + 1,
                "parent_signature": signature,
            }
    return {"nodes": [node], "proposals": proposals}


def collect(state: DrilldownState) -> dict[str, Any]:
    # `proposals` accumulates across rounds (reducers only merge), so novelty
    # is judged against `seen` rather than by clearing the dict.
    seen = state.get("seen", {})
    novel = {sig: item for sig, item in (state.get("proposals") or {}).items() if sig not in seen}
    return {"frontier": list(novel.values()), "seen": {sig: True for sig in novel}}


def report(state: DrilldownState) -> dict[str, Any]:
    """Render the discovered tree depth-first as markdown."""
    by_parent: dict[str, list[dict[str, Any]]] = {}
    for node in state.get("nodes", []):
        by_parent.setdefault(node["parent"], []).append(node)

    lines: list[str] = [f"# Bottleneck drill-down: {' → '.join(state['steps'])}", ""]

    def render(node: dict[str, Any], indent: int) -> None:
        pad = "  " * indent
        label = ", ".join(f"{k}={v}" for k, v in node["filters"].items()) or "all users"
        first, last = node["funnel"][0], node["funnel"][-1]
        conv = (last["users"] / first["users"] * 100) if first["users"] else 0.0
        lines.append(f"{pad}- **{label}** — {first['users']} users, {conv:.1f}% conversion")
        worst = node.get("worst_step")
        if worst:
            lines.append(
                f"{pad}  worst step: {worst['from']} → {worst['to']} (-{worst['lost_users']} users)"
            )
        for blocker in node.get("blockers", []):
            lines.append(
                f"{pad}  blocker: {blocker['property']}={blocker['value']} "
                f"(lift {blocker['lift']:+.1%}, {blocker['affected_users']} users)"
            )
        for child in sorted(by_parent.get(node["signature"], []), key=lambda n: n["signature"]):
            render(child, indent + 1)

    for root in by_parent.get("", []):
        render(root, 0)
    return {"report": "\n".join(lines)}


def build_drilldown_graph(checkpointer=None):
    g = StateGraph(DrilldownState)
    g.add_node("init", init)
    g.add_node("expand_cohort", expand_cohort)
    g.add_node("collect", collect)
    g.add_node("report", report)

    g.add_edge(START, "init")
    g.add_conditional_edges("init", route_frontier, ["expand_cohort", "report"])
    g.add_edge("expand_cohort", "collect")
    g.add_conditional_edges("collect", route_frontier, ["expand_cohort", "report"])
    g.add_edge("report", END)
    return g.compile(checkpointer=checkpointer)
