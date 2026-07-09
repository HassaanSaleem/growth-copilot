"""Full-pipeline offline runs: a preloaded plan enters at `ground`, fans out
with Send, enriches, and synthesizes deterministically (no API key)."""

from __future__ import annotations

from typing import Any

import pytest

from growth_copilot.domain.tasks import load_recipe
from growth_copilot.graph import build_graph


def _recipe_payload(name: str) -> dict[str, Any]:
    recipe = load_recipe(name)
    plan = recipe.resolve()
    return {"question": recipe.description, "plan": [t.model_dump(mode="json") for t in plan.tasks]}


def test_conversion_blockers_recipe_runs_offline_end_to_end():
    final = build_graph().invoke(_recipe_payload("conversion-blockers"))
    results = final["results"]
    assert set(results) == {"1", "2", "3", "4", "5"}
    assert all(r["status"] == "success" for r in results.values())

    # Both auto-comparisons fire: 2 discoveries and 2 profiles were run.
    kinds = {f.get("kind") for f in final.get("findings", [])}
    assert {"segment_event_discovery_comparison", "profile_segment_comparison"} <= kinds

    assert "5/5" in final["answer"]["summary"]


def test_unknown_event_is_isolated_not_fatal():
    plan = [
        {
            "id": 1,
            "tool": "funnel_analysis",
            "args": {"steps": ["no_such_event", "file_uploaded"], "timeframe_days": 60},
            "depends_on": [],
        },
        {
            "id": 2,
            "tool": "insight_query",
            "args": {"events": ["account_created"], "metric": "unique_users", "timeframe_days": 60},
            "depends_on": [],
        },
    ]
    final = build_graph().invoke({"question": "failure isolation", "plan": plan})

    # Failure isolation: a result exists for every task id and nothing raised.
    results = final["results"]
    assert set(results) == {"1", "2"}
    assert results["2"]["status"] == "success"
    bad = results["1"]
    assert bad["status"] == "error" or bad["data"]["steps"][0]["users"] == 0

    # Grounding reported the unresolvable name instead of guessing.
    assert any(c["to"] is None and c["from"] == "no_such_event" for c in final.get("grounding", []))
    assert final["answer"]["summary"]


def test_duplicate_segment_exports_are_rejected_at_grounding():
    # Two tasks exporting the same segment name would silently overwrite each
    # other's cohort mid-run; the plan is rejected before anything executes.
    plan = [
        {
            "id": 1,
            "tool": "funnel_analysis",
            "args": {
                "steps": ["account_created", "plan_upgraded"],
                "export_converted": True,
                "segment_name": "shared",
            },
            "depends_on": [],
        },
        {
            "id": 2,
            "tool": "funnel_analysis",
            "args": {
                "steps": ["file_uploaded", "plan_upgraded"],
                "export_converted": True,
                "segment_name": "shared",
            },
            "depends_on": [],
        },
    ]
    with pytest.raises(ValueError, match="both export segment 'shared_converted'"):
        build_graph().invoke({"question": "dup exports", "plan": plan})
