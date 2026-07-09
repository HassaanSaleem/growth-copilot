"""Shipped recipes: load, resolve into valid Plans, reference only real tools."""

from __future__ import annotations

import pytest

from growth_copilot.catalog import load_catalog
from growth_copilot.domain.tasks import Plan, list_recipes, load_recipe


def test_every_recipe_resolves_to_valid_plan():
    recipes = list_recipes()
    assert len(recipes) >= 3
    for recipe in recipes:
        plan = recipe.resolve()  # defaults only — the Plan validator checks ids, deps, and cycles
        assert isinstance(plan, Plan)
        assert plan.tasks


def test_every_referenced_tool_exists_in_catalog():
    catalog = load_catalog()
    for recipe in list_recipes():
        for task in recipe.resolve().tasks:
            assert task.tool in catalog, f"recipe '{recipe.name}' task {task.id} uses unknown tool '{task.tool}'"


def test_param_override_changes_args():
    default_plan = load_recipe("conversion-blockers").resolve()
    override_plan = load_recipe("conversion-blockers").resolve({"timeframe_days": "30"})
    assert default_plan.tasks[0].args["timeframe_days"] == "90"
    assert override_plan.tasks[0].args["timeframe_days"] == "30"


def test_missing_param_value_raises():
    with pytest.raises(ValueError, match="missing values for params"):
        load_recipe("conversion-blockers").resolve({"timeframe_days": ""})


def test_unknown_recipe_names_available_ones():
    with pytest.raises(FileNotFoundError, match="conversion-blockers"):
        load_recipe("does-not-exist")


def test_param_values_cannot_inject_plan_structure():
    # Substitution happens on the parsed structure, so a quote-laden value is
    # just a string — it can never add keys or rewrite the plan shape.
    recipe = load_recipe("conversion-blockers")
    plan = recipe.resolve({"timeframe_days": '30", "hack": "1'})
    funnel = plan.tasks[0]
    assert funnel.args["timeframe_days"] == '30", "hack": "1'
    assert "hack" not in funnel.args


def test_unknown_override_param_is_rejected():
    recipe = load_recipe("conversion-blockers")
    with pytest.raises(ValueError, match="no params"):
        recipe.resolve({"not_a_param": "1"})
