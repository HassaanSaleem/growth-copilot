"""Late binding: $ref resolution anywhere in args, DependencyError on bad edges."""

from __future__ import annotations

import pytest

from growth_copilot.graph.binding import DependencyError, bind_dependencies

DEPS = {
    "1": {
        "status": "success",
        "data": {"rows": [1, 2, 3]},
        "segment_name": "recent_upgraders",
        "event_names": ["file_uploaded", "link_shared"],
    }
}


def test_resolves_refs_nested_in_dicts_and_lists():
    args = {
        "segment": {"$ref": 1, "field": "segment_name"},
        "outer": {"inner": {"allowed": {"$ref": "1", "field": "event_names"}}},
        "mixed": [{"$ref": 1, "field": "segment_name"}, "literal", 7],
        "untouched": {"top_n": 5},
    }
    bound = bind_dependencies(args, DEPS)
    assert bound["segment"] == "recent_upgraders"
    assert bound["outer"]["inner"]["allowed"] == ["file_uploaded", "link_shared"]
    assert bound["mixed"] == ["recent_upgraders", "literal", 7]
    assert bound["untouched"] == {"top_n": 5}


def test_ref_field_defaults_to_data():
    assert bind_dependencies({"x": {"$ref": 1}}, DEPS)["x"] == {"rows": [1, 2, 3]}


def test_missing_dependency_raises():
    with pytest.raises(DependencyError, match="not a declared dependency"):
        bind_dependencies({"x": {"$ref": 2, "field": "data"}}, DEPS)


def test_failed_dependency_raises():
    failed = {"1": {"status": "error", "tool": "funnel_analysis", "execution_error": "boom"}}
    with pytest.raises(DependencyError, match="failed"):
        bind_dependencies({"x": {"$ref": 1, "field": "data"}}, failed)


def test_missing_field_raises():
    with pytest.raises(DependencyError, match="no field 'nope'"):
        bind_dependencies({"x": {"$ref": 1, "field": "nope"}}, DEPS)
