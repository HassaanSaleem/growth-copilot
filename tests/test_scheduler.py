"""Scheduler: level structure, determinism, and loud failures on bad DAGs."""

from __future__ import annotations

import pytest

from growth_copilot.domain.tasks import Task
from growth_copilot.graph.scheduler import build_batches, ready_tasks


def _diamond() -> list[Task]:
    #    1
    #   / \
    #  2   3
    #   \ /
    #    4
    return [
        Task(id=1, tool="insight_query"),
        Task(id=2, tool="insight_query", depends_on=[1]),
        Task(id=3, tool="insight_query", depends_on=[1]),
        Task(id=4, tool="insight_query", depends_on=[2, 3]),
    ]


def _ids(batches: list[list[Task]]) -> list[list[int]]:
    return [[t.id for t in level] for level in batches]


def test_diamond_level_structure():
    levels = _ids(build_batches(_diamond()))
    assert [set(level) for level in levels] == [{1}, {2, 3}, {4}]


def test_ordering_is_deterministic():
    tasks = _diamond()
    first = _ids(build_batches(tasks))
    assert all(_ids(build_batches(tasks)) == first for _ in range(5))
    # Independent roots come out sorted by id, regardless of input order.
    roots = [Task(id=9, tool="t"), Task(id=2, tool="t"), Task(id=5, tool="t")]
    assert _ids(build_batches(roots)) == [[2, 5, 9]]


def test_cycle_raises_naming_members():
    tasks = [Task(id=1, tool="t", depends_on=[2]), Task(id=2, tool="t", depends_on=[1])]
    with pytest.raises(ValueError, match=r"cycle detected among tasks \[1, 2\]"):
        build_batches(tasks)


def test_unknown_dependency_raises_naming_it():
    tasks = [Task(id=1, tool="t", depends_on=[99])]
    with pytest.raises(ValueError, match="unknown dependencies") as exc:
        build_batches(tasks)
    assert "99" in str(exc.value)


def test_ready_tasks_incremental():
    tasks = _diamond()
    assert [t.id for t in ready_tasks(tasks, set())] == [1]
    assert [t.id for t in ready_tasks(tasks, {1})] == [2, 3]
    assert [t.id for t in ready_tasks(tasks, {1, 2})] == [3]
    assert [t.id for t in ready_tasks(tasks, {1, 2, 3})] == [4]
    assert ready_tasks(tasks, {1, 2, 3, 4}) == []
