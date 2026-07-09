"""Dependency-graph scheduling: Kahn's algorithm over Task.depends_on.

`build_batches` computes the full topological level structure (used for
plan validation and display); `ready_tasks` computes the incremental ready
set the execute loop feeds to `Send` fan-out. Failures are isolated per
task: a failed dependency yields an error dict, and downstream tasks still
run, receiving the error as data.
"""

from __future__ import annotations

from collections import defaultdict, deque

from growth_copilot.domain.tasks import Task


def build_batches(tasks: list[Task]) -> list[list[Task]]:
    """Group tasks into dependency levels; every task in a level may run in parallel.

    Raises ValueError on unknown dependencies or cycles.
    """
    by_id = {t.id: t for t in tasks}
    unknown = {t.id: [d for d in t.depends_on if d not in by_id] for t in tasks}
    unknown = {k: v for k, v in unknown.items() if v}
    if unknown:
        raise ValueError(f"unknown dependencies: {unknown}")

    in_degree = {t.id: len(t.depends_on) for t in tasks}
    children: dict[int, list[int]] = defaultdict(list)
    for t in tasks:
        for dep in t.depends_on:
            children[dep].append(t.id)

    ready = deque(sorted(tid for tid, deg in in_degree.items() if deg == 0))
    batches: list[list[Task]] = []
    seen = 0
    while ready:
        level = list(ready)
        ready.clear()
        batches.append([by_id[tid] for tid in level])
        seen += len(level)
        for parent in level:
            for child in children[parent]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    ready.append(child)
    if seen != len(tasks):
        stuck = sorted(tid for tid, deg in in_degree.items() if deg > 0)
        raise ValueError(f"cycle detected among tasks {stuck}")
    return batches


def ready_tasks(tasks: list[Task], done: set[int]) -> list[Task]:
    """Tasks whose dependencies are all complete and which haven't run yet."""
    return [t for t in tasks if t.id not in done and all(d in done for d in t.depends_on)]
