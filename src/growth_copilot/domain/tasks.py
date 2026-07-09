"""The task graph is the product: a JSON-serializable, human-editable plan.

Unlike opaque agent traces, a `Plan` can be reviewed before execution,
saved as a `Recipe`, diffed, and re-run — the unit of reuse and audit.
"""

from __future__ import annotations

import json
from pathlib import Path
from string import Template
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Task(BaseModel):
    """One node of the plan. `depends_on` are the DAG edges.

    Args may contain reference values of the form ``{"$ref": <task_id>,
    "field": "<key>"}`` which are resolved from the referenced task's
    result at execution time (late binding of planned placeholders to
    runtime data).
    """

    model_config = ConfigDict(populate_by_name=True)

    id: int
    tool: str = Field(alias="name")
    args: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[int] = Field(default_factory=list)


class Plan(BaseModel):
    tasks: list[Task]

    @model_validator(mode="after")
    def _validate_dag(self) -> Plan:
        if not self.tasks:
            raise ValueError("plan has no tasks")
        ids = [t.id for t in self.tasks]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate task ids in plan")
        known = set(ids)
        for t in self.tasks:
            unknown = [d for d in t.depends_on if d not in known]
            if unknown:
                raise ValueError(f"task {t.id} depends on unknown task(s) {unknown}")
            if t.id in t.depends_on:
                raise ValueError(f"task {t.id} depends on itself")
        # Cycle check is delegated to the scheduler (Kahn's algorithm) so the
        # error message can name the cycle members.
        from growth_copilot.graph.scheduler import build_batches

        build_batches(self.tasks)
        return self


class Recipe(BaseModel):
    """A saved, parameterized plan — editable JSON, no code deployment.

    Parameters appear in task args as ``$param`` strings and are resolved
    with ``string.Template.safe_substitute`` at load time.
    """

    name: str
    description: str = ""
    params: dict[str, str] = Field(default_factory=dict, description="param name -> default value")
    tasks: list[dict[str, Any]]

    def resolve(self, overrides: dict[str, str] | None = None) -> Plan:
        values = {**self.params, **(overrides or {})}
        unknown = [k for k in (overrides or {}) if k not in self.params]
        if unknown:
            raise ValueError(f"recipe '{self.name}' has no params {unknown}; declared: {list(self.params)}")
        missing = [k for k, v in values.items() if v == ""]
        if missing:
            raise ValueError(f"recipe '{self.name}' is missing values for params: {missing}")

        # Substitute on the parsed structure, never on serialized JSON text —
        # splicing raw values into JSON source would let a param containing a
        # quote rewrite the plan structure itself.
        def substitute(node: Any) -> Any:
            if isinstance(node, str):
                return Template(node).safe_substitute(values)
            if isinstance(node, dict):
                return {k: substitute(v) for k, v in node.items()}
            if isinstance(node, list):
                return [substitute(v) for v in node]
            return node

        resolved = substitute(json.loads(json.dumps(self.tasks)))
        return Plan(tasks=[Task.model_validate(t) for t in resolved])


def recipes_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "resources" / "recipes"


def load_recipe(name: str) -> Recipe:
    path = recipes_dir() / f"{name}.json"
    if not path.exists():
        available = sorted(p.stem for p in recipes_dir().glob("*.json"))
        raise FileNotFoundError(f"no recipe named '{name}'; available: {available}")
    return Recipe.model_validate_json(path.read_text())


def list_recipes() -> list[Recipe]:
    return [Recipe.model_validate_json(p.read_text()) for p in sorted(recipes_dir().glob("*.json"))]
