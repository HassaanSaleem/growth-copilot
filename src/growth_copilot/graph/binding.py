"""Late binding of planned placeholders to runtime data.

A plan is written before any data exists, so args may reference upstream
results: ``{"$ref": 3, "field": "segment_name"}`` resolves to the value the
executed task 3 produced. Edge semantics stay here — the scheduler moves
data, it never interprets it.
"""

from __future__ import annotations

from typing import Any


class DependencyError(ValueError):
    pass


def bind_dependencies(args: dict[str, Any], dep_results: dict[str, Any]) -> dict[str, Any]:
    def resolve(value: Any) -> Any:
        if isinstance(value, dict) and "$ref" in value:
            ref = str(value["$ref"])
            dep = dep_results.get(ref)
            if dep is None:
                raise DependencyError(f"$ref to task {ref}, which is not a declared dependency")
            if isinstance(dep, dict) and (dep.get("execution_error") or dep.get("status") == "error"):
                raise DependencyError(f"dependency task {ref} failed: {dep.get('execution_error', dep)}")
            field = value.get("field", "data")
            if not isinstance(dep, dict) or field not in dep:
                raise DependencyError(f"dependency task {ref} has no field '{field}'")
            return dep[field]
        if isinstance(value, dict):
            return {k: resolve(v) for k, v in value.items()}
        if isinstance(value, list):
            return [resolve(v) for v in value]
        return value

    return resolve(args)
