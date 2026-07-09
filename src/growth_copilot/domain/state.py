"""Shared graph state.

One flat TypedDict flows through the whole graph; parallel `Send` branches
write into `results` through a merge reducer so fan-out never loses data.
Task results use string keys because checkpoint/JSON round-trips do not
preserve int dict keys.
"""

from __future__ import annotations

from typing import Annotated, Any, TypedDict


def merge_results(left: dict[str, Any] | None, right: dict[str, Any] | None) -> dict[str, Any]:
    return {**(left or {}), **(right or {})}


def append_list(left: list | None, right: list | None) -> list:
    return [*(left or []), *(right or [])]


class CopilotState(TypedDict, total=False):
    question: str
    clarifications: list[str]
    # questions the clarify node will suspend on (set by a clarify verdict)
    pending_questions: list[str]
    # triage verdict: analyze | clarify | greeting | off_topic
    intent: str
    reply: str
    # the plan (serialized Task dicts) — the auditable recipe
    plan: list[dict[str, Any]]
    # grounding correction records: {task_id, field, from, to, score}
    grounding: Annotated[list[dict[str, Any]], append_list]
    # task_id (as str) -> tool result; merged across parallel Send branches
    results: Annotated[dict[str, Any], merge_results]
    # emergent post-execution findings (auto-comparisons etc.)
    findings: Annotated[list[dict[str, Any]], append_list]
    # final {summary, insights, recommendations}
    answer: dict[str, Any]
    error: str


class TaskInvocation(TypedDict):
    """Private input state for one `Send("run_task", ...)` branch."""

    task: dict[str, Any]
    dep_results: dict[str, Any]
