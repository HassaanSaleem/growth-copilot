"""Graph nodes: triage → plan → ground → (Send fan-out) → enrich → synthesize.

Design rules carried over from production experience:
- Failures are data. A task that raises becomes an ``execution_error`` result;
  siblings keep running and downstream tasks see the error as input.
- LLM for judgment, code for truth. Planning and narration are LLM calls;
  grounding, scheduling, and comparisons are deterministic.
- Every stage emits progress through the stream writer, never through state.
"""

from __future__ import annotations

import json
from string import Template
from typing import Any, Literal

from langgraph.config import get_stream_writer
from langgraph.types import Send, interrupt
from pydantic import BaseModel, Field

from growth_copilot import warehouse
from growth_copilot.catalog import catalog_for_prompt, load_catalog
from growth_copilot.config import get_settings
from growth_copilot.domain.state import CopilotState, TaskInvocation
from growth_copilot.domain.tasks import Plan, Task
from growth_copilot.graph.binding import bind_dependencies
from growth_copilot.graph.scheduler import ready_tasks
from growth_copilot.grounding import ground_plan
from growth_copilot.llm import get_chat
from growth_copilot.prompts import load_prompt


class TriageVerdict(BaseModel):
    intent: Literal["analyze", "clarify", "greeting", "off_topic"]
    reply: str = Field(default="", description="direct reply for greeting/off_topic")
    questions: list[str] = Field(default_factory=list, description="clarifying questions when intent=clarify")
    refined_question: str = Field(default="", description="normalized analytical question when intent=analyze")


class Answer(BaseModel):
    summary: str
    insights: list[str] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)


# --------------------------------------------------------------------------- triage

MAX_CLARIFY_ROUNDS = 2


def triage(state: CopilotState) -> dict[str, Any]:
    writer = get_stream_writer()
    question = state["question"]
    clarifications = list(state.get("clarifications", []))
    llm = get_chat("triage")
    if llm is None:
        # Offline: no judgment available, treat everything as analyzable.
        return {"intent": "analyze"}

    catalog = warehouse.metadata_catalog(warehouse.get_connection(get_settings().db_path))
    prompt = Template(load_prompt("triage")).safe_substitute(
        question=question,
        clarifications="\n".join(clarifications) or "(none)",
        events=", ".join(catalog["events"]),
        properties=", ".join(catalog["properties"]),
    )
    verdict = llm.with_structured_output(TriageVerdict).invoke(prompt)
    writer({"type": "triage", "intent": verdict.intent})

    if verdict.intent == "clarify" and verdict.questions and len(clarifications) < MAX_CLARIFY_ROUNDS:
        # Don't interrupt here: an interrupt replays its whole node on resume,
        # which would re-run the LLM and could flip the verdict, discarding the
        # user's answer. Commit the verdict to state; the clarify node (whose
        # first statement is the interrupt) owns the suspend/resume cycle.
        return {"intent": "clarify", "pending_questions": verdict.questions}
    if verdict.intent in ("greeting", "off_topic"):
        return {"intent": verdict.intent, "reply": verdict.reply or "Hi! Ask me a product analytics question."}
    refined = verdict.refined_question or question
    return {"intent": "analyze", "question": refined}


def route_triage(state: CopilotState) -> str:
    intent = state.get("intent")
    if intent == "analyze":
        return "plan"
    if intent == "clarify":
        return "clarify"
    return "respond_direct"


def clarify(state: CopilotState) -> dict[str, Any]:
    """Suspend for the user's answer, then loop back to triage.

    interrupt() is the first statement so the node's replay on resume is
    deterministic — the verdict that triggered it is already checkpointed.
    """
    answer = interrupt({"questions": state.get("pending_questions", [])})
    return {
        "clarifications": [*state.get("clarifications", []), str(answer)],
        "question": f"{state['question']}\n\nClarification from the user: {answer}",
        "pending_questions": [],
    }


def respond_direct(state: CopilotState) -> dict[str, Any]:
    return {"answer": {"summary": state.get("reply", ""), "insights": [], "recommendations": []}}


# --------------------------------------------------------------------------- plan


def plan_node(state: CopilotState) -> dict[str, Any]:
    writer = get_stream_writer()
    if state.get("plan"):
        # Recipe mode: the plan was supplied up front; planning is a no-op.
        writer({"type": "plan", "source": "recipe", "tasks": state["plan"]})
        return {}

    llm = get_chat("planner", max_tokens=8192)
    if llm is None:
        raise RuntimeError(
            "No ANTHROPIC_API_KEY set. Either export a key for LLM planning, "
            "or run a saved recipe: `growth-copilot recipe <name>`."
        )
    catalog = warehouse.metadata_catalog(warehouse.get_connection(get_settings().db_path))
    prompt = Template(load_prompt("planner")).safe_substitute(
        question=state["question"],
        tools=catalog_for_prompt(),
        metadata=catalog["summary"],
    )
    structured = llm.with_structured_output(Plan)
    try:
        plan = structured.invoke(prompt)
    except Exception as exc:  # one self-correction retry with the validation error
        plan = structured.invoke(prompt + f"\n\nYour previous plan was invalid: {exc}. Emit a corrected plan.")
    writer({"type": "plan", "source": "llm", "tasks": [t.model_dump(mode="json") for t in plan.tasks]})
    return {"plan": [t.model_dump(mode="json") for t in plan.tasks]}


# --------------------------------------------------------------------------- ground


def ground(state: CopilotState) -> dict[str, Any]:
    writer = get_stream_writer()
    tasks = [Task.model_validate(t) for t in state["plan"]]
    unknown = [t.tool for t in tasks if t.tool not in load_catalog()]
    if unknown:
        raise ValueError(f"plan references unknown tools: {unknown}")
    # Two tasks exporting the same segment name would silently overwrite each
    # other's cohort; the names are statically known, so reject the plan here.
    exports: dict[str, int] = {}
    for t in tasks:
        for name in warehouse.exported_segment_names(t.tool, t.args):
            if name in exports:
                raise ValueError(
                    f"tasks {exports[name]} and {t.id} both export segment '{name}'; names must be unique"
                )
            exports[name] = t.id
    catalog = warehouse.metadata_catalog(warehouse.get_connection(get_settings().db_path))
    grounded, corrections = ground_plan(
        tasks, catalog["events"], catalog["properties"], catalog["property_values"]
    )
    Plan(tasks=grounded)  # re-validate the DAG after correction
    if corrections:
        writer({"type": "grounding", "corrections": corrections})
    return {"plan": [t.model_dump(mode="json") for t in grounded], "grounding": corrections}


# --------------------------------------------------------------------------- execute (Send fan-out)


def schedule(state: CopilotState) -> dict[str, Any]:
    """Barrier node. Routing happens in `route_schedule`; the node itself is a no-op."""
    return {}


def route_schedule(state: CopilotState) -> list[Send] | str:
    tasks = [Task.model_validate(t) for t in state["plan"]]
    done = {int(k) for k in state.get("results", {})}
    ready = ready_tasks(tasks, done)
    if not ready:
        return "enrich"
    results = state.get("results", {})
    return [
        Send(
            "run_task",
            TaskInvocation(
                task=t.model_dump(mode="json"),
                dep_results={str(d): results[str(d)] for d in t.depends_on},
            ),
        )
        for t in ready
    ]


def run_task(invocation: TaskInvocation) -> dict[str, Any]:
    writer = get_stream_writer()
    task = Task.model_validate(invocation["task"])
    writer({"type": "task_started", "task_id": task.id, "tool": task.tool})
    try:
        args = bind_dependencies(task.args, invocation["dep_results"])
        con = warehouse.get_connection(get_settings().db_path)
        result = warehouse.execute_tool(con, task.tool, args)
    except Exception as exc:
        # Failure isolation: the error becomes this task's result; the rest
        # of the graph keeps going and can reason about the failure.
        result = {"status": "error", "tool": task.tool, "execution_error": str(exc)}
    writer(
        {
            "type": "task_finished",
            "task_id": task.id,
            "tool": task.tool,
            "status": result.get("status", "success"),
        }
    )
    return {"results": {str(task.id): result}}


# --------------------------------------------------------------------------- enrich


def enrich(state: CopilotState) -> dict[str, Any]:
    """Emergent analysis: work the plan didn't ask for but the data invites.

    When a run produces exactly two comparable datasets (two segment
    discoveries, or two segment profiles — typically converted vs stalled
    cohorts), compute their deltas deterministically and surface only the
    significant differences.
    """
    writer = get_stream_writer()
    tasks = {str(t["id"]): t for t in state.get("plan", [])}
    results = state.get("results", {})
    findings: list[dict[str, Any]] = []

    for tool, compare in (
        ("segment_event_discovery", warehouse.compare_discoveries),
        ("profile_segment", warehouse.compare_profiles),
    ):
        matching = [
            (tid, res)
            for tid, res in results.items()
            if tasks.get(tid, {}).get("tool") == tool and res.get("status") == "success"
        ]
        if len(matching) == 2:
            (id_a, res_a), (id_b, res_b) = sorted(matching, key=lambda pair: int(pair[0]))
            try:
                finding = compare(res_a, res_b)
                finding.update({"kind": f"{tool}_comparison", "between_tasks": [int(id_a), int(id_b)]})
                findings.append(finding)
                writer({"type": "finding", "kind": finding["kind"]})
            except Exception as exc:
                findings.append({"kind": f"{tool}_comparison", "error": str(exc)})
    return {"findings": findings}


# --------------------------------------------------------------------------- synthesize


def _format_dependency_blocks(state: CopilotState, char_budget: int) -> str:
    """Frame each task result as a bounded block so one oversized result
    can never crowd out its siblings in the synthesis prompt."""
    tasks = {str(t["id"]): t for t in state.get("plan", [])}
    blocks: list[str] = []
    for tid in sorted(state.get("results", {}), key=int):
        tool = tasks.get(tid, {}).get("tool", "?")
        payload = json.dumps(state["results"][tid], default=str)
        if len(payload) > char_budget:
            payload = payload[:char_budget] + f'... [truncated at {char_budget} chars]'
        blocks.append(f"--- Task {tid} ({tool}) ---\n{payload}")
    for finding in state.get("findings", []):
        blocks.append(f"--- Finding ({finding.get('kind', 'comparison')}) ---\n{json.dumps(finding, default=str)}")
    return "\n\n".join(blocks)


def synthesize(state: CopilotState) -> dict[str, Any]:
    writer = get_stream_writer()
    settings = get_settings()
    context = _format_dependency_blocks(state, settings.dep_char_budget)
    llm = get_chat("synthesis", max_tokens=8192)
    if llm is None:
        answer = _deterministic_answer(state)
    else:
        prompt = Template(load_prompt("synthesize")).safe_substitute(
            question=state["question"], context=context
        )
        answer = llm.with_structured_output(Answer).invoke(prompt).model_dump()
    writer({"type": "answer", "answer": answer})
    return {"answer": answer}


def _deterministic_answer(state: CopilotState) -> dict[str, Any]:
    """Offline renderer: an honest, template-free readout of what ran."""
    results = state.get("results", {})
    tasks = {str(t["id"]): t for t in state.get("plan", [])}
    ok = sum(1 for r in results.values() if r.get("status") == "success")
    failed = {tid: r["execution_error"] for tid, r in results.items() if r.get("execution_error")}
    insights: list[str] = []
    for tid in sorted(results, key=int):
        res = results[tid]
        if res.get("status") != "success":
            continue
        headline = res.get("headline")
        if headline:
            insights.append(f"[task {tid} · {tasks.get(tid, {}).get('tool', '?')}] {headline}")
    for finding in state.get("findings", []):
        for line in finding.get("highlights", [])[:5]:
            insights.append(f"[comparison] {line}")
    summary = f"Executed {ok}/{len(results)} tasks successfully."
    if failed:
        summary += f" Failed tasks: {', '.join(f'{tid} ({err})' for tid, err in failed.items())}."
    return {"summary": summary, "insights": insights, "recommendations": []}
