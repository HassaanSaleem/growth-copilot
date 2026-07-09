"""Interrupt/resume and checkpoint-resume semantics, fully offline.

Where a stage needs LLM judgment to reach the path under test (a `clarify`
verdict cannot happen offline), a scripted chat stands in for the model at
the `get_chat` seam; everything downstream of the verdicts is the real
deterministic pipeline against the seeded test warehouse.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from growth_copilot.domain.tasks import Plan, load_recipe
from growth_copilot.graph import build_graph
from growth_copilot.graph.build import thread_id_for
from growth_copilot.graph.nodes import TriageVerdict


class ScriptedChat:
    """Stands in for ChatAnthropic: pops pre-scripted structured outputs."""

    def __init__(self, outputs: list[Any]):
        self.outputs = list(outputs)
        self.calls = 0

    def with_structured_output(self, schema: Any) -> ScriptedChat:
        return self

    def invoke(self, prompt: str) -> Any:
        self.calls += 1
        return self.outputs.pop(0)


def test_clarify_interrupt_resume_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    # Triage asks for clarification once, then accepts; the planner emits one
    # task; synthesis gets None and falls back to the deterministic answer.
    triage_chat = ScriptedChat(
        [
            TriageVerdict(intent="clarify", questions=["Which funnel do you mean?"]),
            TriageVerdict(intent="analyze", refined_question="signup volume over the last 60 days"),
        ]
    )
    planner_chat = ScriptedChat(
        [
            Plan.model_validate(
                {
                    "tasks": [
                        {
                            "id": 1,
                            "tool": "insight_query",
                            "args": {
                                "events": ["account_created"],
                                "metric": "unique_users",
                                "timeframe_days": 60,
                            },
                            "depends_on": [],
                        }
                    ]
                }
            )
        ]
    )

    def scripted_get_chat(stage: str, max_tokens: int = 4096) -> ScriptedChat | None:
        return {"triage": triage_chat, "planner": planner_chat}.get(stage)

    monkeypatch.setattr("growth_copilot.graph.nodes.get_chat", scripted_get_chat)

    graph = build_graph(checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": "clarify-round-trip"}}

    # First pass suspends inside `clarify` — the interrupt carries the questions.
    first = graph.invoke({"question": "how is the funnel doing?"}, config)
    assert first["__interrupt__"][0].value == {"questions": ["Which funnel do you mean?"]}
    assert graph.get_state(config).next == ("clarify",)

    # Command(resume=...) replays clarify deterministically, loops back to
    # triage, and the run completes end-to-end.
    final = graph.invoke(Command(resume="the signup funnel"), config)
    assert final["clarifications"] == ["the signup funnel"]
    assert final["question"] == "signup volume over the last 60 days"
    assert final["results"]["1"]["status"] == "success"
    assert final["answer"]["summary"] == "Executed 1/1 tasks successfully."
    # Triage ran exactly twice (before and after the pause); planning once.
    assert triage_chat.calls == 2
    assert planner_chat.calls == 1


def test_same_question_resumes_from_sqlite_checkpoint(tmp_path: Any) -> None:
    """A run that dies mid-execution is resumed, not restarted: the second ask
    hashes to the same thread id, finds the SqliteSaver checkpoint from a fresh
    graph instance, and picks up at `schedule` — the already-grounded plan is
    reused and only task execution runs."""
    from langgraph.checkpoint.sqlite import SqliteSaver

    checkpoint_file = tmp_path / "checkpoints.sqlite"
    recipe = load_recipe("conversion-blockers")
    question = recipe.description
    payload = {"question": question, "plan": [t.model_dump(mode="json") for t in recipe.resolve().tasks]}
    config = {"configurable": {"thread_id": thread_id_for(question)}}

    # First "process": enters at ground (plan supplied) and crashes before any
    # task runs — simulated with a static breakpoint at `schedule`.
    conn1 = sqlite3.connect(str(checkpoint_file), check_same_thread=False)
    graph1 = build_graph(checkpointer=SqliteSaver(conn1))
    graph1.invoke(payload, config, interrupt_before=["schedule"])
    conn1.close()

    # Second "process": a brand-new graph over the same checkpoint file. The
    # deterministic thread id alone locates the pending run — grounded plan
    # checkpointed, no results yet, `schedule` next (the CLI's reuse condition).
    conn2 = sqlite3.connect(str(checkpoint_file), check_same_thread=False)
    graph2 = build_graph(checkpointer=SqliteSaver(conn2))
    pending = graph2.get_state(config)
    assert pending.values["question"] == question
    assert len(pending.values["plan"]) == 5
    assert not pending.values.get("results")
    assert pending.next == ("schedule",)

    # Resume (input=None). The stream starts directly with task execution:
    # no re-entry through plan or ground, so no plan/grounding events.
    events = []
    for mode, chunk in graph2.stream(None, config, stream_mode=["custom", "updates"]):
        if mode == "custom":
            events.append(chunk)
    kinds = [e["type"] for e in events]
    assert kinds[0] == "task_started"
    assert "plan" not in kinds and "grounding" not in kinds
    assert kinds.count("task_finished") == 5

    final = graph2.get_state(config)
    assert not final.next  # thread completed — the CLI would not reuse it again
    assert set(final.values["results"]) == {"1", "2", "3", "4", "5"}
    assert "5/5" in final.values["answer"]["summary"]
    conn2.close()
