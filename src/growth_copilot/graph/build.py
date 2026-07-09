"""Wire the copilot graph.

    START ─→ triage ─┬→ respond_direct ─→ END
                     ├→ clarify (interrupt) ─→ triage
                     └→ plan ─→ ground ─→ schedule ⇄ run_task (Send fan-out)
                                              │
                                              └→ enrich ─→ synthesize ─→ END

A state that already carries a plan (recipe mode) enters at `ground`,
skipping both LLM stages. `schedule` re-routes after every completed level:
tasks fan out with `Send` as soon as their dependencies are done, which is
exactly Kahn's-algorithm level scheduling expressed as graph supersteps.
"""

from __future__ import annotations

import hashlib

from langgraph.graph import END, START, StateGraph

from growth_copilot.domain.state import CopilotState
from growth_copilot.graph import nodes


def _route_entry(state: CopilotState) -> str:
    return "ground" if state.get("plan") else "triage"


def build_graph(checkpointer=None):
    g = StateGraph(CopilotState)
    g.add_node("triage", nodes.triage)
    g.add_node("clarify", nodes.clarify)
    g.add_node("respond_direct", nodes.respond_direct)
    g.add_node("plan", nodes.plan_node)
    g.add_node("ground", nodes.ground)
    g.add_node("schedule", nodes.schedule)
    g.add_node("run_task", nodes.run_task)
    g.add_node("enrich", nodes.enrich)
    g.add_node("synthesize", nodes.synthesize)

    g.add_conditional_edges(START, _route_entry, {"triage": "triage", "ground": "ground"})
    g.add_conditional_edges(
        "triage",
        nodes.route_triage,
        {"plan": "plan", "clarify": "clarify", "respond_direct": "respond_direct"},
    )
    g.add_edge("clarify", "triage")
    g.add_edge("respond_direct", END)
    g.add_edge("plan", "ground")
    g.add_edge("ground", "schedule")
    g.add_conditional_edges("schedule", nodes.route_schedule, ["run_task", "enrich"])
    g.add_edge("run_task", "schedule")
    g.add_edge("enrich", "synthesize")
    g.add_edge("synthesize", END)
    return g.compile(checkpointer=checkpointer)


def thread_id_for(question: str) -> str:
    """Deterministic thread id: retrying the same question after an interrupt
    or crash lands on the same checkpointed thread, so planning work already
    paid for is not repeated. Completed threads are never reused (see the
    CLI's thread resolution) — a finished run's reducers would leak state
    into the retry."""
    return hashlib.sha256(question.strip().lower().encode()).hexdigest()[:16]
