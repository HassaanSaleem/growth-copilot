"""CLI: seed the warehouse, ask questions, run recipes, drill into bottlenecks.

Progress streams live from the graph's custom stream writer — the same
event feed the API's SSE endpoint serves.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from growth_copilot.config import get_settings

app = typer.Typer(help="growth-copilot: plain-English product analytics on LangGraph + DuckDB.")
console = Console()


def _require_warehouse() -> None:
    """A missing warehouse is an environment error, not an analytic result —
    fail loudly instead of seeding an empty database and 'succeeding'."""
    settings = get_settings()
    if not settings.db_path.exists():
        console.print(
            f"[red]No warehouse at {settings.db_path}[/red] — run [bold]growth-copilot seed[/bold] first "
            "(data lands in ./data/ relative to your current directory; override with GROWTH_COPILOT_DB)."
        )
        raise typer.Exit(1)


def _checkpointer():
    from langgraph.checkpoint.sqlite import SqliteSaver

    settings = get_settings()
    settings.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    import sqlite3

    conn = sqlite3.connect(str(settings.checkpoint_path), check_same_thread=False)
    return SqliteSaver(conn)


def _render_event(event: dict[str, Any]) -> None:
    kind = event.get("type")
    if kind == "triage":
        console.print(f"[dim]triage →[/dim] {event['intent']}")
    elif kind == "plan":
        table = Table(title=f"Plan ({event['source']})", show_lines=False)
        table.add_column("id"), table.add_column("tool"), table.add_column("depends on"), table.add_column("args")
        for t in event["tasks"]:
            table.add_row(
                str(t["id"]), t.get("tool", t.get("name", "?")),
                ",".join(map(str, t.get("depends_on", []))) or "—",
                json.dumps(t.get("args", {}))[:80],
            )
        console.print(table)
    elif kind == "grounding":
        for c in event["corrections"]:
            if c.get("to"):
                console.print(f"[yellow]grounded[/yellow] task {c['task_id']} {c['field']}: "
                              f"'{c['from']}' → '{c['to']}' (score {c['score']})")
            else:
                console.print(f"[red]unresolved[/red] task {c['task_id']} {c['field']}: '{c['from']}'")
    elif kind == "task_started":
        console.print(f"[dim]▶ task {event['task_id']} ({event['tool']})[/dim]")
    elif kind == "task_finished":
        color = "green" if event["status"] == "success" else "red"
        console.print(f"[{color}]✔ task {event['task_id']} ({event['tool']}) — {event['status']}[/{color}]")
    elif kind == "finding":
        console.print(f"[cyan]finding:[/cyan] {event['kind']}")
    elif kind == "drilldown_expand":
        label = ", ".join(f"{k}={v}" for k, v in event["filters"].items()) or "all users"
        console.print(f"[dim]expanding cohort (depth {event['depth']}): {label}[/dim]")


def _render_answer(answer: dict[str, Any]) -> None:
    body = [answer.get("summary", "")]
    if answer.get("insights"):
        body.append("\n[bold]Insights[/bold]")
        body.extend(f"• {line}" for line in answer["insights"])
    if answer.get("recommendations"):
        body.append("\n[bold]Recommendations[/bold]")
        body.extend(f"• {line}" for line in answer["recommendations"])
    console.print(Panel("\n".join(body), title="Answer", border_style="green"))


def _resolve_thread(graph, base: str) -> str:
    """Reuse a thread only when it is genuinely resumable (pending work from
    an interrupt or crash). A completed thread must NOT be reused: its
    accumulating reducers (results, findings, drill-down nodes) would merge
    stale state into the new run."""
    thread = base
    suffix = 1
    while True:
        state = graph.get_state({"configurable": {"thread_id": thread}})
        if not state.values or state.next:
            return thread
        suffix += 1
        thread = f"{base}-r{suffix}"


def _run(graph, payload: Any, config: dict[str, Any]) -> None:
    from langgraph.types import Command

    config = dict(config)
    config["configurable"] = {
        **config["configurable"],
        "thread_id": _resolve_thread(graph, config["configurable"]["thread_id"]),
    }

    while True:
        interrupted = None
        for mode, chunk in graph.stream(payload, config, stream_mode=["custom", "updates"]):
            if mode == "custom":
                _render_event(chunk)
            elif mode == "updates" and "__interrupt__" in chunk:
                interrupted = chunk["__interrupt__"]
        if interrupted:
            questions = interrupted[0].value.get("questions", [])
            console.print("[bold yellow]The copilot needs clarification:[/bold yellow]")
            for q in questions:
                console.print(f"  • {q}")
            answer = typer.prompt("Your answer")
            payload = Command(resume=answer)
            continue
        break
    state = graph.get_state(config)
    answer = state.values.get("answer") or {}
    if state.values.get("report"):
        console.print(Panel(state.values["report"], title="Bottleneck drill-down", border_style="cyan"))
    elif answer:
        _render_answer(answer)


@app.command()
def seed(
    users: int = typer.Option(20000, help="number of synthetic users"),
    days: int = typer.Option(120, help="days of event history"),
    seed: int = typer.Option(42, help="RNG seed (fully deterministic dataset)"),
) -> None:
    """Generate the synthetic SaaS warehouse (fictional product: 'Relay')."""
    from growth_copilot import warehouse

    settings = get_settings()
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    stats = warehouse.seed(settings.db_path, users=users, days=days, seed=seed)
    console.print(f"[green]Seeded[/green] {settings.db_path}: {json.dumps(stats)}")


@app.command()
def ask(
    question: str = typer.Argument(..., help="plain-English analytics question"),
    thread: str = typer.Option("", help="thread id to resume (defaults to a hash of the question)"),
) -> None:
    """Ask a question. Requires ANTHROPIC_API_KEY (LLM planning + synthesis)."""
    _require_warehouse()
    from growth_copilot.graph.build import build_graph, thread_id_for

    graph = build_graph(checkpointer=_checkpointer())
    config = {
        "configurable": {"thread_id": thread or thread_id_for(question)},
        "max_concurrency": get_settings().max_parallel_tasks,
    }
    _run(graph, {"question": question}, config)


@app.command()
def recipe(
    name: str = typer.Argument(..., help="recipe name (see `growth-copilot recipes`)"),
    param: list[str] = typer.Option([], "--param", "-p", help="override recipe params, e.g. -p timeframe_days=30"),
) -> None:
    """Run a saved recipe — fully deterministic, no LLM required."""
    _require_warehouse()
    from growth_copilot.domain.tasks import load_recipe
    from growth_copilot.graph.build import build_graph

    overrides = dict(p.split("=", 1) for p in param)
    loaded = load_recipe(name)
    plan = loaded.resolve(overrides)
    graph = build_graph(checkpointer=_checkpointer())
    # sha256, not hash(): built-in str hashing is salted per process, which
    # would make a crashed run unfindable on retry.
    params_digest = hashlib.sha256(json.dumps(overrides, sort_keys=True).encode()).hexdigest()[:8]
    config = {
        "configurable": {"thread_id": f"recipe-{name}-{params_digest}"},
        "max_concurrency": get_settings().max_parallel_tasks,
    }
    payload = {
        "question": loaded.description or f"Run recipe {name}",
        "plan": [t.model_dump(mode="json") for t in plan.tasks],
    }
    _run(graph, payload, config)


@app.command()
def recipes() -> None:
    """List saved recipes."""
    from growth_copilot.domain.tasks import list_recipes

    table = Table(title="Recipes")
    table.add_column("name"), table.add_column("params"), table.add_column("description")
    for r in list_recipes():
        table.add_row(r.name, json.dumps(r.params), r.description)
    console.print(table)


@app.command()
def drilldown(
    steps: str = typer.Option(
        "workspace_created,file_uploaded,link_shared",
        help="comma-separated funnel events",
    ),
    days: int = typer.Option(90, help="lookback window"),
    depth: int = typer.Option(3, help="max recursion depth"),
) -> None:
    """Recursive bottleneck drill-down: the analysis tree grows during execution."""
    _require_warehouse()
    from growth_copilot.graph.drilldown import build_drilldown_graph

    graph = build_drilldown_graph(checkpointer=_checkpointer())
    config = {
        "configurable": {"thread_id": f"drilldown-{steps}-{days}-{depth}"},
        "max_concurrency": get_settings().max_parallel_tasks,
        "recursion_limit": 100,
    }
    payload = {"steps": [s.strip() for s in steps.split(",")], "timeframe_days": days, "max_depth": depth}
    _run(graph, payload, config)


@app.command()
def serve(port: int = typer.Option(8000), host: str = typer.Option("127.0.0.1")) -> None:
    """Serve the HTTP API (SSE streaming of the same event feed)."""
    import uvicorn

    uvicorn.run("growth_copilot.api:create_app", host=host, port=port, factory=True)


if __name__ == "__main__":
    app()
