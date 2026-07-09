"""HTTP API: the CLI's event feed over Server-Sent Events.

POST /ask streams the graph's progress events as SSE and ends with the
final answer; recipes run deterministically without an LLM. No auth — this
is a local demo service, not a deployment artifact.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from growth_copilot.catalog import load_catalog
from growth_copilot.config import get_settings
from growth_copilot.domain.tasks import list_recipes, load_recipe


class AskRequest(BaseModel):
    question: str
    thread_id: str | None = None


class RecipeRequest(BaseModel):
    params: dict[str, str] = {}


def _sse(event: dict[str, Any]) -> str:
    return f"data: {json.dumps(event, default=str)}\n\n"


async def _stream_graph(graph, payload: dict[str, Any], config: dict[str, Any]) -> AsyncIterator[str]:
    final: dict[str, Any] = {}
    async for mode, chunk in graph.astream(payload, config, stream_mode=["custom", "values"]):
        if mode == "custom":
            yield _sse(chunk)
        elif mode == "values":
            final = chunk
    if "__interrupt__" in final:
        yield _sse({"type": "interrupt", "detail": "clarification required; resume via CLI"})
    answer = final.get("answer")
    if answer:
        yield _sse({"type": "final", "answer": answer, "grounding": final.get("grounding", [])})
    if final.get("report"):
        yield _sse({"type": "final", "report": final["report"]})


def create_app() -> FastAPI:
    app = FastAPI(title="growth-copilot", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/catalog")
    def catalog() -> dict[str, Any]:
        return {"tools": list(load_catalog().values())}

    @app.get("/recipes")
    def recipes() -> dict[str, Any]:
        return {"recipes": [r.model_dump() for r in list_recipes()]}

    @app.post("/ask")
    async def ask(request: AskRequest) -> StreamingResponse:
        from growth_copilot.graph.build import build_graph, thread_id_for

        if not get_settings().api_key:
            raise HTTPException(400, "ANTHROPIC_API_KEY is not set; use POST /recipes/{name}/run instead")
        graph = build_graph()
        config = {
            "configurable": {"thread_id": request.thread_id or thread_id_for(request.question)},
            "max_concurrency": get_settings().max_parallel_tasks,
        }
        return StreamingResponse(
            _stream_graph(graph, {"question": request.question}, config), media_type="text/event-stream"
        )

    @app.post("/recipes/{name}/run")
    async def run_recipe(name: str, request: RecipeRequest) -> StreamingResponse:
        from growth_copilot.graph.build import build_graph

        try:
            recipe = load_recipe(name)
        except FileNotFoundError as exc:
            raise HTTPException(404, str(exc)) from exc
        plan = recipe.resolve(request.params)
        graph = build_graph()
        payload = {
            "question": recipe.description or f"Run recipe {name}",
            "plan": [t.model_dump(mode="json") for t in plan.tasks],
        }
        config = {"configurable": {"thread_id": f"api-recipe-{name}"},
                  "max_concurrency": get_settings().max_parallel_tasks}
        return StreamingResponse(_stream_graph(graph, payload, config), media_type="text/event-stream")

    return app
