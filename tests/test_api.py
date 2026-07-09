"""The API serves the same typed event feed over SSE — exercised offline
through the recipe endpoint, in-process via httpx's ASGI transport."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from growth_copilot.api import create_app


def _collect_sse_events(path: str, body: dict[str, Any]) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        transport = httpx.ASGITransport(app=create_app())
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            async with client.stream("POST", path, json=body) as response:
                assert response.status_code == 200
                assert response.headers["content-type"].startswith("text/event-stream")
                events = []
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        events.append(json.loads(line.removeprefix("data: ")))
                return events

    return asyncio.run(run())


def test_recipe_run_streams_typed_events_offline():
    events = _collect_sse_events("/recipes/conversion-blockers/run", {"params": {}})
    kinds = [e["type"] for e in events]

    # The full fan-out is visible in the stream: every task starts and
    # finishes, and both auto-comparisons surface as findings.
    assert kinds.count("task_started") == 5
    assert kinds.count("task_finished") == 5
    assert all(e["status"] == "success" for e in events if e["type"] == "task_finished")
    assert kinds.count("finding") == 2

    # The stream ends with the synthesized answer, then the final payload.
    assert kinds[-2:] == ["answer", "final"]
    assert "5/5" in events[-1]["answer"]["summary"]
