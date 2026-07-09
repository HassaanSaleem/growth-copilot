"""Ground-truth validation of planned args — LLM for judgment, code for truth.

After planning, every event name and property reference in the plan is
checked against the warehouse metadata catalog. Near-misses (an LLM writing
`file_uploade` for `file_uploaded`) are fuzzy-corrected with
difflib at a 0.9 similarity threshold; anything below threshold is left
untouched and reported. Grounding is fail-open: it improves plans, it never
blocks them.
"""

from __future__ import annotations

import difflib
from typing import Any

from growth_copilot.domain.tasks import Task

SIMILARITY_THRESHOLD = 0.9

# arg keys whose values are event names
EVENT_ARG_KEYS = {"steps", "events", "allowed_events", "start_event", "end_event", "event"}
# arg keys whose values are user-property names
PROPERTY_ARG_KEYS = {"breakdown_property", "group_by_property"}


def _best_match(value: str, candidates: list[str]) -> tuple[str, float]:
    if not candidates:
        return value, 0.0
    scored = [(c, difflib.SequenceMatcher(None, value.lower(), c.lower()).ratio()) for c in candidates]
    return max(scored, key=lambda pair: pair[1])


def _correct(value: str, candidates: list[str], record: dict[str, Any], corrections: list) -> str:
    if value in candidates:
        return value
    match, score = _best_match(value, candidates)
    if score >= SIMILARITY_THRESHOLD:
        corrections.append({**record, "from": value, "to": match, "score": round(score, 3)})
        return match
    corrections.append({**record, "from": value, "to": None, "score": round(score, 3)})
    return value


def ground_plan(
    tasks: list[Task],
    known_events: list[str],
    known_properties: list[str],
    property_values: dict[str, list[str]],
) -> tuple[list[Task], list[dict[str, Any]]]:
    """Return (corrected tasks, correction records)."""
    corrections: list[dict[str, Any]] = []
    grounded: list[Task] = []
    for task in tasks:
        args = dict(task.args)
        for key, value in list(args.items()):
            record = {"task_id": task.id, "field": key}
            if key in EVENT_ARG_KEYS:
                if isinstance(value, str):
                    args[key] = _correct(value, known_events, record, corrections)
                elif isinstance(value, list) and all(isinstance(v, str) for v in value):
                    args[key] = [_correct(v, known_events, record, corrections) for v in value]
            elif key in PROPERTY_ARG_KEYS and isinstance(value, str):
                args[key] = _correct(value, known_properties, record, corrections)
            elif key == "event_filters" and isinstance(value, list):
                fixed_filters = []
                for i, item in enumerate(value):
                    if isinstance(item, dict) and isinstance(item.get("event"), str):
                        item = {
                            **item,
                            "event": _correct(
                                item["event"],
                                known_events,
                                {**record, "field": f"event_filters[{i}].event"},
                                corrections,
                            ),
                        }
                    fixed_filters.append(item)
                args[key] = fixed_filters
            elif key == "filters" and isinstance(value, dict):
                fixed: dict[str, Any] = {}
                for prop, filter_value in value.items():
                    prop2 = _correct(prop, known_properties, {**record, "field": f"filters.{prop}"}, corrections)
                    if isinstance(filter_value, str) and property_values.get(prop2):
                        filter_value = _correct(
                            filter_value,
                            property_values[prop2],
                            {**record, "field": f"filters.{prop2}=" },
                            corrections,
                        )
                    fixed[prop2] = filter_value
                args[key] = fixed
        grounded.append(task.model_copy(update={"args": args}))
    # Only keep records that changed something or flag a low-confidence miss.
    corrections = [c for c in corrections if c["to"] != c["from"]]
    return grounded, corrections
