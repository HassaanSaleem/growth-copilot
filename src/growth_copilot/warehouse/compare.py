"""Deterministic cohort comparisons — code computes the truth, the LLM only
narrates it.

Both functions take two finished tool results, join their rows, and keep
only differences of at least 5 percentage points. When an item appears on
one side only (top-N truncation), the missing side falls back to the shared
population baseline — the least-surprising honest estimate.
"""

from __future__ import annotations

from typing import Any

MIN_DELTA_PP = 0.05  # 5 percentage points


def _label(result: dict[str, Any], fallback: str) -> str:
    return result.get("data", {}).get("segment") or fallback


def _finish(deltas: list[dict[str, Any]], label_key: str) -> tuple[list[dict[str, Any]], list[str]]:
    deltas.sort(key=lambda d: abs(d["delta"]), reverse=True)
    highlights = [
        f"{d[label_key]}: {d['a_pct'] * 100:.1f}% vs {d['b_pct'] * 100:.1f}% ({d['delta'] * 100:+.1f}pp)"
        for d in deltas[:5]
    ]
    return deltas, highlights


def compare_discoveries(result_a: dict[str, Any], result_b: dict[str, Any]) -> dict[str, Any]:
    """Delta of per-event participation between two segment_event_discovery results."""
    rows_a = {r["event"]: r for r in result_a.get("data", {}).get("rows", [])}
    rows_b = {r["event"]: r for r in result_b.get("data", {}).get("rows", [])}
    deltas = []
    for event in sorted(set(rows_a) | set(rows_b)):
        row_a, row_b = rows_a.get(event), rows_b.get(event)
        baseline = (row_a or row_b)["population_pct"]
        a_pct = row_a["segment_pct"] if row_a else baseline
        b_pct = row_b["segment_pct"] if row_b else baseline
        delta = round(a_pct - b_pct, 4)
        if abs(delta) >= MIN_DELTA_PP:
            deltas.append({"event": event, "a_pct": a_pct, "b_pct": b_pct, "delta": delta})
    deltas, highlights = _finish(deltas, "event")
    return {
        "segment_a": _label(result_a, "segment A"),
        "segment_b": _label(result_b, "segment B"),
        "deltas": deltas,
        "highlights": highlights,
    }


def compare_profiles(result_a: dict[str, Any], result_b: dict[str, Any]) -> dict[str, Any]:
    """Delta of property-value composition between two profile_segment results."""
    dev_a = {(d["property"], d["value"]): d for d in result_a.get("data", {}).get("deviations", [])}
    dev_b = {(d["property"], d["value"]): d for d in result_b.get("data", {}).get("deviations", [])}
    deltas = []
    for key in sorted(set(dev_a) | set(dev_b)):
        row_a, row_b = dev_a.get(key), dev_b.get(key)
        baseline = (row_a or row_b)["population_pct"]
        a_pct = row_a["segment_pct"] if row_a else baseline
        b_pct = row_b["segment_pct"] if row_b else baseline
        delta = round(a_pct - b_pct, 4)
        if abs(delta) >= MIN_DELTA_PP:
            prop, value = key
            deltas.append(
                {"property": prop, "value": value, "label": f"{prop}={value}",
                 "a_pct": a_pct, "b_pct": b_pct, "delta": delta}
            )
    deltas, highlights = _finish(deltas, "label")
    return {
        "segment_a": _label(result_a, "segment A"),
        "segment_b": _label(result_b, "segment B"),
        "deltas": deltas,
        "highlights": highlights,
    }
