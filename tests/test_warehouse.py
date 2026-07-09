"""Warehouse tools on the seeded Relay dataset: invariants and planted effects."""

from __future__ import annotations

import math
from datetime import date

from growth_copilot.warehouse import execute_tool

SEED_USERS = 800  # must match tests/conftest.py
SEED_DAYS = 60

SOLO_FUNNEL = ["workspace_created", "file_uploaded", "link_shared"]

UPGRADERS_SEGMENT = {
    "name": "wh_upgraders",
    "event_filters": [{"event": "plan_upgraded", "op": "at_least", "count": 1, "timeframe_days": SEED_DAYS}],
}


def test_seed_stats_sane(seeded_db):
    _, stats = seeded_db
    assert stats["users"] == SEED_USERS
    assert stats["events"] > stats["users"]  # every user emits at least account_created
    assert 0 < stats["upgraded_users"] < stats["users"]
    start, end = (date.fromisoformat(d) for d in stats["date_range"])
    assert (end - start).days == SEED_DAYS


def test_funnel_step_users_monotonically_non_increasing(con):
    result = execute_tool(con, "funnel_analysis", {"steps": SOLO_FUNNEL, "timeframe_days": SEED_DAYS})
    steps = result["data"]["steps"]
    assert result["status"] == "success"
    users = [s["users"] for s in steps]
    assert users[0] > 0
    assert all(a >= b for a, b in zip(users, users[1:], strict=False))
    for step in steps:
        assert 0.0 <= step["conversion_from_start"] <= 1.0
    assert steps[0]["conversion_from_start"] == 1.0


def test_segment_exports_create_rows_and_are_idempotent(con):
    args = {
        "steps": ["account_created", "file_uploaded", "plan_upgraded"],
        "timeframe_days": SEED_DAYS,
        "export_stalled_at_step": 2,
        "export_converted": True,
        "segment_name": "wh_export_test",
    }
    first = execute_tool(con, "funnel_analysis", args)
    assert first["stalled_segment_name"] == "wh_export_test_stalled"
    assert first["converted_segment_name"] == "wh_export_test_converted"

    def count(name: str) -> int:
        cur = con.cursor()
        try:
            return cur.execute("SELECT COUNT(*) FROM segments WHERE segment_name = ?", [name]).fetchone()[0]
        finally:
            cur.close()

    stalled_1, converted_1 = count("wh_export_test_stalled"), count("wh_export_test_converted")
    assert stalled_1 > 0
    assert converted_1 > 0
    execute_tool(con, "funnel_analysis", args)  # re-run must not duplicate segment rows
    assert count("wh_export_test_stalled") == stalled_1
    assert count("wh_export_test_converted") == converted_1


def test_segment_event_discovery_event_names_and_lift_ordering(con):
    execute_tool(con, "segment_definition", UPGRADERS_SEGMENT)
    result = execute_tool(con, "segment_event_discovery", {"segment": "wh_upgraders", "top_n": 10})
    rows = result["data"]["rows"]
    assert rows
    assert len(rows) <= 10
    assert result["event_names"] == [r["event"] for r in rows]

    # Rows are ordered most-distinctive first: distinctiveness = |log(lift)|.
    def distinctiveness(row: dict) -> float:
        return abs(math.log(row["lift"])) if row["lift"] > 0 else abs(math.log(1e-4))

    keys = [distinctiveness(r) for r in rows]
    assert keys == sorted(keys, reverse=True)


def test_profile_segment_deviations_respect_threshold(con):
    execute_tool(con, "segment_definition", UPGRADERS_SEGMENT)
    loose = execute_tool(con, "profile_segment", {"segment": "wh_upgraders", "min_relative_change": 0.3})
    deviations = loose["data"]["deviations"]
    assert deviations  # upgraders skew on planted drivers (trial plan, referral channel)
    assert all(abs(d["relative_change"]) >= 0.3 for d in deviations)

    tight = execute_tool(con, "profile_segment", {"segment": "wh_upgraders", "min_relative_change": 0.8})
    assert all(abs(d["relative_change"]) >= 0.8 for d in tight["data"]["deviations"])
    assert len(tight["data"]["deviations"]) <= len(deviations)


def test_product_paths_contain_endpoints_without_consecutive_duplicates(con):
    result = execute_tool(
        con,
        "product_paths",
        {"start_event": "account_created", "end_event": "plan_upgraded", "timeframe_days": SEED_DAYS, "top_n": 8},
    )
    paths = result["data"]["paths"]
    assert paths
    for entry in paths:
        seq = entry["path"].split(" -> ")
        assert seq[0] == "account_created"
        assert seq[-1] == "plan_upgraded"
        assert all(a != b for a, b in zip(seq, seq[1:], strict=False))
        assert entry["users"] > 0


def test_insight_query_by_week_returns_rows(con):
    result = execute_tool(
        con,
        "insight_query",
        {"events": ["account_created"], "metric": "unique_users", "by_week": True, "timeframe_days": SEED_DAYS},
    )
    rows = result["data"]["rows"]
    assert rows
    for row in rows:
        assert set(row) == {"week", "value"}
        assert row["value"] >= 0
    assert [r["week"] for r in rows] == sorted(r["week"] for r in rows)
    assert result["data"]["total"] > 0


def test_funnel_breakdown_surfaces_planted_mobile_blocker(con):
    result = execute_tool(
        con,
        "funnel_breakdown",
        {"steps": ["workspace_created", "file_uploaded"], "timeframe_days": SEED_DAYS},
    )
    data = result["data"]
    assert 0.0 < data["base_conversion"] < 1.0
    blockers = {(b["property"], b["value"]): b for b in data["blockers"]}
    # The flagship planted effect: mobile uploads at half the desktop rate.
    assert ("device", "mobile") in blockers
    assert blockers[("device", "mobile")]["lift"] < 0
    assert blockers[("device", "mobile")]["affected_users"] > 0


def test_path_window_is_minimal_across_interior_restarts():
    from growth_copilot.warehouse.analytics import _path_window

    # An interior restart must not inflate the window: the LAST start before
    # the first end wins.
    seq = ["account_created", "file_uploaded", "account_created", "plan_upgraded"]
    assert _path_window(seq, "account_created", "plan_upgraded") == ["account_created", "plan_upgraded"]
    # end before any start: fall forward to the first start's window
    seq2 = ["plan_upgraded", "account_created", "file_uploaded", "plan_upgraded"]
    assert _path_window(seq2, "account_created", "plan_upgraded") == [
        "account_created", "file_uploaded", "plan_upgraded",
    ]
    assert _path_window(["file_uploaded"], "account_created", "plan_upgraded") is None
