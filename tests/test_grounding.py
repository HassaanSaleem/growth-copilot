"""Grounding: correct confident near-misses, never guess below threshold."""

from __future__ import annotations

from growth_copilot.domain.tasks import Task
from growth_copilot.grounding import SIMILARITY_THRESHOLD, ground_plan

EVENTS = ["account_created", "workspace_created", "file_uploaded", "link_shared", "plan_upgraded"]
PROPERTIES = ["country", "device", "channel", "plan", "company_size"]
PROPERTY_VALUES = {
    "channel": ["organic_search", "paid_ads", "referral", "product_hunt", "outbound"],
    "device": ["desktop", "mobile", "tablet"],
}


def _ground(task: Task):
    return ground_plan([task], EVENTS, PROPERTIES, PROPERTY_VALUES)


def test_near_miss_event_corrected_at_threshold():
    task = Task(id=1, tool="funnel_analysis", args={"steps": ["account_created", "file_uploade"]})
    [grounded], corrections = _ground(task)
    assert grounded.args["steps"] == ["account_created", "file_uploaded"]
    assert len(corrections) == 1
    record = corrections[0]
    assert record["from"] == "file_uploade"
    assert record["to"] == "file_uploaded"
    assert record["score"] >= SIMILARITY_THRESHOLD


def test_below_threshold_left_unchanged_but_recorded():
    task = Task(id=3, tool="insight_query", args={"events": ["purchase"]})
    [grounded], corrections = _ground(task)
    assert grounded.args["events"] == ["purchase"]  # a low-confidence guess would be worse than no fix
    assert len(corrections) == 1
    record = corrections[0]
    assert record["from"] == "purchase"
    assert record["to"] is None
    assert record["score"] < SIMILARITY_THRESHOLD


def test_filters_property_and_value_corrected():
    task = Task(
        id=2,
        tool="funnel_analysis",
        args={"steps": ["account_created", "plan_upgraded"], "filters": {"chanel": "referal"}},
    )
    [grounded], corrections = _ground(task)
    assert grounded.args["filters"] == {"channel": "referral"}
    by_field = {c["field"]: c for c in corrections}
    assert by_field["filters.chanel"]["to"] == "channel"
    assert by_field["filters.channel="]["to"] == "referral"


def test_correction_record_shape():
    task = Task(id=7, tool="funnel_analysis", args={"steps": ["account_created", "mystery_event"]})
    _, corrections = _ground(task)
    assert corrections
    for record in corrections:
        assert set(record) == {"task_id", "field", "from", "to", "score"}
        assert record["task_id"] == 7
        assert record["field"] == "steps"
        assert isinstance(record["score"], float)


def test_exact_names_produce_no_records():
    task = Task(
        id=4,
        tool="funnel_analysis",
        args={"steps": ["account_created", "plan_upgraded"], "filters": {"device": "mobile"}},
    )
    [grounded], corrections = _ground(task)
    assert grounded.args == task.args
    assert corrections == []


def test_event_filters_events_are_grounded():
    task = Task(
        id=9,
        tool="segment_definition",
        args={
            "name": "upgraders",
            "event_filters": [{"event": "plan_upgradedd", "op": "at_least", "count": 1, "timeframe_days": 60}],
        },
    )
    [grounded], corrections = _ground(task)
    assert grounded.args["event_filters"][0]["event"] == "plan_upgraded"
    assert corrections and corrections[0]["field"] == "event_filters[0].event"
