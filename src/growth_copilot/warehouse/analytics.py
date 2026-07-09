"""Tool execution against the warehouse — SQL computes, dicts come out.

Contracts every handler honors:
- aggregates only; user-level rows never leave the warehouse
- every success returns {status, tool, data, headline}; rates rounded to 4 dp
- numeric args are coerced defensively (recipes pass strings)
- timeframes are anchored to the newest event, not wall-clock, so a seeded
  demo file keeps producing identical results regardless of when it is read
- every call runs on a fresh `con.cursor()` (its own session), which makes
  parallel Send branches and per-call TEMP tables mutually invisible
- SQL identifiers are allowlist-validated (USER_PROPERTIES) before
  interpolation; all values are bound `?` parameters
"""

from __future__ import annotations

import math
from collections import Counter
from datetime import datetime, timedelta
from typing import Any

import duckdb

from growth_copilot.warehouse.metadata import USER_PROPERTIES

Cursor = duckdb.DuckDBPyConnection

POPULATION_PCT_FLOOR = 0.005  # div-by-zero guard for lift ratios


# ------------------------------------------------------------------ helpers


def _int(value: Any, default: int) -> int:
    if value is None or value == "":
        return default
    return int(float(value))


def _float(value: Any, default: float) -> float:
    if value is None or value == "":
        return default
    return float(value)


def _bool(value: Any) -> bool:
    return value in (True, 1, "1", "true", "True", "yes")


def _anchor(cur: Cursor) -> datetime:
    row = cur.execute("SELECT MAX(ts) FROM events").fetchone()
    return row[0] if row and row[0] else datetime.now()


def _cutoff(cur: Cursor, timeframe_days: int) -> datetime:
    return _anchor(cur) - timedelta(days=timeframe_days)


def _filters_clause(filters: dict[str, Any] | None, alias: str = "u") -> tuple[str, list[Any]]:
    """Equality filters on user-profile properties, as (SQL fragment, params)."""
    if not filters:
        return "", []
    clauses, params = [], []
    for prop, value in filters.items():
        if prop not in USER_PROPERTIES:
            raise ValueError(f"unknown user property '{prop}'; known: {USER_PROPERTIES}")
        clauses.append(f"{alias}.{prop} = ?")
        params.append(str(value))
    return " WHERE " + " AND ".join(clauses), params


def _require_events(args: dict[str, Any], key: str, minimum: int = 1, maximum: int = 6) -> list[str]:
    value = args.get(key)
    if isinstance(value, str):
        value = [v.strip() for v in value.split(",") if v.strip()]
    if not isinstance(value, list) or len(value) < minimum or len(value) > maximum:
        raise ValueError(f"'{key}' must be a list of {minimum}-{maximum} event names")
    return [str(v) for v in value]


def _require_segment(cur: Cursor, args: dict[str, Any], key: str = "segment") -> tuple[str, int]:
    name = args.get(key)
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"'{key}' must be a segment name string (or a $ref to an exported segment)")
    name = name.strip()
    size = cur.execute(
        "SELECT COUNT(DISTINCT user_id) FROM segments WHERE segment_name = ?", [name]
    ).fetchone()[0]
    if not size:
        raise ValueError(f"segment '{name}' is unknown or empty")
    return name, int(size)


def _write_segment(cur: Cursor, name: str, select_sql: str, params: list[Any]) -> int:
    """Idempotent segment export: DELETE-then-INSERT under one name.

    Runs in an explicit transaction — parallel Send branches each get their
    own cursor, and without the transaction two same-named exports interleave
    into a silent union of both cohorts (and expose a deleted-but-not-yet-
    rewritten window to concurrent readers). Duplicate names are also rejected
    statically at grounding time; this guards the runtime invariant.
    """
    cur.execute("BEGIN TRANSACTION")
    try:
        cur.execute("DELETE FROM segments WHERE segment_name = ?", [name])
        cur.execute(f"INSERT INTO segments SELECT ?, user_id FROM ({select_sql})", [name, *params])
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise
    return int(cur.execute("SELECT COUNT(*) FROM segments WHERE segment_name = ?", [name]).fetchone()[0])


def exported_segment_names(task_tool: str, args: dict[str, Any]) -> list[str]:
    """The segment names a task will write, statically derivable from its args.

    Used by the grounding stage to reject plans where two tasks export the
    same name — the one plan shape that could corrupt downstream cohorts.
    """
    names: list[str] = []
    if task_tool == "funnel_analysis":
        base = args.get("segment_name")
        if base:
            if args.get("export_stalled_at_step"):
                names.append(f"{base}_stalled")
            if args.get("export_converted"):
                names.append(f"{base}_converted")
    elif task_tool == "segment_definition" and args.get("name"):
        names.append(str(args["name"]))
    return names


# ------------------------------------------------------- funnel core (shared)


def _create_reach_table(
    cur: Cursor,
    steps: list[str],
    timeframe_days: int,
    window_days: int,
    filters: dict[str, Any] | None,
) -> None:
    """Materialize the ordered-sequence funnel as TEMP table reach(user_id, reached).

    Step 1 is a user's first matching event in the timeframe; step N is the
    earliest matching event strictly after step N-1 and within
    `window_days` of step 1. Per-user reach is independent of cohort
    membership, so breakdowns can slice this one table instead of re-running
    the funnel per property value.
    """
    cutoff = _cutoff(cur, timeframe_days)
    where, fparams = _filters_clause(filters, alias="u")
    params: list[Any] = [*fparams, steps[0], cutoff]
    ctes = [
        f"cohort AS (SELECT user_id FROM users u{where})",
        "s1 AS (SELECT e.user_id, MIN(e.ts) AS ts, MIN(e.ts) AS first_ts "
        "FROM events e JOIN cohort c ON e.user_id = c.user_id "
        "WHERE e.event = ? AND e.ts >= ? GROUP BY e.user_id)",
    ]
    for i in range(1, len(steps)):
        ctes.append(
            f"s{i + 1} AS (SELECT e.user_id, MIN(e.ts) AS ts, MIN(p.first_ts) AS first_ts "
            f"FROM events e JOIN s{i} p ON e.user_id = p.user_id "
            f"WHERE e.event = ? AND e.ts > p.ts "
            f"AND e.ts <= p.first_ts + INTERVAL '1 day' * {window_days} GROUP BY e.user_id)"
        )
        params.append(steps[i])
    levels = " UNION ALL ".join(f"SELECT user_id, {i + 1} AS step FROM s{i + 1}" for i in range(len(steps)))
    cur.execute("DROP TABLE IF EXISTS reach")
    cur.execute(
        f"CREATE TEMPORARY TABLE reach AS WITH {', '.join(ctes)}, levels AS ({levels}) "
        "SELECT user_id, MAX(step) AS reached FROM levels GROUP BY user_id",
        params,
    )


def _users_per_step(counts_by_reach: dict[int, int], n_steps: int) -> list[int]:
    return [sum(n for reached, n in counts_by_reach.items() if reached >= k) for k in range(1, n_steps + 1)]


def _steps_payload(steps: list[str], users_at: list[int]) -> list[dict[str, Any]]:
    out = []
    for i, event in enumerate(steps):
        users = users_at[i]
        conv = round(users / users_at[0], 4) if users_at[0] else 0.0
        drop = 0.0 if i == 0 else (round(1 - users / users_at[i - 1], 4) if users_at[i - 1] else 0.0)
        out.append(
            {"event": event, "users": users, "conversion_from_start": conv, "lost_from_previous": drop}
        )
    return out


def _reach_counts(cur: Cursor) -> dict[int, int]:
    return dict(cur.execute("SELECT reached, COUNT(*) FROM reach GROUP BY reached").fetchall())


def _reach_counts_by_property(cur: Cursor, prop: str) -> dict[str, dict[int, int]]:
    rows = cur.execute(
        f"SELECT u.{prop}, r.reached, COUNT(*) FROM reach r JOIN users u ON r.user_id = u.user_id "
        "GROUP BY 1, 2"
    ).fetchall()
    by_value: dict[str, dict[int, int]] = {}
    for value, reached, n in rows:
        if value is None:
            continue
        by_value.setdefault(str(value), {})[reached] = n
    return by_value


# ------------------------------------------------------------------ handlers


def _funnel_analysis(cur: Cursor, args: dict[str, Any]) -> dict[str, Any]:
    steps = _require_events(args, "steps", minimum=2)
    timeframe = _int(args.get("timeframe_days"), 90)
    window = _int(args.get("conversion_window_days"), 14)
    filters = args.get("filters") or {}
    _create_reach_table(cur, steps, timeframe, window, filters)

    users_at = _users_per_step(_reach_counts(cur), len(steps))
    data: dict[str, Any] = {"steps": _steps_payload(steps, users_at)}
    result: dict[str, Any] = {"data": data}

    breakdown_property = args.get("breakdown_property")
    if breakdown_property:
        if breakdown_property not in USER_PROPERTIES:
            raise ValueError(f"unknown breakdown_property '{breakdown_property}'; known: {USER_PROPERTIES}")
        by_value = _reach_counts_by_property(cur, breakdown_property)
        per_value = {v: _users_per_step(c, len(steps)) for v, c in by_value.items()}
        top = sorted(per_value, key=lambda v: per_value[v][0], reverse=True)[:8]
        data["breakdown"] = {v: _steps_payload(steps, per_value[v]) for v in top}

    export_stalled = args.get("export_stalled_at_step")
    export_converted = _bool(args.get("export_converted"))
    if export_stalled not in (None, "") or export_converted:
        base = str(args.get("segment_name") or "").strip()
        if not base:
            raise ValueError("'segment_name' is required when using export_stalled_at_step/export_converted")
        if export_stalled not in (None, ""):
            k = _int(export_stalled, 0)
            if not 1 <= k < len(steps):
                raise ValueError(f"export_stalled_at_step must be between 1 and {len(steps) - 1}")
            name = f"{base}_stalled"
            n = _write_segment(cur, name, "SELECT user_id FROM reach WHERE reached = ?", [k])
            result["stalled_segment_name"] = name
            data["stalled_users"] = n
        if export_converted:
            name = f"{base}_converted"
            n = _write_segment(cur, name, "SELECT user_id FROM reach WHERE reached >= ?", [len(steps)])
            result["converted_segment_name"] = name
            data["converted_users"] = n

    if not users_at[0]:
        result["headline"] = f"No users performed {steps[0]} in the last {timeframe} days."
        return result
    conv = users_at[-1] / users_at[0]
    losses = [(i, users_at[i] - users_at[i + 1]) for i in range(len(steps) - 1)]
    worst, lost = max(losses, key=lambda pair: pair[1])
    worst_pct = lost / users_at[worst] if users_at[worst] else 0.0
    result["headline"] = (
        f"{conv:.1%} of {users_at[0]:,} users completed {' → '.join(steps)}; "
        f"biggest loss after {steps[worst]} (-{worst_pct:.1%}, {lost:,} users)"
    )
    return result


def _insight_query(cur: Cursor, args: dict[str, Any]) -> dict[str, Any]:
    events = _require_events(args, "events", minimum=1, maximum=20)
    metric = str(args.get("metric") or "unique_users")
    if metric not in ("unique_users", "event_count"):
        raise ValueError("metric must be 'unique_users' or 'event_count'")
    timeframe = _int(args.get("timeframe_days"), 90)
    by_week = _bool(args.get("by_week"))
    group_by = args.get("group_by_property")
    filters = args.get("filters") or {}

    agg = "COUNT(DISTINCT e.user_id)" if metric == "unique_users" else "COUNT(*)"
    where, fparams = _filters_clause(filters, alias="u")
    join = "JOIN users u ON e.user_id = u.user_id " if filters else ""
    conditions = f"e.event IN ({', '.join('?' for _ in events)}) AND e.ts >= ?"
    if where:
        conditions += " AND " + where.removeprefix(" WHERE ")
    params: list[Any] = [*events, _cutoff(cur, timeframe), *fparams]
    base = f"FROM events e {join}WHERE {conditions}"

    total = int(cur.execute(f"SELECT {agg} {base}", params).fetchone()[0])
    data: dict[str, Any] = {"metric": metric, "events": events, "total": total}

    if by_week:
        # Simplification: when both by_week and group_by_property are set,
        # we group by week only — one dimension per query keeps rows flat.
        if group_by:
            data["note"] = "group_by_property ignored because by_week=true (grouped by week only)"
        rows = cur.execute(
            f"SELECT CAST(date_trunc('week', e.ts) AS DATE) AS week, {agg} {base} GROUP BY 1 ORDER BY 1",
            params,
        ).fetchall()
        data["rows"] = [{"week": str(week), "value": int(value)} for week, value in rows]
    elif group_by:
        if group_by not in USER_PROPERTIES:
            raise ValueError(f"unknown group_by_property '{group_by}'; known: {USER_PROPERTIES}")
        join = "JOIN users u ON e.user_id = u.user_id "
        base = f"FROM events e {join}WHERE {conditions}"
        rows = cur.execute(
            f"SELECT u.{group_by}, {agg} {base} GROUP BY 1 ORDER BY 2 DESC LIMIT 12", params
        ).fetchall()
        data["rows"] = [{"group": str(g), "value": int(v)} for g, v in rows if g is not None]
    else:
        data["rows"] = [{"group": "all users", "value": total}]

    label = "unique users did" if metric == "unique_users" else "occurrences of"
    headline = f"{total:,} {label} {', '.join(events)} in the last {timeframe} days"
    if by_week and data["rows"]:
        headline += f" across {len(data['rows'])} weeks (latest: {data['rows'][-1]['value']:,})"
    elif group_by and data["rows"]:
        top = data["rows"][0]
        headline += f"; top {group_by}: {top['group']} ({top['value']:,})"
    return {"data": data, "headline": headline}


def _segment_definition(cur: Cursor, args: dict[str, Any]) -> dict[str, Any]:
    name = str(args.get("name") or "").strip()
    if not name:
        raise ValueError("segment_definition requires 'name'")
    filters = args.get("filters") or {}
    event_filters = args.get("event_filters") or []
    anchor = _anchor(cur)

    where, params = _filters_clause(filters, alias="u")
    ctes = [f"c0 AS (SELECT user_id FROM users u{where})"]
    prev = "c0"
    for i, ef in enumerate(event_filters, start=1):
        op = {"at_least": ">=", "at_most": "<="}.get(str(ef.get("op") or "at_least"))
        if op is None:
            raise ValueError(f"event_filter op must be at_least or at_most, got {ef.get('op')!r}")
        cutoff = anchor - timedelta(days=_int(ef.get("timeframe_days"), 90))
        ctes.append(
            f"c{i} AS (SELECT b.user_id FROM {prev} b LEFT JOIN "
            "(SELECT user_id, COUNT(*) AS n FROM events WHERE event = ? AND ts >= ? GROUP BY user_id) x "
            f"ON b.user_id = x.user_id WHERE COALESCE(x.n, 0) {op} ?)"
        )
        params += [str(ef.get("event")), cutoff, _int(ef.get("count"), 1)]
        prev = f"c{i}"

    user_count = _write_segment(cur, name, f"WITH {', '.join(ctes)} SELECT user_id FROM {prev}", params)
    return {
        "data": {"user_count": user_count, "filters": filters, "event_filters": event_filters},
        "segment_name": name,
        "headline": f"Segment '{name}' contains {user_count:,} users",
    }


def _segment_event_discovery(cur: Cursor, args: dict[str, Any]) -> dict[str, Any]:
    name, seg_size = _require_segment(cur, args)
    top_n = _int(args.get("top_n"), 15)
    timeframe = _int(args.get("timeframe_days"), 90)
    pop_size = int(cur.execute("SELECT COUNT(*) FROM users").fetchone()[0])

    rows = cur.execute(
        "WITH seg AS (SELECT DISTINCT user_id FROM segments WHERE segment_name = ?) "
        "SELECT e.event, COUNT(DISTINCT e.user_id) AS pop_users, "
        "COUNT(DISTINCT CASE WHEN s.user_id IS NOT NULL THEN e.user_id END) AS seg_users "
        "FROM events e LEFT JOIN seg s ON e.user_id = s.user_id "
        "WHERE e.ts >= ? GROUP BY e.event",
        [name, _cutoff(cur, timeframe)],
    ).fetchall()

    scored = []
    for event, pop_users, seg_users in rows:
        segment_pct = round(seg_users / seg_size, 4)
        population_pct = round(pop_users / pop_size, 4) if pop_size else 0.0
        lift = round(segment_pct / max(population_pct, POPULATION_PCT_FLOOR), 4)
        sort_key = abs(math.log(lift)) if lift > 0 else abs(math.log(1e-4))
        scored.append(
            (sort_key, {"event": event, "segment_pct": segment_pct, "population_pct": population_pct, "lift": lift})
        )
    scored.sort(key=lambda pair: pair[0], reverse=True)
    out_rows = [row for _, row in scored[:top_n]]

    headline = f"Segment '{name}' ({seg_size:,} users): no distinctive events in the last {timeframe} days"
    if out_rows:
        top = max(out_rows, key=lambda r: r["lift"])
        direction = "over-indexes" if top["lift"] >= 1 else "under-indexes"
        top = top if top["lift"] >= 1 else min(out_rows, key=lambda r: r["lift"])
        headline = (
            f"Segment '{name}' ({seg_size:,} users) {direction} most on {top['event']}: "
            f"{top['segment_pct']:.1%} vs {top['population_pct']:.1%} population ({top['lift']:.1f}x)"
        )
    return {
        "data": {"rows": out_rows, "segment": name, "segment_size": seg_size},
        "event_names": [r["event"] for r in out_rows],
        "headline": headline,
    }


def _profile_segment(cur: Cursor, args: dict[str, Any]) -> dict[str, Any]:
    name, seg_size = _require_segment(cur, args)
    min_relative_change = _float(args.get("min_relative_change"), 0.3)
    pop_size = int(cur.execute("SELECT COUNT(*) FROM users").fetchone()[0])

    deviations = []
    for prop in USER_PROPERTIES:
        rows = cur.execute(
            "WITH seg AS (SELECT DISTINCT user_id FROM segments WHERE segment_name = ?) "
            f"SELECT u.{prop}, COUNT(*) AS pop_n, COUNT(s.user_id) AS seg_n "
            "FROM users u LEFT JOIN seg s ON u.user_id = s.user_id GROUP BY 1",
            [name],
        ).fetchall()
        for value, pop_n, seg_n in rows:
            if value is None:
                continue
            segment_pct = round(seg_n / seg_size, 4)
            population_pct = round(pop_n / pop_size, 4) if pop_size else 0.0
            relative_change = round((segment_pct - population_pct) / max(population_pct, 0.01), 4)
            if abs(relative_change) >= min_relative_change and segment_pct >= 0.02:
                deviations.append(
                    {
                        "property": prop,
                        "value": str(value),
                        "segment_pct": segment_pct,
                        "population_pct": population_pct,
                        "relative_change": relative_change,
                    }
                )
    deviations.sort(key=lambda d: abs(d["relative_change"]), reverse=True)

    headline = f"Segment '{name}' ({seg_size:,} users) profiles close to the overall population"
    if deviations:
        top = deviations[0]
        headline = (
            f"Segment '{name}' ({seg_size:,} users) skews most on {top['property']}={top['value']}: "
            f"{top['segment_pct']:.1%} vs {top['population_pct']:.1%} population "
            f"({top['relative_change']:+.0%} relative)"
        )
    return {"data": {"deviations": deviations, "segment": name, "segment_size": seg_size}, "headline": headline}


# --------------------------------------------------------- paths and journeys


def _compress(seq: list[str]) -> list[str]:
    out: list[str] = []
    for event in seq:
        if not out or out[-1] != event:
            out.append(event)
    return out


def _path_window(seq: list[str], start: str, end: str) -> list[str] | None:
    """Minimal start→end window: the LAST start before the first end.

    Using the first start would keep interior restarts (start appearing twice
    in one window), fragmenting counts across long noisy path strings.
    """
    try:
        j = seq.index(end)
    except ValueError:
        return None
    prefix = seq[:j]
    if start not in prefix:
        # end occurred before any start; look for the first start, then the
        # next end after it.
        try:
            i = seq.index(start)
            j = seq.index(end, i + 1)
        except ValueError:
            return None
        return _path_window(seq[i:], start, end)
    i = len(prefix) - 1 - prefix[::-1].index(start)
    return seq[i : j + 1]


def _user_sequences(
    cur: Cursor,
    timeframe_days: int,
    allowed_events: list[str] | None,
    segment: str | None,
) -> list[list[str]]:
    """Per-user ordered event sequences (internal only — never returned to callers)."""
    params: list[Any] = []
    join = ""
    if segment:
        join = "JOIN (SELECT DISTINCT user_id FROM segments WHERE segment_name = ?) s ON e.user_id = s.user_id "
        params.append(segment)
    conditions = ["e.ts >= ?"]
    params.append(_cutoff(cur, timeframe_days))
    if allowed_events:
        conditions.append(f"e.event IN ({', '.join('?' for _ in allowed_events)})")
        params.extend(allowed_events)
    rows = cur.execute(
        f"SELECT e.user_id, LIST(e.event ORDER BY e.ts) FROM events e {join}"
        f"WHERE {' AND '.join(conditions)} GROUP BY e.user_id",
        params,
    ).fetchall()
    return [list(seq) for _, seq in rows]


def _product_paths(cur: Cursor, args: dict[str, Any]) -> dict[str, Any]:
    start = str(args.get("start_event") or "").strip()
    end = str(args.get("end_event") or "").strip()
    if not start or not end:
        raise ValueError("product_paths requires 'start_event' and 'end_event'")
    top_n = _int(args.get("top_n"), 10)
    timeframe = _int(args.get("timeframe_days"), 90)
    allowed = args.get("allowed_events")
    if isinstance(allowed, str):
        allowed = [v.strip() for v in allowed.split(",") if v.strip()]
    if allowed:
        allowed = sorted({*[str(a) for a in allowed], start, end})

    counter: Counter[str] = Counter()
    completed = 0
    for seq in _user_sequences(cur, timeframe, allowed, None):
        window = _path_window(_compress(seq), start, end)
        if window is None:
            continue  # only users who actually reached end_event count
        completed += 1
        counter[" -> ".join(window)] += 1

    paths = [{"path": path, "users": users} for path, users in counter.most_common(top_n)]
    headline = f"No users reached {end} from {start} in the last {timeframe} days"
    if paths:
        headline = (
            f"{completed:,} users reached {end} from {start}; "
            f"top path ({paths[0]['users']:,} users): {paths[0]['path']}"
        )
    return {"data": {"paths": paths, "completed_users": completed}, "headline": headline}


def _journey_map(cur: Cursor, args: dict[str, Any]) -> dict[str, Any]:
    start = str(args.get("start_event") or "").strip()
    end = str(args.get("end_event") or "").strip()
    if not start or not end:
        raise ValueError("journey_map requires 'start_event' and 'end_event'")
    timeframe = _int(args.get("timeframe_days"), 90)
    segment = args.get("segment") or None
    if segment is not None:
        segment, _ = _require_segment(cur, {"segment": segment})

    sequences = []
    for seq in _user_sequences(cur, timeframe, None, segment):
        comp = _compress(seq)
        if start in comp:
            window = _path_window(comp, start, end)
            sequences.append(window if window is not None else comp[comp.index(start) :])

    # Canonical paths: the top 5 completed start→end windows.
    counter = Counter(" -> ".join(s) for s in sequences if s and s[-1] == end and s[0] == start and end in s)
    canonical = [(path.split(" -> "), users) for path, users in counter.most_common(5)]
    data_paths = [{"path": " -> ".join(p), "users": u} for p, u in canonical]

    if not canonical or not sequences:
        return {
            "data": {"nodes": [], "paths": data_paths},
            "headline": f"No users completed {start} → {end} in the last {timeframe} days",
        }

    # Simplification (documented): each user is aligned to the canonical path
    # they match furthest, via a greedy in-order prefix match — longest match
    # wins; ties go to the more-traveled path. No global optimal alignment.
    def prefix_match(tail: list[str], path: list[str]) -> int:
        pos = matched = 0
        for step_event in path:
            try:
                pos = tail.index(step_event, pos) + 1
            except ValueError:
                break
            matched += 1
        return matched

    node_users: Counter[tuple[int, str]] = Counter()
    for tail in sequences:
        matched, best = max(
            ((prefix_match(tail, path), path) for path, users in canonical),
            key=lambda pair: (pair[0], next(u for p, u in canonical if p == pair[1])),
        )
        for i in range(matched):
            node_users[(i, best[i])] += 1

    start_total = len(sequences)
    nodes = [
        {"step": step, "event": event, "users": users, "pct_of_start": round(users / start_total, 4)}
        for (step, event), users in sorted(node_users.items(), key=lambda kv: (kv[0][0], -kv[1]))
    ]

    completed_users = sum(1 for s in sequences if s[0] == start and s[-1] == end)
    step_totals: dict[int, int] = {}
    for (step, _), users in node_users.items():
        step_totals[step] = step_totals.get(step, 0) + users
    totals = [step_totals[i] for i in sorted(step_totals)]
    headline = f"Of {start_total:,} users who did {start}, {completed_users / start_total:.1%} reached {end}"
    if len(totals) > 1:
        losses = [(i, totals[i] - totals[i + 1]) for i in range(len(totals) - 1)]
        worst, lost = max(losses, key=lambda pair: pair[1])
        worst_events = [e for (s, e), _ in node_users.items() if s == worst]
        headline += f"; steepest loss after step {worst + 1} ({'/'.join(sorted(set(worst_events)))}, -{lost:,} users)"
    return {"data": {"nodes": nodes, "paths": data_paths}, "headline": headline}


# ------------------------------------------------------------ breakdown miner


def _funnel_breakdown(cur: Cursor, args: dict[str, Any]) -> dict[str, Any]:
    steps = _require_events(args, "steps", minimum=2)
    timeframe = _int(args.get("timeframe_days"), 90)
    window = _int(args.get("conversion_window_days"), 14)
    filters = args.get("filters") or {}
    max_values = _int(args.get("max_values_per_property"), 8)

    _create_reach_table(cur, steps, timeframe, window, filters)
    users_at = _users_per_step(_reach_counts(cur), len(steps))
    base_users = users_at[0]
    base_conversion = round(users_at[-1] / base_users, 4) if base_users else 0.0
    if not base_users:
        return {
            "data": {"base_conversion": 0.0, "base_users": 0, "boosters": [], "blockers": []},
            "headline": f"No users performed {steps[0]} in the last {timeframe} days.",
        }

    threshold = max(50, math.ceil(0.01 * base_users))
    entries = []
    for prop in USER_PROPERTIES:
        if prop in filters:
            continue  # already conditioned on this property
        per_value = {
            value: _users_per_step(counts, len(steps))
            for value, counts in _reach_counts_by_property(cur, prop).items()
        }
        candidates = sorted(per_value, key=lambda v: per_value[v][0], reverse=True)[:max_values]
        for value in candidates:
            v_users = per_value[value]
            if v_users[0] < threshold:
                continue
            conversion = round(v_users[-1] / v_users[0], 4)
            lift = round(conversion - base_conversion, 4)
            entries.append(
                {
                    "property": prop,
                    "value": value,
                    "conversion": conversion,
                    "lift": lift,
                    "affected_users": v_users[0],
                    "impact": round(v_users[0] * abs(lift), 1),
                }
            )

    boosters = sorted((e for e in entries if e["lift"] > 0), key=lambda e: e["impact"], reverse=True)[:5]
    blockers = sorted((e for e in entries if e["lift"] < 0), key=lambda e: e["impact"], reverse=True)[:5]
    headline = f"Base conversion {base_conversion:.1%} across {base_users:,} users"
    if blockers:
        b = blockers[0]
        headline += (
            f"; top blocker: {b['property']}={b['value']} "
            f"({b['lift']:+.1%} vs base, {b['affected_users']:,} users affected)"
        )
    elif boosters:
        b = boosters[0]
        headline += f"; top booster: {b['property']}={b['value']} ({b['lift']:+.1%} vs base)"
    return {
        "data": {
            "base_conversion": base_conversion,
            "base_users": base_users,
            "boosters": boosters,
            "blockers": blockers,
        },
        "headline": headline,
    }


# ------------------------------------------------------------------ dispatch


_HANDLERS = {
    "funnel_analysis": _funnel_analysis,
    "insight_query": _insight_query,
    "segment_definition": _segment_definition,
    "segment_event_discovery": _segment_event_discovery,
    "profile_segment": _profile_segment,
    "product_paths": _product_paths,
    "journey_map": _journey_map,
    "funnel_breakdown": _funnel_breakdown,
}


def execute_tool(con: duckdb.DuckDBPyConnection, tool: str, args: dict[str, Any] | None) -> dict[str, Any]:
    """Execute one catalog tool on a fresh cursor and return its result dict."""
    handler = _HANDLERS.get(tool)
    if handler is None:
        raise ValueError(f"unknown tool '{tool}'; available: {sorted(_HANDLERS)}")
    cur = con.cursor()
    try:
        payload = handler(cur, dict(args or {}))
    finally:
        cur.close()
    return {"status": "success", "tool": tool, **payload}
