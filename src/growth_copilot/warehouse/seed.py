"""Deterministic synthetic warehouse for "Relay", a fictional B2B team
file-sharing SaaS.

The journey is deliberately non-linear. Users reach value through two
independent routes that interleave freely — a solo route (create a
workspace, upload files, share links) and a team route (invite a teammate,
they join, collaboration starts) — and "activated" is a derived milestone
(first `link_shared` OR first `comment_added`), not a gate event. Upgrades
are usage-driven: hitting the storage limit is the trigger, amplified by
collaboration behaviors, never the tail of a fixed step sequence.

Every behavioral effect the analytics tools are expected to find is planted
explicitly and documented in PLANTED_EFFECTS — the generator reads its
probabilities from that table, so tests assert against the same numbers the
data was built from. `seed()` DROPs and recreates the three tables: the
warehouse is a fresh local demo file, never a store of record.
"""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from growth_copilot.warehouse.db import get_connection

# Canonical event taxonomy — the only events that ever appear in the warehouse.
EVENTS = [
    "account_created",
    "workspace_created",
    "file_uploaded",
    "link_shared",
    "teammate_invited",
    "teammate_joined",
    "comment_added",
    "integration_connected",
    "storage_limit_reached",
    "plan_upgrade_viewed",
    "plan_upgraded",
    "ticket_opened",
]

COUNTRIES = ["US", "UK", "DE", "IN", "BR", "JP"]
COUNTRY_WEIGHTS = [0.34, 0.14, 0.12, 0.18, 0.12, 0.10]
DEVICES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.55, 0.35, 0.10]
CHANNELS = ["organic_search", "paid_ads", "referral", "product_hunt", "outbound"]
CHANNEL_WEIGHTS = [0.30, 0.25, 0.18, 0.15, 0.12]
COMPANY_SIZES = ["1-10", "11-50", "51-200", "200+"]
COMPANY_SIZE_WEIGHTS = [0.40, 0.30, 0.20, 0.10]
# `plan` is the SIGNUP-time plan, fixed at account creation. It must never be
# derived from the upgrade outcome: a property that encodes the analysis's end
# state would show up as a fake "blocker" in every upgrade analysis (target
# leakage). Trial signups upgrading more is the legitimate planted signal.
PLANS = ["free", "trial"]
PLAN_WEIGHTS = [0.85, 0.15]

# The causal structure of the dataset — single source of truth for what an
# analysis SHOULD find. The generator reads every probability from here.
PLANTED_EFFECTS: dict[str, dict[str, float]] = {
    # P(file_uploaded | workspace_created) by device.
    # mobile uploads at HALF the desktop rate — the flagship blocker that
    # funnel_breakdown and the bottleneck drill-down must surface.
    "upload_rate_by_device": {"desktop": 0.78, "mobile": 0.39, "tablet": 0.64},
    # Channel multiplier on that upload probability (solo activation):
    # paid_ads activates materially worse than referral/organic.
    "upload_channel_multiplier": {
        "organic_search": 1.06,
        "paid_ads": 0.82,
        "referral": 1.10,
        "product_hunt": 1.00,
        "outbound": 0.95,
    },
    # P(teammate_joined | teammate_invited) by company size — invites into
    # bigger organizations get accepted more.
    "invite_acceptance_by_company_size": {"1-10": 0.45, "11-50": 0.58, "51-200": 0.66, "200+": 0.72},
    # P(plan_upgraded) base rate by whether the user hit the storage limit.
    # Hitting the limit is THE upgrade trigger — usage-driven monetization.
    "upgrade_base_rate": {"limit_reached": 0.34, "no_limit": 0.015},
    # Channel multiplier on upgrade probability: paid_ads converts to paid
    # far worse than referral/organic.
    "upgrade_channel_multiplier": {
        "organic_search": 1.20,
        "paid_ads": 0.45,
        "referral": 1.40,
        "product_hunt": 1.00,
        "outbound": 0.90,
    },
    # Behavioral multipliers on upgrade probability: users whose teammate
    # actually joined upgrade ~2.5x more; users who connected an integration
    # ~2x more. These are CONDITIONAL multipliers (all else equal). The raw
    # marginal ratio in the data is larger, because both behaviors correlate
    # with heavy usage — tests should assert the multiplier, or expect
    # observed lift >= it.
    "upgrade_behavior_multiplier": {"teammate_joined": 2.5, "integration_connected": 2.0},
    # Signup-plan multiplier on upgrade probability: trial signups convert to
    # paid ~1.8x more than free signups.
    "upgrade_plan_multiplier": {"free": 1.0, "trial": 1.8},
}

WORKSPACE_RATE = 0.72
SHARE_RATE = 0.58
# Team-route entry propensity scales with company size (independent of the
# solo route — a user can take either route, both, or neither).
INVITE_RATE_BY_COMPANY_SIZE = {"1-10": 0.22, "11-50": 0.32, "51-200": 0.40, "200+": 0.48}
COMMENT_RATE = 0.62
INTEGRATION_RATE_ENGAGED = 0.22
INTEGRATION_RATE_IDLE = 0.05
# P(storage_limit_reached) grows with upload volume, capped.
LIMIT_PER_UPLOAD = 0.08
LIMIT_CAP = 0.85
TICKET_RATE = 0.06
UPGRADE_VIEW_AFTER_LIMIT_RATE = 0.35
UPGRADE_VIEW_IDLE_RATE = 0.06
MAX_UPGRADE_PROBABILITY = 0.85


def _later(rng: random.Random, ts: datetime, end: datetime, min_h: float, max_h: float) -> datetime | None:
    """Advance `ts` by a random gap of hours; None when it would fall past the
    window end (right-censoring — recent signups genuinely haven't converted yet)."""
    nxt = ts + timedelta(hours=rng.uniform(min_h, max_h))
    return nxt if nxt <= end else None


def seed(db_path: Path | str, users: int = 20000, days: int = 120, seed: int = 42) -> dict[str, Any]:
    """Generate the Relay demo warehouse. Deterministic for a given
    (users, days, seed) on a given calendar day — the event window is
    anchored to today so freshly seeded data reads as "recent"; analytics
    anchors its timeframes to the newest event, so results don't drift."""
    rng = random.Random(seed)
    end = datetime.combine(date.today(), time(12, 0))
    start = end - timedelta(days=days)

    user_rows: list[tuple] = []
    event_rows: list[tuple[str, str, datetime]] = []
    upgraded_users = 0

    upload_by_device = PLANTED_EFFECTS["upload_rate_by_device"]
    upload_channel = PLANTED_EFFECTS["upload_channel_multiplier"]
    invite_acceptance = PLANTED_EFFECTS["invite_acceptance_by_company_size"]
    upgrade_base = PLANTED_EFFECTS["upgrade_base_rate"]
    upgrade_channel = PLANTED_EFFECTS["upgrade_channel_multiplier"]
    behavior_mult = PLANTED_EFFECTS["upgrade_behavior_multiplier"]
    plan_mult = PLANTED_EFFECTS["upgrade_plan_multiplier"]

    for i in range(users):
        uid = f"u{i:06d}"
        country = rng.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
        device = rng.choices(DEVICES, weights=DEVICE_WEIGHTS)[0]
        channel = rng.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]
        company_size = rng.choices(COMPANY_SIZES, weights=COMPANY_SIZE_WEIGHTS)[0]
        plan = rng.choices(PLANS, weights=PLAN_WEIGHTS)[0]

        signup_ts = start + timedelta(minutes=rng.randrange(days * 24 * 60))
        event_rows.append((uid, "account_created", signup_ts))

        # --- solo route: workspace → upload → share (each step optional) ---
        workspace_ts = upload_ts = share_ts = None
        if rng.random() < WORKSPACE_RATE:
            workspace_ts = _later(rng, signup_ts, end, 1, 72)
        if workspace_ts is not None:
            event_rows.append((uid, "workspace_created", workspace_ts))
            p_upload = min(upload_by_device[device] * upload_channel[channel], 0.95)
            if rng.random() < p_upload:
                upload_ts = _later(rng, workspace_ts, end, 1, 96)
                if upload_ts is not None:
                    event_rows.append((uid, "file_uploaded", upload_ts))
            if upload_ts is not None and rng.random() < SHARE_RATE:
                share_ts = _later(rng, upload_ts, end, 0.5, 48)
                if share_ts is not None:
                    event_rows.append((uid, "link_shared", share_ts))

        # --- team route: invite → join → comment. Anchored to signup, NOT to
        # the solo route — the two routes interleave freely in time. ---
        invite_ts = join_ts = comment_ts = None
        if rng.random() < INVITE_RATE_BY_COMPANY_SIZE[company_size]:
            invite_ts = _later(rng, signup_ts, end, 2, 24 * 8)
        if invite_ts is not None:
            event_rows.append((uid, "teammate_invited", invite_ts))
            if rng.random() < invite_acceptance[company_size]:
                join_ts = _later(rng, invite_ts, end, 1, 24 * 5)
                if join_ts is not None:
                    event_rows.append((uid, "teammate_joined", join_ts))
            if join_ts is not None and rng.random() < COMMENT_RATE:
                comment_ts = _later(rng, join_ts, end, 0.5, 24 * 4)
                if comment_ts is not None:
                    event_rows.append((uid, "comment_added", comment_ts))

        engaged_anchor = max((t for t in (upload_ts, join_ts) if t), default=None)
        integration_ts = None
        p_integration = INTEGRATION_RATE_ENGAGED if engaged_anchor else INTEGRATION_RATE_IDLE
        if rng.random() < p_integration:
            integration_ts = _later(rng, engaged_anchor or signup_ts, end, 8, 24 * 10)
            if integration_ts is not None:
                event_rows.append((uid, "integration_connected", integration_ts))

        # --- organic repeat usage BEFORE any upgrade decision, so the causal
        # chain stays clean: usage → limit → upgrade (→ more usage below) ---
        n_uploads = 1 if upload_ts is not None else 0
        if upload_ts is not None:
            horizon = (end - upload_ts).total_seconds()
            for _ in range(rng.randint(0, 7)):
                event_rows.append((uid, "file_uploaded", upload_ts + timedelta(seconds=rng.random() * horizon)))
                n_uploads += 1
        if share_ts is not None:
            horizon = (end - share_ts).total_seconds()
            for _ in range(rng.randint(0, 3)):
                event_rows.append((uid, "link_shared", share_ts + timedelta(seconds=rng.random() * horizon)))
        if comment_ts is not None:
            horizon = (end - comment_ts).total_seconds()
            for _ in range(rng.randint(0, 4)):
                event_rows.append((uid, "comment_added", comment_ts + timedelta(seconds=rng.random() * horizon)))

        # --- storage limit: a function of upload volume, nothing else ---
        limit_ts = None
        if n_uploads and rng.random() < min(LIMIT_CAP, LIMIT_PER_UPLOAD * n_uploads):
            limit_ts = _later(rng, upload_ts, end, 12, 24 * 14)
            if limit_ts is not None:
                event_rows.append((uid, "storage_limit_reached", limit_ts))

        # --- upgrade decision: limit trigger x channel x planted behavior lifts ---
        p_upgrade = upgrade_base["limit_reached" if limit_ts is not None else "no_limit"]
        p_upgrade *= upgrade_channel[channel]
        p_upgrade *= plan_mult[plan]
        if join_ts is not None:
            p_upgrade *= behavior_mult["teammate_joined"]
        if integration_ts is not None:
            p_upgrade *= behavior_mult["integration_connected"]
        p_upgrade = min(p_upgrade, MAX_UPGRADE_PROBABILITY)

        upgraded = False
        if rng.random() < p_upgrade:
            view_anchor = max(t for t in (limit_ts, engaged_anchor, signup_ts) if t)
            view_ts = _later(rng, view_anchor, end, 2, 24 * 6)
            up_ts = _later(rng, view_ts, end, 2, 72) if view_ts else None
            if view_ts and up_ts:
                event_rows.append((uid, "plan_upgrade_viewed", view_ts))
                event_rows.append((uid, "plan_upgraded", up_ts))
                upgraded = True
                upgraded_users += 1
        else:
            p_view = UPGRADE_VIEW_AFTER_LIMIT_RATE if limit_ts is not None else (
                UPGRADE_VIEW_IDLE_RATE if engaged_anchor else 0.0
            )
            if rng.random() < p_view:
                view_ts = _later(rng, limit_ts or engaged_anchor, end, 6, 24 * 14)
                if view_ts is not None:
                    event_rows.append((uid, "plan_upgrade_viewed", view_ts))

        # --- upgraders keep using the product harder after converting ---
        if upgraded and upload_ts is not None:
            horizon = (end - upload_ts).total_seconds()
            for _ in range(rng.randint(2, 10)):
                event_rows.append((uid, "file_uploaded", upload_ts + timedelta(seconds=rng.random() * horizon)))
            for _ in range(rng.randint(1, 6)):
                event_rows.append((uid, "link_shared", upload_ts + timedelta(seconds=rng.random() * horizon)))

        # --- a small fraction opens support tickets ---
        if rng.random() < TICKET_RATE:
            horizon = (end - signup_ts).total_seconds()
            for _ in range(rng.randint(1, 2)):
                event_rows.append((uid, "ticket_opened", signup_ts + timedelta(seconds=rng.random() * horizon)))

        user_rows.append((uid, signup_ts, country, device, channel, plan, company_size))

    event_rows.sort(key=lambda row: row[2])

    con = get_connection(db_path)
    cur = con.cursor()
    try:
        users_df = pd.DataFrame(
            user_rows,
            columns=["user_id", "signup_ts", "country", "device", "channel", "plan", "company_size"],
        )
        events_df = pd.DataFrame(event_rows, columns=["user_id", "event", "ts"])
        cur.execute("DROP TABLE IF EXISTS events")
        cur.execute("DROP TABLE IF EXISTS segments")
        cur.execute("DROP TABLE IF EXISTS users")
        cur.execute(
            "CREATE TABLE users (user_id VARCHAR PRIMARY KEY, signup_date DATE, country VARCHAR, "
            "device VARCHAR, channel VARCHAR, plan VARCHAR, company_size VARCHAR)"
        )
        cur.execute("CREATE TABLE events (user_id VARCHAR, event VARCHAR, ts TIMESTAMP)")
        cur.execute("CREATE TABLE segments (segment_name VARCHAR, user_id VARCHAR)")
        cur.register("users_src", users_df)
        cur.register("events_src", events_df)
        cur.execute(
            "INSERT INTO users SELECT user_id, CAST(signup_ts AS DATE), country, device, channel, "
            "plan, company_size FROM users_src"
        )
        cur.execute("INSERT INTO events SELECT user_id, event, CAST(ts AS TIMESTAMP) FROM events_src")
        cur.unregister("users_src")
        cur.unregister("events_src")
    finally:
        cur.close()

    return {
        "users": users,
        "events": len(event_rows),
        "upgraded_users": upgraded_users,
        "date_range": [start.date().isoformat(), end.date().isoformat()],
    }
