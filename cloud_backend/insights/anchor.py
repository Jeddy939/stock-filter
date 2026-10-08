"""Point-in-time anchor shared by appraisal features and outcomes.

Features are calculated through, and outcomes are measured from, the same
market session: the last session that had closed when the appraisal was made.
The SQL function ``appraisal_cutoff_date`` (migration 013) is the production
implementation; this module mirrors it for tests and Python callers.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


ANCHOR_POLICY = "last_completed_session_at_appraisal"
SESSION_COMPLETE_LOCAL_TIME = time(16, 30)
MARKET_TIMEZONES = {
    "asx": ZoneInfo("Australia/Sydney"),
    "us": ZoneInfo("America/New_York"),
}


def session_complete_utc(market: str, session_date: date) -> datetime:
    """The moment ``session_date``'s session counts as complete, in UTC."""
    zone = MARKET_TIMEZONES.get(str(market).strip().lower(), MARKET_TIMEZONES["us"])
    return datetime.combine(session_date, SESSION_COMPLETE_LOCAL_TIME, tzinfo=zone).astimezone(timezone.utc)


def appraisal_cutoff_date(market: str, event_at_utc: datetime) -> date:
    """Latest calendar date whose session had closed at ``event_at_utc``."""
    if event_at_utc.tzinfo is None:
        event_at_utc = event_at_utc.replace(tzinfo=timezone.utc)
    zone = MARKET_TIMEZONES.get(str(market).strip().lower(), MARKET_TIMEZONES["us"])
    local = event_at_utc.astimezone(zone)
    if local.time() >= SESSION_COMPLETE_LOCAL_TIME:
        return local.date()
    return local.date() - timedelta(days=1)
