"""GTFS time helpers.

GTFS clock times are *service-day* times, not wall-clock times: "25:10:00" is 1:10 AM on
the calendar day after the service date. They are also local times in the agency's
timezone, and the agency's day may be 23 or 25 hours long across a DST boundary. So a
GTFS time only becomes an instant once you pair it with a service date and a timezone.
"""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def parse_gtfs_time(value: str | None) -> int | None:
    """ "HH:MM:SS" -> seconds after midnight of the service date. Hours may exceed 23."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    parts = value.split(":")
    if len(parts) != 3:
        raise ValueError(f"not a GTFS time: {value!r}")
    hours, minutes, seconds = (int(p) for p in parts)
    return hours * 3600 + minutes * 60 + seconds


def format_gtfs_time(seconds: int) -> str:
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def service_time_to_instant(service_date: date, seconds: int, timezone: str) -> datetime:
    """Combine a service date and a GTFS time into a timezone-aware instant.

    Midnight of the service date is resolved in local time first, then the offset is added,
    which is what keeps 25:10:00 landing on the next calendar day.
    """
    tz = ZoneInfo(timezone)
    local_midnight = datetime(service_date.year, service_date.month, service_date.day, tzinfo=tz)
    return local_midnight + timedelta(seconds=seconds)


def parse_gtfs_date(value: str) -> date:
    """ "20260816" -> date(2026, 8, 16)."""
    return datetime.strptime(value.strip(), "%Y%m%d").date()
