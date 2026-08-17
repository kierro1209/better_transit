from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from transit.timeutil import (
    format_gtfs_time,
    parse_gtfs_date,
    parse_gtfs_time,
    service_time_to_instant,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("00:00:00", 0),
        ("07:05:30", 7 * 3600 + 5 * 60 + 30),
        ("7:05:30", 7 * 3600 + 5 * 60 + 30),  # GTFS allows an unpadded hour
        ("24:00:00", 86400),
        ("25:10:00", 25 * 3600 + 600),  # 1:10 AM of the *next* calendar day
        ("", None),
        (None, None),
    ],
)
def test_parse_gtfs_time(value, expected):
    assert parse_gtfs_time(value) == expected


def test_parse_gtfs_time_rejects_garbage():
    with pytest.raises(ValueError):
        parse_gtfs_time("tomorrow")


def test_format_round_trips_past_midnight():
    assert format_gtfs_time(parse_gtfs_time("25:10:00")) == "25:10:00"


def test_service_time_after_midnight_belongs_to_the_previous_service_day():
    """25:10 on Monday is 1:10 AM Tuesday, but it is still Monday's service."""
    instant = service_time_to_instant(date(2025, 3, 3), 25 * 3600 + 600, "America/Los_Angeles")
    local = instant.astimezone(ZoneInfo("America/Los_Angeles"))
    assert (local.date(), local.hour, local.minute) == (date(2025, 3, 4), 1, 10)


def test_service_time_crossing_dst_stays_on_the_wall_clock():
    """2025-03-09 is a 23-hour day in Los Angeles; 10:00 service time is still 10:00 local."""
    instant = service_time_to_instant(date(2025, 3, 9), 10 * 3600, "America/Los_Angeles")
    local = instant.astimezone(ZoneInfo("America/Los_Angeles"))
    assert (local.hour, local.minute) == (10, 0)
    assert instant == datetime(2025, 3, 9, 17, 0, tzinfo=ZoneInfo("UTC"))


def test_parse_gtfs_date():
    assert parse_gtfs_date("20250309") == date(2025, 3, 9)
