from datetime import date, timedelta

import pytest

from transit.matching import TripMatcher, active_service_ids, resolve_service_date
from transit.models import Service, ServiceException

MONDAY = date(2025, 3, 10)
SUNDAY = date(2025, 3, 9)


@pytest.fixture
def weekday_service(session):
    """A weekday-only service on its own agency, so calendar logic can be tested on fixed
    dates without depending on when the suite runs."""
    session.add(
        Service(
            agency_key="cal",
            service_id="WEEKDAY",
            monday=True,
            tuesday=True,
            wednesday=True,
            thursday=True,
            friday=True,
            saturday=False,
            sunday=False,
            start_date=MONDAY - timedelta(days=30),
            end_date=MONDAY + timedelta(days=30),
        )
    )
    session.flush()


def test_weekday_service_is_active_on_a_monday(session, weekday_service):
    assert active_service_ids(session, "cal", MONDAY) == {"WEEKDAY"}


def test_weekday_service_is_not_active_on_a_sunday(session, weekday_service):
    assert active_service_ids(session, "cal", SUNDAY) == set()


def test_weekday_service_is_not_active_outside_its_date_range(session, weekday_service):
    assert active_service_ids(session, "cal", MONDAY + timedelta(days=90)) == set()


def test_calendar_dates_removes_service_on_a_holiday(session, weekday_service):
    session.add(
        ServiceException(
            agency_key="cal", service_id="WEEKDAY", exception_date=MONDAY, exception_type=2
        )
    )
    session.flush()
    assert active_service_ids(session, "cal", MONDAY) == set()


def test_calendar_dates_adds_service_on_a_sunday(session, weekday_service):
    session.add(
        ServiceException(
            agency_key="cal", service_id="WEEKDAY", exception_date=SUNDAY, exception_type=1
        )
    )
    session.flush()
    assert active_service_ids(session, "cal", SUNDAY) == {"WEEKDAY"}


def test_matches_on_route_short_name_and_start_time(session, schedule, service_date):
    matcher = TripMatcher(session, "test", service_date)
    matched = matcher.match("R12", "16:00:00")
    assert matched is not None
    assert (matched.scheduled_trip_id, matched.route_id) == ("T1", "4108")


def test_no_match_when_the_start_time_is_unknown(session, schedule, service_date):
    matcher = TripMatcher(session, "test", service_date)
    assert matcher.match("R12", "16:07:00") is None
    assert matcher.match("R12", None) is None
    assert matcher.match(None, "16:00:00") is None


def test_no_match_when_the_route_short_name_is_unknown(session, schedule, service_date):
    matcher = TripMatcher(session, "test", service_date)
    assert matcher.match("R99", "16:00:00") is None


def test_stop_ids_resolve_through_the_matched_trip(session, schedule, service_date):
    """The realtime stop id is ignored; (trip, stop_sequence) names the static stop."""
    matcher = TripMatcher(session, "test", service_date)
    matched = matcher.match("R12", "16:00:00")
    matcher.load_stop_sequences([matched.scheduled_trip_id])
    assert matcher.static_stop_id("T1", 3) == "S3"
    assert matcher.static_stop_id("T1", 99) is None
    assert matcher.static_stop_id(None, 3) is None
    assert matcher.static_stop_id("T1", None) is None


def test_stop_sequences_are_not_loaded_for_unmatched_trips(session, schedule, service_date):
    matcher = TripMatcher(session, "test", service_date)
    assert matcher.static_stop_id("T1", 1) is None  # nothing loaded yet


def test_resolve_service_date_prefers_the_feeds_start_date():
    assert resolve_service_date("20250309", "America/Los_Angeles") == date(2025, 3, 9)


def test_resolve_service_date_falls_back_to_today_on_garbage():
    assert resolve_service_date("not-a-date", "America/Los_Angeles") is not None
