"""Join realtime entities back to the static schedule.

Big Blue Bus's realtime feed does not use the static feed's identifiers:

* ``trip.route_id`` in the feed is the static ``route_short_name`` ("R12"), not the static
  ``route_id`` ("4108").
* ``trip.trip_id`` in the feed ("1430010") does not appear in trips.txt at all — it comes
  from the AVL system, not from the published schedule.
* ``trip.direction_id`` is consistently the opposite of the static ``direction_id``, so it
  must not be used as a join key.
* ``stop_time_update.stop_id`` is a third id space: only ~55% of the stop ids in a poll
  exist in stops.txt, and those that do name the wrong place.

What *is* reliable is the trip's scheduled start: (route short name, service date, start
time) identifies exactly one static trip in this feed. So that is the join key, and every
failure to match is counted rather than hidden.

Stops are then resolved *through* the matched trip: stop_sequence is consistent across the
two feeds, so (scheduled trip, stop_sequence) -> stops.txt id. Measured over one poll that
mapping is a bijection - 660 realtime stop ids onto 660 distinct static stops, no
collisions - which is the evidence that the sequence numbers really do line up.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from transit.models import Route, Service, ServiceException, StopTime, Trip
from transit.timeutil import parse_gtfs_date, parse_gtfs_time

_WEEKDAY_COLUMNS = (
    Service.monday,
    Service.tuesday,
    Service.wednesday,
    Service.thursday,
    Service.friday,
    Service.saturday,
    Service.sunday,
)


def service_date_now(timezone: str) -> date:
    """The agency's current service date.

    Between midnight and ~3 AM this is arguably still the previous service date (GTFS
    models late-night trips as 24:xx of the day before). We use the calendar date and let
    the start-time join handle the wrap-around, because BBB has no trips past 01:30.
    """
    return datetime.now(ZoneInfo(timezone)).date()


def active_service_ids(session: Session, agency_key: str, on: date) -> set[str]:
    """calendar.txt weekday rules, then calendar_dates.txt overrides (2 removes, 1 adds)."""
    weekday_column = _WEEKDAY_COLUMNS[on.weekday()]
    base = set(
        session.scalars(
            select(Service.service_id).where(
                Service.agency_key == agency_key,
                Service.start_date <= on,
                Service.end_date >= on,
                weekday_column.is_(True),
            )
        )
    )
    exceptions = session.execute(
        select(ServiceException.service_id, ServiceException.exception_type).where(
            ServiceException.agency_key == agency_key,
            ServiceException.exception_date == on,
        )
    ).all()
    for service_id, exception_type in exceptions:
        if exception_type == 1:
            base.add(service_id)
        elif exception_type == 2:
            base.discard(service_id)
    return base


@dataclass
class MatchedTrip:
    scheduled_trip_id: str
    route_id: str
    route_short_name: str | None
    direction_id: int | None
    headsign: str | None
    start_seconds: int


class TripMatcher:
    """Index of (route short name, start seconds) -> static trip for one service date.

    Built once per ingestion cycle (~1k rows) instead of issuing one query per realtime
    trip. That is the whole optimization: 1 query per poll rather than ~100.
    """

    def __init__(self, session: Session, agency_key: str, service_date: date):
        self.agency_key = agency_key
        self.service_date = service_date
        self._session = session
        self._index: dict[tuple[str, int], list[MatchedTrip]] = {}
        self._stops: dict[tuple[str, int], str] = {}
        self._stops_loaded: set[str] = set()

        service_ids = active_service_ids(session, agency_key, service_date)
        if not service_ids:
            return
        rows = session.execute(
            select(
                Route.short_name,
                Trip.trip_id,
                Trip.route_id,
                Trip.direction_id,
                Trip.headsign,
                Trip.start_seconds,
            )
            .join(
                Route,
                (Route.agency_key == Trip.agency_key) & (Route.route_id == Trip.route_id),
            )
            .where(
                Trip.agency_key == agency_key,
                Trip.service_id.in_(service_ids),
                Trip.start_seconds.isnot(None),
            )
        ).all()
        for short_name, trip_id, route_id, direction_id, headsign, start_seconds in rows:
            if short_name is None:
                continue
            self._index.setdefault((short_name, start_seconds), []).append(
                MatchedTrip(
                    scheduled_trip_id=trip_id,
                    route_id=route_id,
                    route_short_name=short_name,
                    direction_id=direction_id,
                    headsign=headsign,
                    start_seconds=start_seconds,
                )
            )

    def match(
        self,
        rt_route_id: str | None,
        start_time: str | None,
        rt_direction_id: int | None = None,
    ) -> MatchedTrip | None:
        if not rt_route_id or not start_time:
            return None
        try:
            start_seconds = parse_gtfs_time(start_time)
        except ValueError:
            return None
        if start_seconds is None:
            return None
        candidates = self._index.get((rt_route_id, start_seconds), [])
        if len(candidates) == 1:
            return candidates[0]
        if not candidates:
            return None
        # Ambiguous start time: fall back to direction, remembering the feed inverts it.
        if rt_direction_id is not None:
            flipped = [c for c in candidates if c.direction_id == 1 - rt_direction_id]
            if len(flipped) == 1:
                return flipped[0]
        return None

    def load_stop_sequences(self, trip_ids: Iterable[str]) -> None:
        """Cache (trip_id, stop_sequence) -> static stop_id for the given scheduled trips.

        One query per ingestion cycle covering every matched trip, rather than one query per
        trip: ~100 trips x ~40 stops is a few thousand rows, cheap to hold for the cycle and
        far cheaper than 100 round trips.
        """
        missing = {t for t in trip_ids if t and t not in self._stops_loaded}
        if not missing:
            return
        rows = self._session.execute(
            select(StopTime.trip_id, StopTime.stop_sequence, StopTime.stop_id).where(
                StopTime.agency_key == self.agency_key,
                StopTime.trip_id.in_(missing),
            )
        ).all()
        for trip_id, stop_sequence, stop_id in rows:
            self._stops[(trip_id, stop_sequence)] = stop_id
        self._stops_loaded |= missing

    def static_stop_id(
        self, scheduled_trip_id: str | None, stop_sequence: int | None
    ) -> str | None:
        if scheduled_trip_id is None or stop_sequence is None:
            return None
        return self._stops.get((scheduled_trip_id, stop_sequence))


def resolve_service_date(start_date: str | None, timezone: str) -> date:
    if start_date:
        try:
            return parse_gtfs_date(start_date)
        except ValueError:
            pass
    return service_date_now(timezone)
