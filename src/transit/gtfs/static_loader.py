"""Load a static GTFS zip into Postgres.

The static feed is the *spine*: the realtime feed only ever sends IDs and Unix timestamps,
so route names, stop names and scheduled times have to come from here.

Loading strategy: delete-then-insert inside a single transaction. The BBB feed is ~124k
stop_times, which loads in a few seconds, and readers never see a half-loaded schedule
because Postgres does not expose uncommitted rows to other transactions.
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx
from sqlalchemy import delete, insert
from sqlalchemy.orm import Session

from transit.config import AGENCY_KEY, AGENCY_TIMEZONE, settings
from transit.models import (
    Agency,
    Route,
    Service,
    ServiceException,
    Stop,
    StopTime,
    Trip,
)
from transit.timeutil import parse_gtfs_date, parse_gtfs_time

log = logging.getLogger(__name__)

BATCH_SIZE = 5000


@dataclass
class LoadReport:
    routes: int = 0
    stops: int = 0
    trips: int = 0
    stop_times: int = 0
    services: int = 0
    service_exceptions: int = 0
    skipped_stop_times: int = 0

    def as_dict(self) -> dict[str, int]:
        return self.__dict__.copy()


def download_static_feed(url: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with httpx.stream("GET", url, timeout=120.0, follow_redirects=True) as response:
        response.raise_for_status()
        with destination.open("wb") as handle:
            for chunk in response.iter_bytes():
                handle.write(chunk)
    return destination


def _rows(archive: zipfile.ZipFile, name: str) -> Iterator[dict[str, str]]:
    if name not in archive.namelist():
        return
    with archive.open(name) as raw:
        # GTFS files are UTF-8 and frequently carry a BOM; utf-8-sig strips it so the first
        # column name does not silently become "\ufeffagency_id".
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        yield from csv.DictReader(text)


def _flush(session: Session, model, buffer: list[dict]) -> int:
    if not buffer:
        return 0
    session.execute(insert(model), buffer)
    count = len(buffer)
    buffer.clear()
    return count


def _int_or_none(value: str | None) -> int | None:
    if value is None or not value.strip():
        return None
    return int(float(value))


def _float_or_none(value: str | None) -> float | None:
    if value is None or not value.strip():
        return None
    return float(value)


def load_static_feed(session: Session, zip_path: Path, agency_key: str = AGENCY_KEY) -> LoadReport:
    report = LoadReport()
    archive = zipfile.ZipFile(zip_path)

    for model in (StopTime, Trip, ServiceException, Service, Stop, Route, Agency):
        session.execute(delete(model).where(model.agency_key == agency_key))

    agency_row = next(_rows(archive, "agency.txt"), None)
    session.execute(
        insert(Agency),
        [
            {
                "agency_key": agency_key,
                "gtfs_agency_id": (agency_row or {}).get("agency_id"),
                "name": (agency_row or {}).get("agency_name", agency_key),
                "timezone": (agency_row or {}).get("agency_timezone", AGENCY_TIMEZONE),
                "url": (agency_row or {}).get("agency_url"),
            }
        ],
    )

    buffer: list[dict] = []
    for row in _rows(archive, "routes.txt"):
        buffer.append(
            {
                "agency_key": agency_key,
                "route_id": row["route_id"],
                "short_name": row.get("route_short_name") or None,
                "long_name": row.get("route_long_name") or None,
                "route_type": _int_or_none(row.get("route_type")),
                "color": row.get("route_color") or None,
            }
        )
        if len(buffer) >= BATCH_SIZE:
            report.routes += _flush(session, Route, buffer)
    report.routes += _flush(session, Route, buffer)

    for row in _rows(archive, "stops.txt"):
        buffer.append(
            {
                "agency_key": agency_key,
                "stop_id": row["stop_id"],
                "stop_code": row.get("stop_code") or None,
                "name": row.get("stop_name") or row["stop_id"],
                "latitude": _float_or_none(row.get("stop_lat")),
                "longitude": _float_or_none(row.get("stop_lon")),
            }
        )
        if len(buffer) >= BATCH_SIZE:
            report.stops += _flush(session, Stop, buffer)
    report.stops += _flush(session, Stop, buffer)

    for row in _rows(archive, "calendar.txt"):
        buffer.append(
            {
                "agency_key": agency_key,
                "service_id": row["service_id"],
                "monday": row.get("monday") == "1",
                "tuesday": row.get("tuesday") == "1",
                "wednesday": row.get("wednesday") == "1",
                "thursday": row.get("thursday") == "1",
                "friday": row.get("friday") == "1",
                "saturday": row.get("saturday") == "1",
                "sunday": row.get("sunday") == "1",
                "start_date": parse_gtfs_date(row["start_date"]),
                "end_date": parse_gtfs_date(row["end_date"]),
            }
        )
        if len(buffer) >= BATCH_SIZE:
            report.services += _flush(session, Service, buffer)
    report.services += _flush(session, Service, buffer)

    seen_exceptions: set[tuple[str, str]] = set()
    for row in _rows(archive, "calendar_dates.txt"):
        key = (row["service_id"], row["date"])
        if key in seen_exceptions:
            continue
        seen_exceptions.add(key)
        buffer.append(
            {
                "agency_key": agency_key,
                "service_id": row["service_id"],
                "exception_date": parse_gtfs_date(row["date"]),
                "exception_type": int(row["exception_type"]),
            }
        )
        if len(buffer) >= BATCH_SIZE:
            report.service_exceptions += _flush(session, ServiceException, buffer)
    report.service_exceptions += _flush(session, ServiceException, buffer)

    # stop_times is read before trips so each trip can carry its first departure time,
    # which is the key the realtime feed matches trips on.
    stop_times_by_trip: dict[str, int] = {}
    for row in _rows(archive, "stop_times.txt"):
        try:
            arrival = parse_gtfs_time(row.get("arrival_time"))
            departure = parse_gtfs_time(row.get("departure_time"))
            sequence = int(row["stop_sequence"])
        except (ValueError, KeyError):
            report.skipped_stop_times += 1
            continue
        first = departure if departure is not None else arrival
        if first is not None:
            current = stop_times_by_trip.get(row["trip_id"])
            if current is None or first < current:
                stop_times_by_trip[row["trip_id"]] = first
        buffer.append(
            {
                "agency_key": agency_key,
                "trip_id": row["trip_id"],
                "stop_sequence": sequence,
                "stop_id": row["stop_id"],
                "arrival_seconds": arrival,
                "departure_seconds": departure,
                "timepoint": row.get("timepoint", "1") != "0",
            }
        )
        if len(buffer) >= BATCH_SIZE:
            report.stop_times += _flush(session, StopTime, buffer)
    report.stop_times += _flush(session, StopTime, buffer)

    for row in _rows(archive, "trips.txt"):
        buffer.append(
            {
                "agency_key": agency_key,
                "trip_id": row["trip_id"],
                "route_id": row["route_id"],
                "service_id": row["service_id"],
                "direction_id": _int_or_none(row.get("direction_id")),
                "headsign": row.get("trip_headsign") or None,
                "shape_id": row.get("shape_id") or None,
                "start_seconds": stop_times_by_trip.get(row["trip_id"]),
            }
        )
        if len(buffer) >= BATCH_SIZE:
            report.trips += _flush(session, Trip, buffer)
    report.trips += _flush(session, Trip, buffer)

    return report


def refresh_static_feed(session: Session, url: str | None = None, cache_dir: Path | None = None):
    url = url or settings.bbb_static_url
    cache_dir = cache_dir or Path("data")
    zip_path = download_static_feed(url, cache_dir / "gtfs_static.zip")
    return load_static_feed(session, zip_path)
