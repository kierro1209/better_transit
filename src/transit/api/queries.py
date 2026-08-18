"""The SQL behind the API, kept out of the route handlers.

The interesting one is :func:`latest_predictions_for_stop`. The predictions table is
append-only, so a stop has many rows per trip - one per poll. "Next arrivals" means "for
each trip, the newest prediction, then order those by predicted arrival".

Postgres does that with DISTINCT ON: it sorts by (rt_trip_id, observed_at DESC) and keeps
the first row of each rt_trip_id group. The alternative is a window function
(ROW_NUMBER() OVER (PARTITION BY rt_trip_id ORDER BY observed_at DESC)) with an outer
filter, which is portable but does the same sort. Both are driven by
ix_arrival_pred_stop_time; without that index this is a sequential scan of the whole
history, which is fine at 10^5 rows and is not fine at 10^8.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy import Row, and_, desc, func, select
from sqlalchemy.orm import Session

from transit.models import (
    ArrivalPrediction,
    IngestRun,
    Route,
    Service,
    ServiceException,
    ShapePoint,
    Stop,
    StopTime,
    Trip,
    VehicleObservation,
)


def get_stop(session: Session, agency_key: str, stop_id: str) -> Stop | None:
    return session.get(Stop, (agency_key, stop_id))


def list_routes(session: Session, agency_key: str) -> list[Route]:
    return list(
        session.scalars(
            select(Route).where(Route.agency_key == agency_key).order_by(Route.short_name)
        )
    )


def route_map(
    session: Session, agency_key: str, route_id: str
) -> tuple[Route, list[list[ShapePoint]], list[Stop]]:
    route = route_by_id_or_short_name(session, agency_key, route_id)
    if route is None:
        raise ValueError(f"unknown route {route_id!r}")

    shape_ids = list(
        session.scalars(
            select(Trip.shape_id)
            .where(
                Trip.agency_key == agency_key,
                Trip.route_id == route.route_id,
                Trip.shape_id.isnot(None),
            )
            .distinct()
        )
    )
    points = (
        list(
            session.scalars(
                select(ShapePoint)
                .where(
                    ShapePoint.agency_key == agency_key,
                    ShapePoint.shape_id.in_(shape_ids),
                )
                .order_by(ShapePoint.shape_id, ShapePoint.shape_pt_sequence)
            )
        )
        if shape_ids
        else []
    )
    paths = []
    for shape_id in shape_ids:
        paths.append([point for point in points if point.shape_id == shape_id])

    stops = list(
        session.scalars(
            select(Stop)
            .join(
                StopTime,
                and_(StopTime.agency_key == Stop.agency_key, StopTime.stop_id == Stop.stop_id),
            )
            .join(
                Trip,
                and_(Trip.agency_key == StopTime.agency_key, Trip.trip_id == StopTime.trip_id),
            )
            .where(
                Stop.agency_key == agency_key,
                Trip.route_id == route.route_id,
            )
            .distinct()
            .order_by(Stop.name)
        )
    )
    return route, paths, stops


def active_service_ids(session: Session, agency_key: str, service_date: date) -> list[str]:
    weekday_column = (
        Service.monday
        if service_date.weekday() == 0
        else Service.tuesday
        if service_date.weekday() == 1
        else Service.wednesday
        if service_date.weekday() == 2
        else Service.thursday
        if service_date.weekday() == 3
        else Service.friday
        if service_date.weekday() == 4
        else Service.saturday
        if service_date.weekday() == 5
        else Service.sunday
    )
    service_ids = set(
        session.scalars(
            select(Service.service_id).where(
                Service.agency_key == agency_key,
                Service.start_date <= service_date,
                Service.end_date >= service_date,
                weekday_column.is_(True),
            )
        )
    )
    exceptions = session.execute(
        select(ServiceException.service_id, ServiceException.exception_type).where(
            ServiceException.agency_key == agency_key,
            ServiceException.exception_date == service_date,
        )
    )
    for service_id, exception_type in exceptions:
        if exception_type == 1:
            service_ids.add(service_id)
        elif exception_type == 2:
            service_ids.discard(service_id)
    return sorted(service_ids)


def schedule_for_stop(
    session: Session,
    agency_key: str,
    stop_id: str,
    service_date: date,
    requested_time: time,
    route_id: str | None = None,
    limit: int = 50,
) -> list[Row]:
    service_ids = active_service_ids(session, agency_key, service_date)
    if not service_ids:
        return []
    seconds = requested_time.hour * 3600 + requested_time.minute * 60 + requested_time.second
    query = (
        select(
            Route.short_name,
            Route.long_name,
            Trip.headsign,
            Trip.trip_id,
            StopTime.stop_id,
            StopTime.arrival_seconds,
            StopTime.departure_seconds,
        )
        .join(Trip, and_(Trip.agency_key == StopTime.agency_key, Trip.trip_id == StopTime.trip_id))
        .join(Route, and_(Route.agency_key == Trip.agency_key, Route.route_id == Trip.route_id))
        .where(
            StopTime.agency_key == agency_key,
            StopTime.stop_id == stop_id,
            Trip.service_id.in_(service_ids),
            func.coalesce(StopTime.departure_seconds, StopTime.arrival_seconds) >= seconds,
        )
        .order_by(func.coalesce(StopTime.departure_seconds, StopTime.arrival_seconds))
        .limit(limit)
    )
    if route_id:
        route = route_by_id_or_short_name(session, agency_key, route_id)
        if route is None:
            return []
        query = query.where(Trip.route_id == route.route_id)
    return list(session.execute(query))


def latest_predictions_for_stop(
    session: Session,
    agency_key: str,
    stop_id: str,
    now: datetime | None = None,
    horizon_minutes: int = 90,
    grace_seconds: int = 60,
    limit: int = 10,
) -> list[Row]:
    now = now or datetime.now(timezone.utc)
    latest = (
        select(ArrivalPrediction)
        .where(
            ArrivalPrediction.agency_key == agency_key,
            ArrivalPrediction.stop_id == stop_id,
            ArrivalPrediction.predicted_arrival.isnot(None),
            ArrivalPrediction.predicted_arrival >= now - timedelta(seconds=grace_seconds),
            ArrivalPrediction.predicted_arrival <= now + timedelta(minutes=horizon_minutes),
        )
        .distinct(ArrivalPrediction.rt_trip_id)
        .order_by(ArrivalPrediction.rt_trip_id, desc(ArrivalPrediction.observed_at))
        .subquery()
    )
    prediction = latest.c
    return list(
        session.execute(
            select(
                prediction.rt_trip_id,
                prediction.scheduled_trip_id,
                prediction.vehicle_id,
                prediction.predicted_arrival,
                prediction.delay_seconds,
                prediction.schedule_relationship,
                prediction.observed_at,
                Route.short_name,
                Route.long_name,
                Trip.headsign,
            )
            .select_from(latest)
            .outerjoin(
                Route,
                and_(
                    Route.agency_key == prediction.agency_key, Route.route_id == prediction.route_id
                ),
            )
            .outerjoin(
                Trip,
                and_(
                    Trip.agency_key == prediction.agency_key,
                    Trip.trip_id == prediction.scheduled_trip_id,
                ),
            )
            .order_by(prediction.predicted_arrival)
            .limit(limit)
        )
    )


def latest_observations_for_route(
    session: Session,
    agency_key: str,
    route_id: str,
    max_age_minutes: int = 15,
    now: datetime | None = None,
) -> list[VehicleObservation]:
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=max_age_minutes)
    return list(
        session.scalars(
            select(VehicleObservation)
            .where(
                VehicleObservation.agency_key == agency_key,
                VehicleObservation.route_id == route_id,
                VehicleObservation.observed_at >= cutoff,
            )
            .distinct(VehicleObservation.vehicle_id)
            .order_by(
                VehicleObservation.vehicle_id,
                desc(VehicleObservation.observed_at),
            )
        )
    )


def latest_observation_for_vehicle(
    session: Session, agency_key: str, vehicle_id: str
) -> VehicleObservation | None:
    return session.scalars(
        select(VehicleObservation)
        .where(
            VehicleObservation.agency_key == agency_key,
            VehicleObservation.vehicle_id == vehicle_id,
        )
        .order_by(desc(VehicleObservation.observed_at))
        .limit(1)
    ).first()


def observations_for_vehicle(
    session: Session,
    agency_key: str,
    vehicle_id: str,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 200,
) -> list[VehicleObservation]:
    query = select(VehicleObservation).where(
        VehicleObservation.agency_key == agency_key,
        VehicleObservation.vehicle_id == vehicle_id,
    )
    if since is not None:
        query = query.where(VehicleObservation.observed_at >= since)
    if until is not None:
        query = query.where(VehicleObservation.observed_at <= until)
    return list(session.scalars(query.order_by(desc(VehicleObservation.observed_at)).limit(limit)))


def latest_ingest_runs(session: Session, agency_key: str) -> list[IngestRun]:
    return list(
        session.scalars(
            select(IngestRun)
            .where(IngestRun.agency_key == agency_key)
            .distinct(IngestRun.feed_kind)
            .order_by(IngestRun.feed_kind, desc(IngestRun.requested_at))
        )
    )


def route_by_id_or_short_name(session: Session, agency_key: str, value: str) -> Route | None:
    route = session.get(Route, (agency_key, value))
    if route is not None:
        return route
    return session.scalars(
        select(Route).where(Route.agency_key == agency_key, Route.short_name == value).limit(1)
    ).first()


def table_counts(session: Session, agency_key: str) -> dict[str, int]:
    def count(model, *criteria) -> int:
        return session.scalar(select(func.count()).select_from(model).where(*criteria)) or 0

    return {
        "stops": count(Stop, Stop.agency_key == agency_key),
        "trips": count(Trip, Trip.agency_key == agency_key),
        "observations": count(VehicleObservation, VehicleObservation.agency_key == agency_key),
        "predictions": count(ArrivalPrediction, ArrivalPrediction.agency_key == agency_key),
    }
