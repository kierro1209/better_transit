"""Normalized internal data model.

Two families of tables live here and they are deliberately not mixed:

*Schedule tables* (agencies, routes, stops, trips, stop_times, services) mirror the static
GTFS feed. They are slowly changing: reloaded when the agency publishes a new zip.

*Observation tables* (ingest_runs, vehicle_observations, arrival_predictions) are
append-only. A newer reading never overwrites an older one, because the history is the
product we are building.

Agency identifiers are namespaced by ``agency_key`` so a second agency (LA Metro) can be
added later without colliding with Big Blue Bus IDs.
"""

from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Agency(Base):
    __tablename__ = "agencies"

    agency_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    gtfs_agency_id: Mapped[str | None] = mapped_column(String(64))
    name: Mapped[str] = mapped_column(Text)
    timezone: Mapped[str] = mapped_column(String(64))
    url: Mapped[str | None] = mapped_column(Text)


class Route(Base):
    __tablename__ = "routes"

    agency_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    route_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    short_name: Mapped[str | None] = mapped_column(String(32), index=True)
    long_name: Mapped[str | None] = mapped_column(Text)
    route_type: Mapped[int | None] = mapped_column(Integer)
    color: Mapped[str | None] = mapped_column(String(8))


class Stop(Base):
    __tablename__ = "stops"

    agency_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    stop_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    stop_code: Mapped[str | None] = mapped_column(String(32))
    name: Mapped[str] = mapped_column(Text)
    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)


class Service(Base):
    """calendar.txt: which weekdays a service_id runs, over which date range."""

    __tablename__ = "services"

    agency_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    service_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    monday: Mapped[bool] = mapped_column(Boolean, default=False)
    tuesday: Mapped[bool] = mapped_column(Boolean, default=False)
    wednesday: Mapped[bool] = mapped_column(Boolean, default=False)
    thursday: Mapped[bool] = mapped_column(Boolean, default=False)
    friday: Mapped[bool] = mapped_column(Boolean, default=False)
    saturday: Mapped[bool] = mapped_column(Boolean, default=False)
    sunday: Mapped[bool] = mapped_column(Boolean, default=False)
    start_date: Mapped[date] = mapped_column(Date)
    end_date: Mapped[date] = mapped_column(Date)


class ServiceException(Base):
    """calendar_dates.txt: one-off additions (1) and removals (2) of service."""

    __tablename__ = "service_exceptions"

    agency_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    service_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    exception_date: Mapped[date] = mapped_column(Date, primary_key=True)
    exception_type: Mapped[int] = mapped_column(Integer)


class Trip(Base):
    """One scheduled run of a route on a service day.

    ``start_seconds`` is denormalized from the trip's first stop_time. The realtime feed
    identifies trips by (route short name, start time) rather than by static trip_id, so
    this column is what makes that join cheap and indexable.
    """

    __tablename__ = "trips"

    agency_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    trip_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    route_id: Mapped[str] = mapped_column(String(64), index=True)
    service_id: Mapped[str] = mapped_column(String(64), index=True)
    direction_id: Mapped[int | None] = mapped_column(Integer)
    headsign: Mapped[str | None] = mapped_column(Text)
    shape_id: Mapped[str | None] = mapped_column(String(64))
    start_seconds: Mapped[int | None] = mapped_column(Integer)

    __table_args__ = (
        ForeignKeyConstraint(["agency_key", "route_id"], ["routes.agency_key", "routes.route_id"]),
        Index("ix_trips_route_start", "agency_key", "route_id", "start_seconds"),
    )


class StopTime(Base):
    """Scheduled arrival/departure of one trip at one stop.

    GTFS times can exceed 24:00:00 (a 00:20 trip belonging to the previous service day),
    so they are stored as seconds after noon-minus-12h of the service date, not as clock
    times. Converting to a real instant requires the service date and the agency timezone.
    """

    __tablename__ = "stop_times"

    agency_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    trip_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    stop_sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    stop_id: Mapped[str] = mapped_column(String(64))
    arrival_seconds: Mapped[int | None] = mapped_column(Integer)
    departure_seconds: Mapped[int | None] = mapped_column(Integer)
    timepoint: Mapped[bool] = mapped_column(Boolean, default=False)

    __table_args__ = (Index("ix_stop_times_stop", "agency_key", "stop_id", "arrival_seconds"),)


class FavoriteStop(Base):
    """The handful of stops this system actually cares about."""

    __tablename__ = "favorite_stops"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    label: Mapped[str] = mapped_column(String(64), index=True)
    agency_key: Mapped[str] = mapped_column(String(32))
    stop_id: Mapped[str] = mapped_column(String(64))
    walk_minutes: Mapped[int | None] = mapped_column(Integer)

    __table_args__ = (UniqueConstraint("label", "agency_key", "stop_id"),)


class IngestRun(Base):
    """One poll of one realtime feed. Written whether or not the poll succeeded."""

    __tablename__ = "ingest_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    agency_key: Mapped[str] = mapped_column(String(32), index=True)
    feed_kind: Mapped[str] = mapped_column(String(32))
    feed_url: Mapped[str] = mapped_column(Text)

    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    request_latency_ms: Mapped[int | None] = mapped_column(Integer)

    http_status: Mapped[int | None] = mapped_column(Integer)
    response_bytes: Mapped[int | None] = mapped_column(Integer)
    feed_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    feed_age_seconds: Mapped[int | None] = mapped_column(Integer)
    is_stale: Mapped[bool | None] = mapped_column(Boolean)

    entity_count: Mapped[int | None] = mapped_column(Integer)
    usable_count: Mapped[int | None] = mapped_column(Integer)
    rejected_count: Mapped[int | None] = mapped_column(Integer)
    persisted_count: Mapped[int | None] = mapped_column(Integer)

    ok: Mapped[bool] = mapped_column(Boolean, default=False)
    error: Mapped[str | None] = mapped_column(Text)


class VehicleObservation(Base):
    """Where a physical vehicle was, as reported by the agency. Append-only.

    ``observed_at`` is when the agency says the reading was taken; ``ingested_at`` is when
    we wrote it down. They differ by feed latency plus our polling interval, and keeping
    both is what lets us measure that latency later.

    Latitude/longitude are nullable: Big Blue Bus's VehiclePositions feed is currently
    frozen, so rows sourced from TripUpdates carry vehicle/trip/next-stop but no position.
    """

    __tablename__ = "vehicle_observations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ingest_run_id: Mapped[int | None] = mapped_column(BigInteger, index=True)

    agency_key: Mapped[str] = mapped_column(String(32))
    vehicle_id: Mapped[str] = mapped_column(String(64))
    rt_trip_id: Mapped[str | None] = mapped_column(String(64))
    rt_route_id: Mapped[str | None] = mapped_column(String(64))
    scheduled_trip_id: Mapped[str | None] = mapped_column(String(64))
    route_id: Mapped[str | None] = mapped_column(String(64))

    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)
    bearing: Mapped[float | None] = mapped_column(Float)
    speed: Mapped[float | None] = mapped_column(Float)

    # rt_stop_id is the identifier the feed used; stop_id is the static GTFS stop it was
    # resolved to. They are different id spaces in this feed - see transit.matching.
    rt_stop_id: Mapped[str | None] = mapped_column(String(64))
    stop_id: Mapped[str | None] = mapped_column(String(64))
    stop_sequence: Mapped[int | None] = mapped_column(Integer)
    current_status: Mapped[str | None] = mapped_column(String(32))
    source: Mapped[str] = mapped_column(String(32))

    __table_args__ = (
        # Same vehicle, same reading time, same source: the agency republished a reading we
        # already have. Postgres rejects the duplicate; the ingester counts it and moves on.
        UniqueConstraint(
            "agency_key",
            "vehicle_id",
            "observed_at",
            "source",
            name="uq_vehicle_observation_reading",
        ),
        Index("ix_vehicle_obs_vehicle_time", "agency_key", "vehicle_id", "observed_at"),
        Index("ix_vehicle_obs_route_time", "agency_key", "route_id", "observed_at"),
    )


class ArrivalPrediction(Base):
    """ "At observed_at, the agency predicted trip T reaches stop S at predicted_time."

    Append-only, and the reason this table is separate from VehicleObservation: a
    prediction is a claim about the future, a position is a fact about the past. Keeping
    every successive prediction for the same (trip, stop) is what makes it possible later
    to ask "how wrong was the 20-minutes-out prediction?" — the ML label.
    """

    __tablename__ = "arrival_predictions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ingest_run_id: Mapped[int | None] = mapped_column(BigInteger, index=True)

    agency_key: Mapped[str] = mapped_column(String(32))
    rt_trip_id: Mapped[str] = mapped_column(String(64))
    rt_route_id: Mapped[str | None] = mapped_column(String(64))
    vehicle_id: Mapped[str | None] = mapped_column(String(64))

    scheduled_trip_id: Mapped[str | None] = mapped_column(String(64))
    route_id: Mapped[str | None] = mapped_column(String(64))

    service_date: Mapped[date | None] = mapped_column(Date)
    trip_start_seconds: Mapped[int | None] = mapped_column(Integer)

    # Raw feed identity is preserved; stop_id is the static stop resolved from
    # (scheduled trip, stop_sequence) and is null when the trip could not be matched.
    rt_stop_id: Mapped[str] = mapped_column(String(64))
    stop_id: Mapped[str | None] = mapped_column(String(64))
    stop_sequence: Mapped[int | None] = mapped_column(Integer)

    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    predicted_arrival: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    predicted_departure: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delay_seconds: Mapped[int | None] = mapped_column(Integer)
    schedule_relationship: Mapped[str | None] = mapped_column(String(32))

    __table_args__ = (
        UniqueConstraint(
            "agency_key",
            "rt_trip_id",
            "rt_stop_id",
            "stop_sequence",
            "observed_at",
            name="uq_arrival_prediction_reading",
        ),
        Index(
            "ix_arrival_pred_stop_time",
            "agency_key",
            "stop_id",
            "predicted_arrival",
        ),
        Index("ix_arrival_pred_trip", "agency_key", "rt_trip_id", "observed_at"),
    )
