"""HTTP API.

What FastAPI does when a request arrives: uvicorn (an ASGI server) accepts the TCP
connection, parses the HTTP request and hands FastAPI a `scope` dict plus receive/send
callables. FastAPI matches the path against its route table, extracts and validates path and
query parameters against the handler's type hints, resolves dependencies (here: a database
session), calls the handler, then serializes the returned pydantic model to JSON.

The handlers below are ``def``, not ``async def``, on purpose. psycopg's synchronous driver
blocks the thread while Postgres answers; a blocking call inside ``async def`` would block
the whole event loop and every other in-flight request with it. FastAPI runs plain ``def``
handlers in a worker threadpool instead, so blocking there is safe. ``async def`` would only
pay off with an async driver, and at this request volume it would buy nothing measurable.
"""

from __future__ import annotations

import logging
import threading
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from transit.api import queries
from transit.api.schemas import (
    ArrivalOut,
    ArrivalsResponse,
    FavoriteOut,
    FeedHealth,
    FreshnessInfo,
    GeocodeResult,
    HealthResponse,
    MapPoint,
    NearbyStopOut,
    NearbyStopsResponse,
    PlanOption,
    PlanRequest,
    PlanResponse,
    RouteMapResponse,
    RouteOut,
    ScheduleOut,
    ScheduleResponse,
    ShortcutResponse,
    StopOut,
    VehicleDetailResponse,
    VehicleHistoryPoint,
    VehicleOut,
    VehiclesResponse,
)
from transit.config import AGENCY_KEY, AGENCY_TIMEZONE, Settings, settings
from transit.db import get_session
from transit.freshness import Freshness
from transit.ingest import run_loop
from transit.models import FavoriteStop, Stop
from transit.timeutil import service_time_to_instant
from transit.web import STATIC_DIR

log = logging.getLogger("transit.api")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Optionally run realtime ingestion alongside the API process.

    The setting is loaded when the server starts rather than when this module is imported,
    which keeps CLI options working when uvicorn's reload supervisor launches a child process.
    """
    runtime_settings = Settings()
    stop_event: threading.Event | None = None
    poller: threading.Thread | None = None
    if runtime_settings.ingest_in_server:
        stop_event = threading.Event()
        poller = threading.Thread(
            target=run_loop,
            args=(runtime_settings.poll_interval_seconds, stop_event),
            daemon=True,
            name="ingestion-poller",
        )
        poller.start()
        log.info(
            "in-process ingestion poller started",
            extra={"fields": {"interval_seconds": runtime_settings.poll_interval_seconds}},
        )
    else:
        log.info("in-process ingestion poller not started", extra={"fields": {"enabled": False}})

    try:
        yield
    finally:
        if stop_event is not None and poller is not None:
            log.info("stopping in-process ingestion poller")
            stop_event.set()
            poller.join(timeout=5)
            if poller.is_alive():
                log.warning("in-process ingestion poller did not stop before timeout")
            else:
                log.info("in-process ingestion poller stopped")


app = FastAPI(
    title="UCLA Transit Intelligence",
    description="Realtime arrivals and vehicle history for the Big Blue Bus routes around UCLA.",
    version="0.1.0",
    lifespan=lifespan,
)

# The UI is served from this same app, but a Shortcut or a separate dev server may not be.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _freshness(moment: datetime | None, now: datetime) -> FreshnessInfo:
    info = Freshness.of(moment, now, settings.feed_stale_after_seconds)
    return FreshnessInfo(
        observed_at=info.observed_at, age_seconds=info.age_seconds, stale=info.stale
    )


@app.get("/health", response_model=HealthResponse, tags=["ops"])
def health(session: Session = Depends(get_session)) -> HealthResponse:
    """Liveness plus data health.

    Inputs: none. Queries: one `SELECT 1`, four counts, one DISTINCT ON over ingest_runs.
    Failure cases: database unreachable -> HTTP 503. A reachable database with a stale feed
    is reported as ``status="degraded"`` rather than an error, because the process is
    healthy and the *data* is not.
    """
    now = datetime.now(timezone.utc)
    try:
        session.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001 - surfaced to the caller as 503
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc

    counts = queries.table_counts(session, AGENCY_KEY)
    feeds = [
        FeedHealth(
            feed_kind=run.feed_kind,
            last_run_at=run.requested_at,
            ok=run.ok,
            feed_timestamp=run.feed_timestamp,
            feed_age_seconds=run.feed_age_seconds,
            stale=run.is_stale,
            entity_count=run.entity_count,
            persisted_count=run.persisted_count,
            request_latency_ms=run.request_latency_ms,
            error=run.error,
        )
        for run in queries.latest_ingest_runs(session, AGENCY_KEY)
    ]
    trip_update_feed = next((f for f in feeds if f.feed_kind == "trip_updates"), None)
    ingest_recent = (
        trip_update_feed is not None
        and trip_update_feed.last_run_at is not None
        and (now - trip_update_feed.last_run_at) < timedelta(minutes=5)
    )
    status = "ok" if counts["stops"] and ingest_recent else "degraded"
    return HealthResponse(
        status=status,
        database="ok",
        schedule_loaded=bool(counts["stops"]),
        stop_count=counts["stops"],
        trip_count=counts["trips"],
        observation_count=counts["observations"],
        prediction_count=counts["predictions"],
        feeds=feeds,
    )


@app.get("/routes", response_model=list[RouteOut], tags=["schedule"])
def list_routes(session: Session = Depends(get_session)) -> list[RouteOut]:
    """Every route in the loaded schedule. Static data: no freshness semantics."""
    return [
        RouteOut(
            route_id=route.route_id,
            short_name=route.short_name,
            long_name=route.long_name,
            route_type=route.route_type,
            color=route.color,
        )
        for route in queries.list_routes(session, AGENCY_KEY)
    ]


@app.get("/routes/{route_id}/map", response_model=RouteMapResponse, tags=["schedule"])
def route_map(route_id: str, session: Session = Depends(get_session)) -> RouteMapResponse:
    try:
        route, paths, stops = queries.route_map(session, AGENCY_KEY, route_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return RouteMapResponse(
        route_id=route.route_id,
        route_short_name=route.short_name,
        paths=[
            [MapPoint(latitude=point.latitude, longitude=point.longitude) for point in path]
            for path in paths
        ],
        stops=[
            StopOut(
                stop_id=stop.stop_id,
                name=stop.name,
                latitude=stop.latitude,
                longitude=stop.longitude,
            )
            for stop in stops
        ],
    )


@app.get("/favorites", response_model=list[FavoriteOut], tags=["schedule"])
def list_favorites(session: Session = Depends(get_session)) -> list[FavoriteOut]:
    """The stops this system exists for. Consumed by the UI and the Shortcut."""
    rows = (
        session.query(FavoriteStop, Stop)
        .join(
            Stop,
            (Stop.agency_key == FavoriteStop.agency_key) & (Stop.stop_id == FavoriteStop.stop_id),
        )
        .order_by(FavoriteStop.label, Stop.name)
        .all()
    )
    return [
        FavoriteOut(
            label=favorite.label,
            stop_id=favorite.stop_id,
            stop_name=stop.name,
            walk_minutes=favorite.walk_minutes,
        )
        for favorite, stop in rows
    ]


@app.get("/geocode", response_model=list[GeocodeResult], tags=["planning"])
def geocode(q: str = Query(min_length=2, max_length=200)) -> list[GeocodeResult]:
    """Search a place using OpenStreetMap's public Nominatim service."""
    response = httpx.get(
        "https://nominatim.openstreetmap.org/search",
        params={"q": q, "format": "jsonv2", "limit": 5, "countrycodes": "us"},
        headers={"User-Agent": "better-transit/0.1 (UCLA transit planner)"},
        timeout=settings.http_timeout_seconds,
    )
    response.raise_for_status()
    return [
        GeocodeResult(
            display_name=item["display_name"],
            latitude=float(item["lat"]),
            longitude=float(item["lon"]),
        )
        for item in response.json()
    ]


@app.get("/nearby/stops", response_model=NearbyStopsResponse, tags=["planning"])
def nearby(
    latitude: float = Query(ge=-90, le=90),
    longitude: float = Query(ge=-180, le=180),
    limit: int = Query(default=8, ge=1, le=20),
    session: Session = Depends(get_session),
) -> NearbyStopsResponse:
    rows = queries.nearby_stops(session, AGENCY_KEY, latitude, longitude, limit)
    return NearbyStopsResponse(
        latitude=latitude,
        longitude=longitude,
        stops=[
            NearbyStopOut(
                stop_id=stop.stop_id,
                name=stop.name,
                latitude=stop.latitude,
                longitude=stop.longitude,
                distance_meters=distance,
                routes=routes,
            )
            for stop, distance, routes in rows
        ],
    )


@app.post("/plan", response_model=PlanResponse, tags=["planning"])
def plan(request: PlanRequest, session: Session = Depends(get_session)) -> PlanResponse:
    if request.mode not in {"depart_now", "arrive_by"}:
        raise HTTPException(status_code=422, detail="mode must be depart_now or arrive_by")
    origin = queries.get_stop(session, AGENCY_KEY, request.origin_stop_id)
    destination = queries.get_stop(session, AGENCY_KEY, request.destination_stop_id)
    if origin is None or destination is None:
        raise HTTPException(status_code=404, detail="unknown origin or destination stop")
    now = datetime.now(timezone.utc)
    deadline = request.arrive_by if request.mode == "arrive_by" else None
    agency_date = now.astimezone(ZoneInfo(AGENCY_TIMEZONE)).date()
    rows = queries.direct_trip_options(
        session,
        AGENCY_KEY,
        origin.stop_id,
        destination.stop_id,
        agency_date,
        now,
        arrive_by=deadline,
    )
    options = []
    for row in rows:
        next_bus_at = service_time_to_instant(agency_date, row.origin_seconds, AGENCY_TIMEZONE)
        arrival_at = service_time_to_instant(agency_date, row.destination_seconds, AGENCY_TIMEZONE)
        scheduled_departure = next_bus_at
        prediction = queries.latest_prediction_for_trip_at_stop(
            session, AGENCY_KEY, row.trip_id, origin.stop_id, now
        )
        realtime = prediction is not None and prediction.predicted_arrival is not None
        if not realtime and next_bus_at < now - timedelta(minutes=1):
            continue
        if realtime:
            next_bus_at = prediction.predicted_arrival
            arrival_at = next_bus_at + (arrival_at - scheduled_departure)
        if request.mode == "arrive_by" and deadline is not None and arrival_at > deadline:
            continue
        origin_out = StopOut(
            stop_id=origin.stop_id,
            name=origin.name,
            latitude=origin.latitude,
            longitude=origin.longitude,
        )
        destination_out = StopOut(
            stop_id=destination.stop_id,
            name=destination.name,
            latitude=destination.latitude,
            longitude=destination.longitude,
        )
        options.append(
            PlanOption(
                route_short_name=row.short_name,
                headsign=row.headsign,
                origin_stop=origin_out,
                destination_stop=destination_out,
                next_bus_at=next_bus_at,
                estimated_arrival_at=arrival_at,
                travel_minutes=max(1, round((arrival_at - next_bus_at).total_seconds() / 60)),
                wait_minutes=max(0, round((next_bus_at - now).total_seconds() / 60)),
                realtime=realtime,
            )
        )
    return PlanResponse(
        origin=StopOut(
            stop_id=origin.stop_id,
            name=origin.name,
            latitude=origin.latitude,
            longitude=origin.longitude,
        ),
        destination=StopOut(
            stop_id=destination.stop_id,
            name=destination.name,
            latitude=destination.latitude,
            longitude=destination.longitude,
        ),
        options=options,
    )


@app.get("/stops/{stop_id}/schedule", response_model=ScheduleResponse, tags=["schedule"])
def stop_schedule(
    stop_id: str,
    service_date: date = Query(alias="date"),
    requested_time: time = Query(alias="time"),
    route: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=100),
    session: Session = Depends(get_session),
) -> ScheduleResponse:
    stop = queries.get_stop(session, AGENCY_KEY, stop_id)
    if stop is None:
        raise HTTPException(status_code=404, detail=f"unknown stop {stop_id!r}")
    rows = queries.schedule_for_stop(
        session,
        AGENCY_KEY,
        stop_id,
        service_date,
        requested_time,
        route_id=route,
        limit=limit,
    )
    schedule = []
    for row in rows:
        seconds = row.departure_seconds or row.arrival_seconds
        if seconds is None:
            continue
        scheduled_time = service_time_to_instant(service_date, seconds, AGENCY_TIMEZONE)
        schedule.append(
            ScheduleOut(
                route_short_name=row.short_name,
                route_long_name=row.long_name,
                headsign=row.headsign,
                trip_id=row.trip_id,
                stop_id=row.stop_id,
                scheduled_time=scheduled_time,
                scheduled_time_label=f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}",
            )
        )
    return ScheduleResponse(
        stop=StopOut(
            stop_id=stop.stop_id,
            name=stop.name,
            latitude=stop.latitude,
            longitude=stop.longitude,
        ),
        service_date=service_date,
        requested_time=requested_time.strftime("%H:%M"),
        generated_at=datetime.now(timezone.utc),
        schedule=schedule,
    )


def _arrivals(session: Session, stop_id: str, limit: int, horizon_minutes: int) -> ArrivalsResponse:
    """Shared by the HTTP handler and the Shortcut endpoint.

    Handlers whose parameters carry ``Query(...)`` defaults cannot be called as plain
    functions - the default *is* the Query object - so the work lives here instead.
    """
    now = datetime.now(timezone.utc)
    stop = queries.get_stop(session, AGENCY_KEY, stop_id)
    if stop is None:
        raise HTTPException(status_code=404, detail=f"unknown stop {stop_id!r}")

    rows = queries.latest_predictions_for_stop(
        session, AGENCY_KEY, stop_id, now=now, horizon_minutes=horizon_minutes, limit=limit
    )
    arrivals = []
    newest_observation: datetime | None = None
    for row in rows:
        eta_seconds = (
            int((row.predicted_arrival - now).total_seconds())
            if row.predicted_arrival is not None
            else None
        )
        if newest_observation is None or row.observed_at > newest_observation:
            newest_observation = row.observed_at
        arrivals.append(
            ArrivalOut(
                route_short_name=row.short_name,
                route_long_name=row.long_name,
                headsign=row.headsign,
                rt_trip_id=row.rt_trip_id,
                scheduled_trip_id=row.scheduled_trip_id,
                vehicle_id=row.vehicle_id,
                predicted_arrival=row.predicted_arrival,
                eta_seconds=eta_seconds,
                eta_minutes=None if eta_seconds is None else max(0, round(eta_seconds / 60)),
                delay_seconds=row.delay_seconds,
                schedule_relationship=row.schedule_relationship,
                freshness=_freshness(row.observed_at, now),
            )
        )

    return ArrivalsResponse(
        stop=StopOut(
            stop_id=stop.stop_id,
            name=stop.name,
            latitude=stop.latitude,
            longitude=stop.longitude,
        ),
        generated_at=now,
        feed_freshness=_freshness(newest_observation, now),
        arrivals=arrivals,
    )


@app.get("/stops/{stop_id}/arrivals", response_model=ArrivalsResponse, tags=["realtime"])
def stop_arrivals(
    stop_id: str,
    limit: int = Query(default=8, ge=1, le=50),
    horizon_minutes: int = Query(default=90, ge=5, le=240),
    session: Session = Depends(get_session),
) -> ArrivalsResponse:
    """Upcoming realtime arrivals at one stop.

    Input: a static GTFS stop id. Output: at most ``limit`` arrivals ordered by predicted
    arrival, each carrying its own freshness.

    Queries: one lookup in `stops`, one DISTINCT ON over `arrival_predictions` joined to
    routes/trips.

    Failure cases: unknown stop -> 404. A known stop with no realtime data (night, or a stop
    the feed never mentions) -> 200 with an empty list; that is information, not an error.

    Freshness: ``feed_freshness`` is the newest observation backing any arrival in the list;
    each arrival also reports when its own prediction was made. Nothing is interpolated - if
    the agency stops predicting, the numbers stop moving and the reported age grows, which is
    exactly what a rider needs to see.
    """
    return _arrivals(session, stop_id, limit, horizon_minutes)


def _vehicle_out(observation, now: datetime, route_short_name: str | None = None) -> VehicleOut:
    return VehicleOut(
        vehicle_id=observation.vehicle_id,
        rt_trip_id=observation.rt_trip_id,
        scheduled_trip_id=observation.scheduled_trip_id,
        route_id=observation.route_id,
        route_short_name=route_short_name or observation.rt_route_id,
        latitude=observation.latitude,
        longitude=observation.longitude,
        bearing=observation.bearing,
        speed=observation.speed,
        stop_id=observation.stop_id,
        stop_sequence=observation.stop_sequence,
        current_status=observation.current_status,
        source=observation.source,
        freshness=_freshness(observation.observed_at, now),
    )


@app.get("/routes/{route_id}/vehicles", response_model=VehiclesResponse, tags=["realtime"])
def route_vehicles(
    route_id: str,
    max_age_minutes: int = Query(default=15, ge=1, le=180),
    session: Session = Depends(get_session),
) -> VehiclesResponse:
    """Currently known vehicles on a route.

    Input: a static route id ("4108") or a route short name ("R12"). Output: the newest
    observation per vehicle seen within ``max_age_minutes``.

    ``position_data_available`` is false whenever no returned observation carries
    coordinates - which is the current state of Big Blue Bus's VehiclePositions feed. The
    field exists so a map client can say "no positions" instead of drawing an empty map and
    implying there are no buses.
    """
    now = datetime.now(timezone.utc)
    route = queries.route_by_id_or_short_name(session, AGENCY_KEY, route_id)
    if route is None:
        raise HTTPException(status_code=404, detail=f"unknown route {route_id!r}")

    observations = queries.latest_observations_for_route(
        session, AGENCY_KEY, route.route_id, max_age_minutes=max_age_minutes, now=now
    )
    vehicles = [_vehicle_out(observation, now, route.short_name) for observation in observations]
    vehicles.sort(key=lambda v: v.freshness.age_seconds or 0)
    return VehiclesResponse(
        route_id=route.route_id,
        route_short_name=route.short_name,
        generated_at=now,
        position_data_available=any(v.latitude is not None for v in vehicles),
        vehicles=vehicles,
    )


@app.get("/vehicles/{vehicle_id}", response_model=VehicleDetailResponse, tags=["realtime"])
def vehicle_detail(
    vehicle_id: str,
    since: datetime | None = Query(default=None, description="ISO timestamp, inclusive"),
    until: datetime | None = Query(default=None, description="ISO timestamp, inclusive"),
    limit: int = Query(default=100, ge=1, le=1000),
    session: Session = Depends(get_session),
) -> VehicleDetailResponse:
    """Latest known state of one vehicle plus its recent observation history.

    This is the endpoint that reads the append-only history rather than current state:
    ``?since=...&until=...`` answers "all observations for vehicle X between 3 and 4 PM".
    Unknown vehicle -> 404.
    """
    now = datetime.now(timezone.utc)
    latest = queries.latest_observation_for_vehicle(session, AGENCY_KEY, vehicle_id)
    if latest is None:
        raise HTTPException(status_code=404, detail=f"unknown vehicle {vehicle_id!r}")
    history = queries.observations_for_vehicle(
        session, AGENCY_KEY, vehicle_id, since=since, until=until, limit=limit
    )
    return VehicleDetailResponse(
        vehicle_id=vehicle_id,
        latest=_vehicle_out(latest, now),
        observations=[
            VehicleHistoryPoint(
                observed_at=observation.observed_at,
                ingested_at=observation.ingested_at,
                latitude=observation.latitude,
                longitude=observation.longitude,
                stop_id=observation.stop_id,
                stop_sequence=observation.stop_sequence,
                rt_trip_id=observation.rt_trip_id,
                source=observation.source,
            )
            for observation in history
        ],
    )


@app.get("/shortcut/next", response_model=ShortcutResponse, tags=["shortcut"])
def shortcut_next(
    label: str = Query(description="Favorite label, e.g. UCLA"),
    route: str | None = Query(default=None, description="Filter to one route short name"),
    session: Session = Depends(get_session),
) -> ShortcutResponse:
    """One deterministic answer to "when should I leave?".

    The agent's job stops at calling this: the recommendation, the numbers and the wording of
    ``summary`` are all computed here from stored predictions. Nothing is generated by a
    model, so the same request always yields the same answer and there is nothing for an LLM
    to invent. ``leave_in_minutes`` subtracts the favorite's configured walking time; it can
    be negative, which means the bus is already unreachable on foot.

    Unknown label -> 404. Known label with no realtime data -> 200 with ``found=false``.
    """
    now = datetime.now(timezone.utc)
    favorites = [f for f in list_favorites(session=session) if f.label.upper() == label.upper()]
    if not favorites:
        raise HTTPException(status_code=404, detail=f"unknown favorite label {label!r}")

    best: tuple[FavoriteOut, ArrivalOut] | None = None
    for favorite in favorites:
        arrivals = _arrivals(session, favorite.stop_id, limit=10, horizon_minutes=90).arrivals
        for arrival in arrivals:
            if route and (arrival.route_short_name or "").upper() != route.upper():
                continue
            # A prediction the agency stopped refreshing minutes ago is not something to
            # send someone out the door on, and neither is a bus that already left.
            if arrival.eta_seconds is None or arrival.eta_seconds < 0 or arrival.freshness.stale:
                continue
            if best is None or arrival.eta_seconds < best[1].eta_seconds:
                best = (favorite, arrival)

    if best is None:
        return ShortcutResponse(
            found=False,
            label=label,
            generated_at=now,
            summary=f"No realtime arrivals right now for {label}.",
        )

    favorite, arrival = best
    walk = favorite.walk_minutes or 0
    leave_in = (arrival.eta_minutes or 0) - walk
    if leave_in <= 0:
        advice = "leave now" if leave_in > -2 else "you would miss it"
    else:
        advice = f"leave in {leave_in} min"
    return ShortcutResponse(
        found=True,
        label=label,
        generated_at=now,
        route_short_name=arrival.route_short_name,
        headsign=arrival.headsign,
        stop_id=favorite.stop_id,
        stop_name=favorite.stop_name,
        eta_minutes=arrival.eta_minutes,
        walk_minutes=walk,
        leave_in_minutes=leave_in,
        data_age_seconds=arrival.freshness.age_seconds,
        stale=arrival.freshness.stale,
        summary=(
            f"{arrival.route_short_name or 'Bus'} to {arrival.headsign or 'destination'} "
            f"in {arrival.eta_minutes} min at {favorite.stop_name}; {advice} "
            f"({walk} min walk). Data {arrival.freshness.age_seconds}s old."
        ),
    )


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")
