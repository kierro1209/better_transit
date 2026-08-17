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

from datetime import datetime, timedelta, timezone

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
    HealthResponse,
    RouteOut,
    ShortcutResponse,
    StopOut,
    VehicleDetailResponse,
    VehicleHistoryPoint,
    VehicleOut,
    VehiclesResponse,
)
from transit.config import AGENCY_KEY, settings
from transit.db import get_session
from transit.freshness import Freshness
from transit.models import FavoriteStop, Stop
from transit.web import STATIC_DIR

app = FastAPI(
    title="UCLA Transit Intelligence",
    description="Realtime arrivals and vehicle history for the Big Blue Bus routes around UCLA.",
    version="0.1.0",
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
