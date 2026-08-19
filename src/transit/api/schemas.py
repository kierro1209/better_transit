from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class FreshnessInfo(BaseModel):
    """Attached to every payload that contains realtime information."""

    observed_at: datetime | None
    age_seconds: int | None
    stale: bool


class RouteOut(BaseModel):
    route_id: str
    short_name: str | None
    long_name: str | None
    route_type: int | None
    color: str | None


class MapPoint(BaseModel):
    latitude: float
    longitude: float


class RouteMapResponse(BaseModel):
    route_id: str
    route_short_name: str | None
    paths: list[list[MapPoint]]
    stops: list[StopOut]


class StopOut(BaseModel):
    stop_id: str
    name: str
    latitude: float | None
    longitude: float | None


class ArrivalOut(BaseModel):
    route_short_name: str | None
    route_long_name: str | None
    headsign: str | None
    rt_trip_id: str
    scheduled_trip_id: str | None
    vehicle_id: str | None
    predicted_arrival: datetime | None
    eta_seconds: int | None
    eta_minutes: int | None
    delay_seconds: int | None
    schedule_relationship: str | None
    freshness: FreshnessInfo


class ArrivalsResponse(BaseModel):
    stop: StopOut
    generated_at: datetime
    feed_freshness: FreshnessInfo
    arrivals: list[ArrivalOut]


class VehicleOut(BaseModel):
    vehicle_id: str
    rt_trip_id: str | None
    scheduled_trip_id: str | None
    route_id: str | None
    route_short_name: str | None
    latitude: float | None
    longitude: float | None
    bearing: float | None
    speed: float | None
    stop_id: str | None
    stop_sequence: int | None
    current_status: str | None
    source: str
    freshness: FreshnessInfo


class VehiclesResponse(BaseModel):
    route_id: str
    route_short_name: str | None
    generated_at: datetime
    position_data_available: bool
    vehicles: list[VehicleOut]


class VehicleHistoryPoint(BaseModel):
    observed_at: datetime
    ingested_at: datetime
    latitude: float | None
    longitude: float | None
    stop_id: str | None
    stop_sequence: int | None
    rt_trip_id: str | None
    source: str


class VehicleDetailResponse(BaseModel):
    vehicle_id: str
    latest: VehicleOut | None
    observations: list[VehicleHistoryPoint]


class FeedHealth(BaseModel):
    feed_kind: str
    last_run_at: datetime | None
    ok: bool | None
    feed_timestamp: datetime | None
    feed_age_seconds: int | None
    stale: bool | None
    entity_count: int | None
    persisted_count: int | None
    request_latency_ms: int | None
    error: str | None


class HealthResponse(BaseModel):
    status: str
    database: str
    schedule_loaded: bool
    stop_count: int
    trip_count: int
    observation_count: int
    prediction_count: int
    feeds: list[FeedHealth]


class ShortcutResponse(BaseModel):
    """Flat, string-and-number payload so an iOS Shortcut can read fields without parsing."""

    found: bool
    label: str
    generated_at: datetime
    summary: str
    route_short_name: str | None = None
    headsign: str | None = None
    stop_id: str | None = None
    stop_name: str | None = None
    eta_minutes: int | None = None
    walk_minutes: int | None = None
    leave_in_minutes: int | None = None
    data_age_seconds: int | None = None
    stale: bool | None = None


class FavoriteOut(BaseModel):
    label: str
    stop_id: str
    stop_name: str
    walk_minutes: int | None


class NearbyStopOut(StopOut):
    distance_meters: int
    routes: list[str]


class NearbyStopsResponse(BaseModel):
    latitude: float
    longitude: float
    stops: list[NearbyStopOut]


class GeocodeResult(BaseModel):
    display_name: str
    latitude: float
    longitude: float


class PlanRequest(BaseModel):
    origin_stop_id: str
    destination_stop_id: str
    mode: str = "depart_now"
    arrive_by: datetime | None = None


class PlanOption(BaseModel):
    route_short_name: str | None
    headsign: str | None
    origin_stop: StopOut
    destination_stop: StopOut
    next_bus_at: datetime
    estimated_arrival_at: datetime
    travel_minutes: int
    wait_minutes: int
    realtime: bool


class PlanResponse(BaseModel):
    origin: StopOut
    destination: StopOut
    options: list[PlanOption]


class ScheduleOut(BaseModel):
    route_short_name: str | None
    route_long_name: str | None
    headsign: str | None
    trip_id: str
    stop_id: str
    scheduled_time: datetime
    scheduled_time_label: str


class ScheduleResponse(BaseModel):
    stop: StopOut
    service_date: date
    requested_time: str
    generated_at: datetime
    schedule: list[ScheduleOut]
