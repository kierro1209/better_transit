"""Fetch and decode GTFS-Realtime protobuf feeds.

What the protobuf actually is: the agency serializes a ``FeedMessage`` — a tree of
length-prefixed, tag-numbered binary fields defined by gtfs-realtime.proto — and serves the
bytes over plain HTTP. ``gtfs-realtime-bindings`` ships the Python classes generated from
that .proto, so ``FeedMessage().ParseFromString(body)`` walks the bytes, matches field tags
to the generated schema and fills in Python attributes. There is no JSON and no schema
negotiation: if the bytes are truncated or not protobuf, parsing raises.

Two consequences worth remembering:
- Unset scalar fields are indistinguishable from zero unless you call ``HasField``. This
  matters for ``timestamp``: a missing timestamp reads as 0, not None.
- The feed is a snapshot, not a stream. Every poll returns the agency's whole current view,
  which is why de-duplication is our job, not theirs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2


@dataclass
class FetchResult:
    url: str
    requested_at: datetime
    latency_ms: int
    status_code: int | None
    body: bytes | None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.status_code == 200 and bool(self.body)


def fetch_feed(url: str, timeout: float = 20.0) -> FetchResult:
    """One HTTP GET, never raising: transport failures become a FetchResult with an error.

    Under the hood httpx resolves the hostname, opens a TCP connection, completes the TLS
    handshake, writes ``GET /tripupdates.bin HTTP/1.1`` plus headers, and reads the response
    body into memory. The latency recorded here therefore includes DNS, TLS and transfer,
    not just the agency's server time.
    """
    requested_at = datetime.now(timezone.utc)
    start = time.perf_counter()
    try:
        response = httpx.get(url, timeout=timeout, follow_redirects=True)
    except httpx.HTTPError as exc:
        return FetchResult(
            url=url,
            requested_at=requested_at,
            latency_ms=int((time.perf_counter() - start) * 1000),
            status_code=None,
            body=None,
            error=f"{type(exc).__name__}: {exc}",
        )
    return FetchResult(
        url=url,
        requested_at=requested_at,
        latency_ms=int((time.perf_counter() - start) * 1000),
        status_code=response.status_code,
        body=response.content,
        error=None if response.status_code == 200 else f"HTTP {response.status_code}",
    )


@dataclass
class StopTimePrediction:
    stop_id: str
    stop_sequence: int | None
    arrival_time: datetime | None
    departure_time: datetime | None
    delay_seconds: int | None
    schedule_relationship: str | None


@dataclass
class DecodedTripUpdate:
    rt_trip_id: str | None
    rt_route_id: str | None
    direction_id: int | None
    start_date: str | None
    start_time: str | None
    vehicle_id: str | None
    timestamp: datetime | None
    schedule_relationship: str | None
    stop_time_updates: list[StopTimePrediction] = field(default_factory=list)


@dataclass
class DecodedVehiclePosition:
    vehicle_id: str | None
    rt_trip_id: str | None
    rt_route_id: str | None
    timestamp: datetime | None
    latitude: float | None
    longitude: float | None
    bearing: float | None
    speed: float | None
    stop_id: str | None
    stop_sequence: int | None
    current_status: str | None


@dataclass
class DecodedFeed:
    feed_timestamp: datetime | None
    entity_count: int
    trip_updates: list[DecodedTripUpdate] = field(default_factory=list)
    vehicle_positions: list[DecodedVehiclePosition] = field(default_factory=list)
    rejected: list[str] = field(default_factory=list)


def _ts(seconds: int | None) -> datetime | None:
    if not seconds:
        return None
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


_SCHEDULE_RELATIONSHIP = {
    0: "SCHEDULED",
    1: "SKIPPED",
    2: "NO_DATA",
    3: "UNSCHEDULED",
}
_TRIP_SCHEDULE_RELATIONSHIP = {
    0: "SCHEDULED",
    1: "ADDED",
    2: "UNSCHEDULED",
    3: "CANCELED",
    5: "REPLACEMENT",
    6: "DUPLICATED",
    7: "DELETED",
}
_VEHICLE_STATUS = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}


def decode_feed(body: bytes) -> DecodedFeed:
    """Bytes -> plain Python dataclasses.

    Anything unusable is counted in ``rejected`` rather than dropped silently: an entity
    with no trip id, or a stop_time_update with neither an arrival nor a departure, tells us
    something about the agency's data quality and we want that number in the ingest log.
    """
    message = gtfs_realtime_pb2.FeedMessage()
    try:
        message.ParseFromString(body)
    except DecodeError as exc:
        raise ValueError(f"feed is not valid GTFS-Realtime protobuf: {exc}") from exc

    decoded = DecodedFeed(
        feed_timestamp=_ts(message.header.timestamp if message.header.timestamp else None),
        entity_count=len(message.entity),
    )

    for entity in message.entity:
        if entity.HasField("trip_update"):
            update = entity.trip_update
            trip_id = update.trip.trip_id or None
            if trip_id is None:
                decoded.rejected.append(f"trip_update {entity.id}: no trip id")
                continue
            predictions: list[StopTimePrediction] = []
            for stu in update.stop_time_update:
                arrival = _ts(stu.arrival.time) if stu.HasField("arrival") else None
                departure = _ts(stu.departure.time) if stu.HasField("departure") else None
                if arrival is None and departure is None:
                    decoded.rejected.append(
                        f"trip_update {trip_id} stop {stu.stop_id}: no arrival or departure"
                    )
                    continue
                delay = None
                if stu.HasField("arrival") and stu.arrival.HasField("delay"):
                    delay = stu.arrival.delay
                elif stu.HasField("departure") and stu.departure.HasField("delay"):
                    delay = stu.departure.delay
                predictions.append(
                    StopTimePrediction(
                        stop_id=stu.stop_id,
                        stop_sequence=stu.stop_sequence if stu.HasField("stop_sequence") else None,
                        arrival_time=arrival,
                        departure_time=departure,
                        delay_seconds=delay,
                        schedule_relationship=_SCHEDULE_RELATIONSHIP.get(stu.schedule_relationship),
                    )
                )
            decoded.trip_updates.append(
                DecodedTripUpdate(
                    rt_trip_id=trip_id,
                    rt_route_id=update.trip.route_id or None,
                    direction_id=(
                        update.trip.direction_id if update.trip.HasField("direction_id") else None
                    ),
                    start_date=update.trip.start_date or None,
                    start_time=update.trip.start_time or None,
                    vehicle_id=update.vehicle.id or None,
                    timestamp=_ts(update.timestamp if update.timestamp else None),
                    schedule_relationship=_TRIP_SCHEDULE_RELATIONSHIP.get(
                        update.trip.schedule_relationship
                    ),
                    stop_time_updates=predictions,
                )
            )
        elif entity.HasField("vehicle"):
            vehicle = entity.vehicle
            vehicle_id = vehicle.vehicle.id or entity.id or None
            if vehicle_id is None:
                decoded.rejected.append(f"vehicle {entity.id}: no vehicle id")
                continue
            position = vehicle.position if vehicle.HasField("position") else None
            decoded.vehicle_positions.append(
                DecodedVehiclePosition(
                    vehicle_id=vehicle_id,
                    rt_trip_id=vehicle.trip.trip_id or None,
                    rt_route_id=vehicle.trip.route_id or None,
                    timestamp=_ts(vehicle.timestamp if vehicle.timestamp else None),
                    latitude=position.latitude if position else None,
                    longitude=position.longitude if position else None,
                    bearing=position.bearing if position and position.HasField("bearing") else None,
                    speed=position.speed if position and position.HasField("speed") else None,
                    stop_id=vehicle.stop_id or None,
                    stop_sequence=(
                        vehicle.current_stop_sequence
                        if vehicle.HasField("current_stop_sequence")
                        else None
                    ),
                    current_status=_VEHICLE_STATUS.get(vehicle.current_status),
                )
            )

    return decoded
