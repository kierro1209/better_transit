"""Polling ingestion.

One cycle per feed:

    request feed -> record request metadata -> decode protobuf -> validate fields
    -> normalize -> match to the schedule -> persist (append-only) -> record counts

Every cycle writes an ``ingest_runs`` row, successful or not. That table is the only way to
answer questions like "was the feed stale at 4 PM?" or "how many observations did we lose
to malformed records?" after the fact.
"""

from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from transit.config import AGENCY_KEY, AGENCY_TIMEZONE, settings
from transit.db import session_scope
from transit.freshness import age_seconds, is_stale
from transit.gtfs.realtime import DecodedFeed, decode_feed, fetch_feed
from transit.matching import TripMatcher, resolve_service_date, service_date_now
from transit.models import ArrivalPrediction, IngestRun, VehicleObservation

log = logging.getLogger("transit.ingest")


@dataclass
class CycleResult:
    feed_kind: str
    ingest_run_id: int | None
    ok: bool
    entity_count: int
    usable_count: int
    rejected_count: int
    persisted_count: int
    duplicate_count: int
    unmatched_trips: int
    is_stale: bool | None
    feed_age_seconds: int | None
    error: str | None = None


def _insert_ignoring_duplicates(session: Session, model, rows: list[dict]) -> int:
    """Append rows, letting Postgres drop exact re-observations.

    The unique constraints encode what "the same observation" means. ON CONFLICT DO NOTHING
    pushes that decision into the database, so two ingesters racing on the same feed cannot
    produce duplicate history.
    """
    if not rows:
        return 0
    # RETURNING is what makes the count exact: with insertmanyvalues batching, rowcount can
    # come back as -1, whereas RETURNING yields one row per row actually inserted.
    statement = pg_insert(model).values(rows).on_conflict_do_nothing().returning(model.id)
    return len(session.execute(statement).scalars().all())


def ingest_trip_updates(session: Session, agency_key: str = AGENCY_KEY) -> CycleResult:
    fetched = fetch_feed(settings.bbb_trip_updates_url, timeout=settings.http_timeout_seconds)
    run = IngestRun(
        agency_key=agency_key,
        feed_kind="trip_updates",
        feed_url=fetched.url,
        requested_at=fetched.requested_at,
        request_latency_ms=fetched.latency_ms,
        http_status=fetched.status_code,
        response_bytes=len(fetched.body) if fetched.body else 0,
        ok=False,
    )
    session.add(run)
    session.flush()

    if not fetched.ok:
        run.error = fetched.error or "empty response"
        run.completed_at = datetime.now(timezone.utc)
        return CycleResult("trip_updates", run.id, False, 0, 0, 0, 0, 0, 0, None, None, run.error)

    try:
        decoded = decode_feed(fetched.body)
    except ValueError as exc:
        run.error = str(exc)
        run.completed_at = datetime.now(timezone.utc)
        return CycleResult("trip_updates", run.id, False, 0, 0, 0, 0, 0, 0, None, None, run.error)

    ingested_at = datetime.now(timezone.utc)
    age = _feed_age(decoded, ingested_at)
    service_date = service_date_now(AGENCY_TIMEZONE)
    matcher = TripMatcher(session, agency_key, service_date)

    predictions: list[dict] = []
    observations: list[dict] = []
    unmatched = 0

    # Pass 1: join every realtime trip to the schedule, then load the stop sequences for the
    # trips we matched in a single query. Pass 2 can then resolve each realtime stop id to a
    # static stop without touching the database again.
    matches = [
        (update, matcher.match(update.rt_route_id, update.start_time, update.direction_id))
        for update in decoded.trip_updates
    ]
    matcher.load_stop_sequences(m.scheduled_trip_id for _, m in matches if m)

    for update, matched in matches:
        if matched is None:
            unmatched += 1
        trip_service_date = resolve_service_date(update.start_date, AGENCY_TIMEZONE)
        observed_at = update.timestamp or decoded.feed_timestamp or ingested_at

        if update.vehicle_id:
            # A TripUpdate proves the vehicle was working this trip at this instant, even
            # though it carries no coordinates. Stored as an observation with a null
            # position and source="trip_updates" so it is never confused with a GPS fix.
            next_stop = min(
                (s for s in update.stop_time_updates if s.stop_sequence is not None),
                key=lambda s: s.stop_sequence,
                default=None,
            )
            observations.append(
                {
                    "ingest_run_id": run.id,
                    "agency_key": agency_key,
                    "vehicle_id": update.vehicle_id,
                    "rt_trip_id": update.rt_trip_id,
                    "rt_route_id": update.rt_route_id,
                    "scheduled_trip_id": matched.scheduled_trip_id if matched else None,
                    "route_id": matched.route_id if matched else None,
                    "observed_at": observed_at,
                    "ingested_at": ingested_at,
                    "latitude": None,
                    "longitude": None,
                    "bearing": None,
                    "speed": None,
                    "rt_stop_id": next_stop.stop_id if next_stop else None,
                    "stop_id": (
                        matcher.static_stop_id(
                            matched.scheduled_trip_id if matched else None,
                            next_stop.stop_sequence if next_stop else None,
                        )
                    ),
                    "stop_sequence": next_stop.stop_sequence if next_stop else None,
                    "current_status": None,
                    "source": "trip_updates",
                }
            )

        for stop_update in update.stop_time_updates:
            predictions.append(
                {
                    "ingest_run_id": run.id,
                    "agency_key": agency_key,
                    "rt_trip_id": update.rt_trip_id,
                    "rt_route_id": update.rt_route_id,
                    "vehicle_id": update.vehicle_id,
                    "scheduled_trip_id": matched.scheduled_trip_id if matched else None,
                    "route_id": matched.route_id if matched else None,
                    "service_date": trip_service_date,
                    "trip_start_seconds": matched.start_seconds if matched else None,
                    "rt_stop_id": stop_update.stop_id,
                    "stop_id": matcher.static_stop_id(
                        matched.scheduled_trip_id if matched else None,
                        stop_update.stop_sequence,
                    ),
                    "stop_sequence": stop_update.stop_sequence,
                    "observed_at": observed_at,
                    "ingested_at": ingested_at,
                    "predicted_arrival": stop_update.arrival_time,
                    "predicted_departure": stop_update.departure_time,
                    "delay_seconds": stop_update.delay_seconds,
                    "schedule_relationship": stop_update.schedule_relationship,
                }
            )

    usable = len(predictions) + len(observations)
    persisted = _insert_ignoring_duplicates(session, ArrivalPrediction, predictions)
    persisted += _insert_ignoring_duplicates(session, VehicleObservation, observations)

    run.feed_timestamp = decoded.feed_timestamp
    run.feed_age_seconds = age
    run.is_stale = is_stale(decoded.feed_timestamp, ingested_at, settings.feed_stale_after_seconds)
    run.entity_count = decoded.entity_count
    run.usable_count = usable
    run.rejected_count = len(decoded.rejected)
    run.persisted_count = persisted
    run.completed_at = datetime.now(timezone.utc)
    run.ok = True

    return CycleResult(
        feed_kind="trip_updates",
        ingest_run_id=run.id,
        ok=True,
        entity_count=decoded.entity_count,
        usable_count=usable,
        rejected_count=len(decoded.rejected),
        persisted_count=persisted,
        duplicate_count=usable - persisted,
        unmatched_trips=unmatched,
        is_stale=run.is_stale,
        feed_age_seconds=age,
    )


def ingest_vehicle_positions(session: Session, agency_key: str = AGENCY_KEY) -> CycleResult:
    fetched = fetch_feed(settings.bbb_vehicle_positions_url, timeout=settings.http_timeout_seconds)
    run = IngestRun(
        agency_key=agency_key,
        feed_kind="vehicle_positions",
        feed_url=fetched.url,
        requested_at=fetched.requested_at,
        request_latency_ms=fetched.latency_ms,
        http_status=fetched.status_code,
        response_bytes=len(fetched.body) if fetched.body else 0,
        ok=False,
    )
    session.add(run)
    session.flush()

    if not fetched.ok:
        run.error = fetched.error or "empty response"
        run.completed_at = datetime.now(timezone.utc)
        return CycleResult(
            "vehicle_positions", run.id, False, 0, 0, 0, 0, 0, 0, None, None, run.error
        )

    try:
        decoded = decode_feed(fetched.body)
    except ValueError as exc:
        run.error = str(exc)
        run.completed_at = datetime.now(timezone.utc)
        return CycleResult(
            "vehicle_positions", run.id, False, 0, 0, 0, 0, 0, 0, None, None, run.error
        )

    ingested_at = datetime.now(timezone.utc)
    age = _feed_age(decoded, ingested_at)
    matcher = TripMatcher(session, agency_key, service_date_now(AGENCY_TIMEZONE))

    rows: list[dict] = []
    rejected = list(decoded.rejected)
    for position in decoded.vehicle_positions:
        if position.latitude is None or position.longitude is None:
            rejected.append(f"vehicle {position.vehicle_id}: no coordinates")
            continue
        matched = matcher.match(position.rt_route_id, None)
        rows.append(
            {
                "ingest_run_id": run.id,
                "agency_key": agency_key,
                "vehicle_id": position.vehicle_id,
                "rt_trip_id": position.rt_trip_id,
                "rt_route_id": position.rt_route_id,
                "scheduled_trip_id": matched.scheduled_trip_id if matched else None,
                "route_id": matched.route_id if matched else None,
                "observed_at": position.timestamp or decoded.feed_timestamp or ingested_at,
                "ingested_at": ingested_at,
                "latitude": position.latitude,
                "longitude": position.longitude,
                "bearing": position.bearing,
                "speed": position.speed,
                "rt_stop_id": position.stop_id,
                "stop_id": matcher.static_stop_id(
                    matched.scheduled_trip_id if matched else None, position.stop_sequence
                ),
                "stop_sequence": position.stop_sequence,
                "current_status": position.current_status,
                "source": "vehicle_positions",
            }
        )

    persisted = _insert_ignoring_duplicates(session, VehicleObservation, rows)

    run.feed_timestamp = decoded.feed_timestamp
    run.feed_age_seconds = age
    run.is_stale = is_stale(decoded.feed_timestamp, ingested_at, settings.feed_stale_after_seconds)
    run.entity_count = decoded.entity_count
    run.usable_count = len(rows)
    run.rejected_count = len(rejected)
    run.persisted_count = persisted
    run.completed_at = datetime.now(timezone.utc)
    run.ok = True

    return CycleResult(
        feed_kind="vehicle_positions",
        ingest_run_id=run.id,
        ok=True,
        entity_count=decoded.entity_count,
        usable_count=len(rows),
        rejected_count=len(rejected),
        persisted_count=persisted,
        duplicate_count=len(rows) - persisted,
        unmatched_trips=0,
        is_stale=run.is_stale,
        feed_age_seconds=age,
    )


def _feed_age(decoded: DecodedFeed, now: datetime) -> int | None:
    return age_seconds(decoded.feed_timestamp, now)


def run_once() -> list[CycleResult]:
    results = []
    for ingest in (ingest_trip_updates, ingest_vehicle_positions):
        with session_scope() as session:
            result = ingest(session)
        results.append(result)
        log.info(
            "ingest cycle complete",
            extra={"fields": result.__dict__},
        )
    return results


def run_forever(interval_seconds: int | None = None) -> None:
    interval = interval_seconds or settings.poll_interval_seconds
    stopping = {"flag": False}

    def handle_signal(signum, _frame):
        log.info("shutdown requested", extra={"fields": {"signal": signum}})
        stopping["flag"] = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    log.info("ingestion loop started", extra={"fields": {"interval_seconds": interval}})
    while not stopping["flag"]:
        started = time.monotonic()
        try:
            run_once()
        except Exception:
            # A single bad cycle must not kill a process that is supposed to run for days.
            log.exception("ingest cycle failed")
        # Sleep the remainder of the interval so the poll rate stays constant regardless of
        # how long the cycle took, and wake up often enough to notice a shutdown signal.
        deadline = started + interval
        while not stopping["flag"] and time.monotonic() < deadline:
            time.sleep(0.5)
    log.info("ingestion loop stopped")
