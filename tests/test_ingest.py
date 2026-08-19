"""Ingestion against a synthetic feed.

The HTTP call is replaced; everything else - decoding, matching, stop resolution, the
ON CONFLICT insert - is the real code path against the real database.
"""

import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from google.transit import gtfs_realtime_pb2
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from transit import ingest as ingest_module
from transit.gtfs.realtime import FetchResult
from transit.ingest import ingest_trip_updates, ingest_vehicle_positions
from transit.models import ArrivalPrediction, IngestRun, VehicleObservation


def build_trip_updates(feed_timestamp: datetime, observed_at: datetime) -> bytes:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.timestamp = int(feed_timestamp.timestamp())

    entity = message.entity.add()
    entity.id = "1"
    update = entity.trip_update
    update.trip.trip_id = "RT-1"
    update.trip.route_id = "R12"
    update.trip.start_time = "16:00:00"
    update.vehicle.id = "1822"
    update.timestamp = int(observed_at.timestamp())
    for sequence in range(1, 5):
        stop = update.stop_time_update.add()
        stop.stop_id = f"RT-STOP-{sequence}"  # deliberately not a static stop id
        stop.stop_sequence = sequence
        stop.arrival.time = int(observed_at.timestamp()) + sequence * 300

    unmatchable = message.entity.add()
    unmatchable.id = "2"
    unmatchable.trip_update.trip.trip_id = "RT-2"
    unmatchable.trip_update.trip.route_id = "R99"
    unmatchable.trip_update.trip.start_time = "16:00:00"
    stop = unmatchable.trip_update.stop_time_update.add()
    stop.stop_id = "RT-STOP-9"
    stop.stop_sequence = 1
    stop.arrival.time = int(observed_at.timestamp()) + 60

    return message.SerializeToString()


@pytest.fixture
def fake_feed(monkeypatch):
    """Swap the HTTP layer for a function that returns bytes we control."""

    def install(body: bytes, status_code: int = 200, error: str | None = None):
        def fake_fetch(url: str, timeout: float = 20.0) -> FetchResult:
            return FetchResult(
                url=url,
                requested_at=datetime.now(timezone.utc),
                latency_ms=7,
                status_code=status_code,
                body=body,
                error=error,
            )

        monkeypatch.setattr(ingest_module, "fetch_feed", fake_fetch)

    return install


def test_trip_updates_are_normalized_and_persisted(session, schedule, fake_feed):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    fake_feed(build_trip_updates(now, now))

    result = ingest_trip_updates(session, agency_key="test")

    assert result.ok
    assert result.entity_count == 2
    assert result.unmatched_trips == 1  # route R99 is not in the schedule
    assert result.persisted_count == result.usable_count
    assert result.is_stale is False

    predictions = session.scalars(select(ArrivalPrediction)).all()
    assert len(predictions) == 5
    matched = [p for p in predictions if p.rt_trip_id == "RT-1"]
    # The realtime stop id is preserved and the static stop is resolved via stop_sequence.
    assert {p.rt_stop_id for p in matched} == {f"RT-STOP-{i}" for i in range(1, 5)}
    assert {p.stop_id for p in matched} == {"S1", "S2", "S3", "S4"}
    assert all(p.scheduled_trip_id == "T1" and p.route_id == "4108" for p in matched)
    # An unmatched trip still records history; it just cannot name a static stop.
    unmatched = [p for p in predictions if p.rt_trip_id == "RT-2"]
    assert unmatched[0].stop_id is None and unmatched[0].rt_stop_id == "RT-STOP-9"


def test_thread_safe_loop_runs_a_cycle_and_stops(engine, fake_feed, monkeypatch):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    fake_feed(build_trip_updates(now, now))

    cycle_finished = threading.Event()
    original_run_once = ingest_module.run_once

    def run_once_and_signal():
        result = original_run_once()
        cycle_finished.set()
        return result

    @contextmanager
    def test_session_scope():
        session = sessionmaker(bind=engine, expire_on_commit=False, future=True)()
        try:
            yield session
        finally:
            session.rollback()
            session.close()

    monkeypatch.setattr(ingest_module, "run_once", run_once_and_signal)
    monkeypatch.setattr(ingest_module, "session_scope", test_session_scope)
    stop_event = threading.Event()
    thread = threading.Thread(target=ingest_module.run_loop, args=(60, stop_event))
    thread.start()

    assert cycle_finished.wait(timeout=5)
    stop_event.set()
    thread.join(timeout=2)

    assert not thread.is_alive()


def test_trip_update_produces_a_positionless_vehicle_observation(session, schedule, fake_feed):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    fake_feed(build_trip_updates(now, now))

    ingest_trip_updates(session, agency_key="test")

    observation = session.scalars(select(VehicleObservation)).one()
    assert observation.vehicle_id == "1822"
    assert observation.source == "trip_updates"
    assert (observation.latitude, observation.longitude) == (None, None)
    assert observation.stop_id == "S1"  # next stop, resolved to the schedule
    assert observation.observed_at == now
    assert observation.ingested_at >= observation.observed_at


def test_re_polling_the_same_feed_persists_nothing_new(session, schedule, fake_feed):
    """The feed is a snapshot: the same reading arrives every poll and must not duplicate."""
    now = datetime.now(timezone.utc).replace(microsecond=0)
    fake_feed(build_trip_updates(now, now))

    first = ingest_trip_updates(session, agency_key="test")
    second = ingest_trip_updates(session, agency_key="test")

    assert second.persisted_count == 0
    assert second.duplicate_count == first.usable_count
    assert session.scalar(select(func.count()).select_from(ArrivalPrediction)) == 5


def test_a_new_reading_of_the_same_trip_is_appended_not_overwritten(session, schedule, fake_feed):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    fake_feed(build_trip_updates(now, now))
    ingest_trip_updates(session, agency_key="test")

    later = now + timedelta(seconds=30)
    fake_feed(build_trip_updates(later, later))
    ingest_trip_updates(session, agency_key="test")

    rows = session.scalars(
        select(ArrivalPrediction).where(
            ArrivalPrediction.rt_trip_id == "RT-1", ArrivalPrediction.stop_sequence == 1
        )
    ).all()
    assert len(rows) == 2  # both predictions survive - this is the ML training signal
    assert {r.observed_at for r in rows} == {now, later}


def test_a_frozen_feed_is_recorded_as_stale(session, schedule, fake_feed):
    frozen = datetime.now(timezone.utc) - timedelta(days=400)
    fake_feed(build_trip_updates(frozen, frozen))

    result = ingest_trip_updates(session, agency_key="test")

    assert result.is_stale is True
    assert result.feed_age_seconds > 400 * 86_400 - 60
    run = session.scalars(select(IngestRun)).one()
    assert run.is_stale is True and run.ok is True


def test_a_failed_request_is_recorded_and_does_not_raise(session, schedule, fake_feed):
    fake_feed(b"", status_code=503, error="HTTP 503")

    result = ingest_trip_updates(session, agency_key="test")

    assert result.ok is False and result.error == "HTTP 503"
    run = session.scalars(select(IngestRun)).one()
    assert (run.ok, run.http_status, run.error) == (False, 503, "HTTP 503")


def test_malformed_bytes_are_recorded_and_do_not_raise(session, schedule, fake_feed):
    fake_feed(b"definitely not protobuf" * 20)

    result = ingest_trip_updates(session, agency_key="test")

    assert result.ok is False
    assert "protobuf" in result.error


def test_vehicle_positions_without_coordinates_are_rejected(session, schedule, fake_feed):
    """Big Blue Bus's live behaviour: entities with no position must not become fake rows."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.timestamp = int(datetime.now(timezone.utc).timestamp())
    entity = message.entity.add()
    entity.id = "v1"
    entity.vehicle.vehicle.id = "1822"
    fake_feed(message.SerializeToString())

    result = ingest_vehicle_positions(session, agency_key="test")

    assert (result.usable_count, result.persisted_count, result.rejected_count) == (0, 0, 1)
    assert session.scalar(select(func.count()).select_from(VehicleObservation)) == 0
