"""Decoding tests built from protobuf we construct ourselves.

Building a FeedMessage in the test and serializing it is the only honest way to test the
decoder: it exercises the same generated classes the agency's bytes go through, and lets us
create the malformed shapes a live feed only produces occasionally.
"""

from datetime import datetime, timezone

import pytest
from google.transit import gtfs_realtime_pb2

from transit.gtfs.realtime import decode_feed


def _feed(timestamp: int | None = 1_700_000_000):
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    if timestamp is not None:
        message.header.timestamp = timestamp
    return message


def test_decodes_a_trip_update():
    message = _feed()
    entity = message.entity.add()
    entity.id = "1"
    update = entity.trip_update
    update.trip.trip_id = "553010"
    update.trip.route_id = "R12"
    update.trip.start_time = "16:05:00"
    update.trip.start_date = "20250309"
    update.trip.direction_id = 1
    update.vehicle.id = "1822"
    update.timestamp = 1_700_000_100
    stop = update.stop_time_update.add()
    stop.stop_id = "421"
    stop.stop_sequence = 3
    stop.arrival.time = 1_700_000_400
    stop.arrival.delay = 65

    decoded = decode_feed(message.SerializeToString())

    assert decoded.entity_count == 1
    assert decoded.feed_timestamp == datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
    trip = decoded.trip_updates[0]
    assert (trip.rt_trip_id, trip.rt_route_id, trip.vehicle_id) == ("553010", "R12", "1822")
    assert trip.direction_id == 1
    assert trip.schedule_relationship == "SCHEDULED"
    prediction = trip.stop_time_updates[0]
    assert prediction.stop_sequence == 3
    assert prediction.delay_seconds == 65
    assert prediction.departure_time is None


def test_missing_optional_fields_do_not_crash():
    """No vehicle, no timestamp, no stop_sequence, no direction: all become None."""
    message = _feed(timestamp=None)
    entity = message.entity.add()
    entity.id = "1"
    entity.trip_update.trip.trip_id = "553010"
    stop = entity.trip_update.stop_time_update.add()
    stop.stop_id = "421"
    stop.departure.time = 1_700_000_400

    decoded = decode_feed(message.SerializeToString())

    trip = decoded.trip_updates[0]
    assert decoded.feed_timestamp is None
    assert (trip.vehicle_id, trip.timestamp, trip.direction_id, trip.start_time) == (
        None,
        None,
        None,
        None,
    )
    assert trip.stop_time_updates[0].stop_sequence is None
    assert trip.stop_time_updates[0].arrival_time is None


def test_useless_entities_are_counted_not_dropped_silently():
    message = _feed()
    no_trip_id = message.entity.add()
    no_trip_id.id = "1"
    no_trip_id.trip_update.trip.route_id = "R12"

    no_times = message.entity.add()
    no_times.id = "2"
    no_times.trip_update.trip.trip_id = "553010"
    no_times.trip_update.stop_time_update.add().stop_id = "421"

    decoded = decode_feed(message.SerializeToString())

    assert decoded.entity_count == 2
    assert len(decoded.rejected) == 2
    assert decoded.trip_updates[0].stop_time_updates == []


def test_vehicle_position_without_coordinates_still_decodes():
    message = _feed()
    entity = message.entity.add()
    entity.id = "v1"
    entity.vehicle.vehicle.id = "1822"
    entity.vehicle.trip.route_id = "R12"
    entity.vehicle.current_status = 1

    decoded = decode_feed(message.SerializeToString())

    position = decoded.vehicle_positions[0]
    assert (position.latitude, position.longitude, position.speed) == (None, None, None)
    assert position.current_status == "STOPPED_AT"


def test_vehicle_position_with_coordinates():
    message = _feed()
    entity = message.entity.add()
    entity.id = "v1"
    entity.vehicle.vehicle.id = "1822"
    entity.vehicle.position.latitude = 34.0694
    entity.vehicle.position.longitude = -118.4448
    entity.vehicle.position.bearing = 180.0
    entity.vehicle.current_stop_sequence = 4

    position = decode_feed(message.SerializeToString()).vehicle_positions[0]

    assert position.latitude == pytest.approx(34.0694, abs=1e-4)
    assert position.stop_sequence == 4


def test_garbage_bytes_raise_a_useful_error():
    with pytest.raises(ValueError, match="not valid GTFS-Realtime protobuf"):
        decode_feed(b"this is not protobuf, it is a sentence" * 10)


def test_empty_feed_is_valid_and_empty():
    decoded = decode_feed(_feed().SerializeToString())
    assert (decoded.entity_count, decoded.trip_updates, decoded.rejected) == (0, [], [])
