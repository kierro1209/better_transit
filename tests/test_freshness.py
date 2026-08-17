from datetime import datetime, timedelta, timezone

from transit.freshness import Freshness, age_seconds, is_stale

NOW = datetime(2025, 3, 9, 12, 0, tzinfo=timezone.utc)


def test_recent_feed_is_fresh():
    assert not is_stale(NOW - timedelta(seconds=45), NOW, stale_after_seconds=180)


def test_old_feed_is_stale():
    assert is_stale(NOW - timedelta(minutes=10), NOW, stale_after_seconds=180)


def test_frozen_feed_is_stale():
    """The real case: Big Blue Bus's VehiclePositions header stuck in April 2025."""
    frozen = datetime(2025, 4, 9, 15, 15, 21, tzinfo=timezone.utc)
    assert is_stale(frozen, frozen + timedelta(days=400))


def test_missing_timestamp_counts_as_stale():
    """Absence of a timestamp is not evidence of freshness."""
    assert is_stale(None, NOW)
    assert age_seconds(None, NOW) is None


def test_freshness_object_carries_all_three_facts():
    info = Freshness.of(NOW - timedelta(seconds=61), NOW, stale_after_seconds=60)
    assert (info.age_seconds, info.stale) == (61, True)
