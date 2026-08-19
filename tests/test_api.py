from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from transit.api import main as api_main
from transit.api.main import app
from transit.db import get_session
from transit.models import ArrivalPrediction, FavoriteStop, IngestRun, VehicleObservation


@pytest.fixture
def client(session):
    """Point the app's session dependency at the rolled-back test transaction.

    Dependency overrides are how FastAPI keeps handlers testable: the handler asks for a
    Session and does not care where it comes from.
    """
    app.dependency_overrides[get_session] = lambda: session
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def agency(monkeypatch):
    monkeypatch.setattr("transit.api.main.AGENCY_KEY", "test")


def _prediction(session, *, stop_id, sequence, arrival, observed_at, trip="RT-1", vehicle="1822"):
    session.add(
        ArrivalPrediction(
            agency_key="test",
            rt_trip_id=trip,
            rt_route_id="R12",
            vehicle_id=vehicle,
            scheduled_trip_id="T1",
            route_id="4108",
            rt_stop_id=f"RT-{stop_id}",
            stop_id=stop_id,
            stop_sequence=sequence,
            observed_at=observed_at,
            ingested_at=observed_at,
            predicted_arrival=arrival,
        )
    )


def test_health_reports_degraded_without_ingestion(client, schedule):
    body = client.get("/health").json()
    assert body["database"] == "ok"
    assert body["schedule_loaded"] is True
    assert body["status"] == "degraded"  # schedule loaded but nothing ingested recently


def test_lifespan_does_not_start_poller_when_disabled(monkeypatch):
    monkeypatch.setenv("INGEST_IN_SERVER", "false")

    def fail_if_started(*args, **kwargs):
        raise AssertionError("poller should not start when ingestion is disabled")

    monkeypatch.setattr(api_main, "run_loop", fail_if_started)
    with TestClient(app):
        pass


def test_health_reports_ok_after_a_recent_run(client, session, schedule):
    now = datetime.now(timezone.utc)
    session.add(
        IngestRun(
            agency_key="test",
            feed_kind="trip_updates",
            feed_url="http://example.test/tripupdates.bin",
            requested_at=now,
            feed_timestamp=now - timedelta(seconds=20),
            feed_age_seconds=20,
            is_stale=False,
            ok=True,
            entity_count=2,
            persisted_count=5,
        )
    )
    session.flush()
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["feeds"][0]["stale"] is False


def test_routes_lists_the_schedule(client, schedule):
    routes = client.get("/routes").json()
    assert [r["short_name"] for r in routes] == ["R12"]


def test_unknown_stop_returns_404(client, schedule):
    response = client.get("/stops/NOPE/arrivals")
    assert response.status_code == 404
    assert "unknown stop" in response.json()["detail"]


def test_known_stop_with_no_realtime_data_returns_an_empty_list(client, schedule):
    body = client.get("/stops/S1/arrivals").json()
    assert body["arrivals"] == []
    assert body["feed_freshness"]["stale"] is True  # no data is not fresh data
    assert body["stop"]["name"] == "Gateway Plaza"


def test_arrivals_are_ordered_and_only_the_newest_prediction_per_trip_is_used(
    client, session, schedule
):
    now = datetime.now(timezone.utc)
    # Two successive predictions for the same trip: the older one said 10 minutes.
    _prediction(
        session,
        stop_id="S2",
        sequence=2,
        arrival=now + timedelta(minutes=10),
        observed_at=now - timedelta(minutes=5),
    )
    _prediction(
        session,
        stop_id="S2",
        sequence=2,
        arrival=now + timedelta(minutes=8),
        observed_at=now - timedelta(seconds=30),
    )
    # A different trip, arriving sooner.
    _prediction(
        session,
        stop_id="S2",
        sequence=2,
        arrival=now + timedelta(minutes=3),
        observed_at=now - timedelta(seconds=30),
        trip="RT-2",
        vehicle="1900",
    )
    session.flush()

    body = client.get("/stops/S2/arrivals").json()

    assert [a["rt_trip_id"] for a in body["arrivals"]] == ["RT-2", "RT-1"]
    assert [a["eta_minutes"] for a in body["arrivals"]] == [3, 8]
    assert body["feed_freshness"]["age_seconds"] <= 31


def test_arrivals_in_the_past_are_not_returned(client, session, schedule):
    now = datetime.now(timezone.utc)
    _prediction(
        session,
        stop_id="S3",
        sequence=3,
        arrival=now - timedelta(minutes=5),
        observed_at=now - timedelta(minutes=6),
    )
    session.flush()
    assert client.get("/stops/S3/arrivals").json()["arrivals"] == []


def test_stale_predictions_are_returned_but_flagged(client, session, schedule):
    now = datetime.now(timezone.utc)
    _prediction(
        session,
        stop_id="S4",
        sequence=4,
        arrival=now + timedelta(minutes=6),
        observed_at=now - timedelta(minutes=30),
    )
    session.flush()
    arrival = client.get("/stops/S4/arrivals").json()["arrivals"][0]
    assert arrival["freshness"]["stale"] is True
    assert arrival["freshness"]["age_seconds"] >= 1800


def test_route_vehicles_accepts_a_short_name_and_reports_missing_positions(
    client, session, schedule
):
    now = datetime.now(timezone.utc)
    session.add(
        VehicleObservation(
            agency_key="test",
            vehicle_id="1822",
            rt_trip_id="RT-1",
            rt_route_id="R12",
            scheduled_trip_id="T1",
            route_id="4108",
            observed_at=now - timedelta(seconds=20),
            ingested_at=now,
            stop_id="S2",
            stop_sequence=2,
            source="trip_updates",
        )
    )
    session.flush()

    body = client.get("/routes/R12/vehicles").json()

    assert body["route_id"] == "4108"
    assert body["position_data_available"] is False
    assert body["vehicles"][0]["vehicle_id"] == "1822"


def test_unknown_route_returns_404(client, schedule):
    assert client.get("/routes/R99/vehicles").status_code == 404


def test_vehicle_history_can_be_windowed(client, session, schedule):
    base = datetime.now(timezone.utc) - timedelta(hours=2)
    for minute in range(4):
        session.add(
            VehicleObservation(
                agency_key="test",
                vehicle_id="1822",
                rt_route_id="R12",
                route_id="4108",
                observed_at=base + timedelta(minutes=minute * 20),
                ingested_at=datetime.now(timezone.utc),
                source="trip_updates",
            )
        )
    session.flush()

    everything = client.get("/vehicles/1822").json()
    assert len(everything["observations"]) == 4

    window = client.get(
        "/vehicles/1822",
        params={
            "since": (base + timedelta(minutes=10)).isoformat(),
            "until": (base + timedelta(minutes=45)).isoformat(),
        },
    ).json()
    assert len(window["observations"]) == 2


def test_unknown_vehicle_returns_404(client, schedule):
    assert client.get("/vehicles/nope").status_code == 404


def test_shortcut_returns_a_deterministic_recommendation(client, session, schedule):
    now = datetime.now(timezone.utc)
    session.add(FavoriteStop(label="UCLA", agency_key="test", stop_id="S1", walk_minutes=5))
    _prediction(
        session,
        stop_id="S1",
        sequence=1,
        arrival=now + timedelta(minutes=12),
        observed_at=now - timedelta(seconds=20),
    )
    session.flush()

    body = client.get("/shortcut/next", params={"label": "ucla"}).json()

    assert body["found"] is True
    assert (body["eta_minutes"], body["walk_minutes"], body["leave_in_minutes"]) == (12, 5, 7)
    assert "leave in 7 min" in body["summary"]


def test_shortcut_ignores_stale_predictions(client, session, schedule):
    now = datetime.now(timezone.utc)
    session.add(FavoriteStop(label="UCLA", agency_key="test", stop_id="S1", walk_minutes=5))
    _prediction(
        session,
        stop_id="S1",
        sequence=1,
        arrival=now + timedelta(minutes=12),
        observed_at=now - timedelta(hours=1),
    )
    session.flush()

    body = client.get("/shortcut/next", params={"label": "UCLA"}).json()
    assert body["found"] is False


def test_shortcut_unknown_label_returns_404(client, schedule):
    assert client.get("/shortcut/next", params={"label": "MARS"}).status_code == 404
