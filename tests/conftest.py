"""Database fixtures.

These tests run against a real Postgres, not SQLite or a mock, because the behaviour under
test *is* Postgres behaviour: ON CONFLICT DO NOTHING and DISTINCT ON have no SQLite
equivalent, and a mock would only assert that we call the functions we call.

Each test runs inside a transaction that is rolled back afterwards, so the tests share one
schema without sharing data. If no Postgres is reachable the whole module is skipped rather
than failing, so `pytest` still works on a machine without Docker running.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from transit.config import AGENCY_TIMEZONE, settings
from transit.matching import service_date_now
from transit.models import Base, Route, Service, Stop, StopTime, Trip

TEST_DB_SUFFIX = "_test"


def _test_engine():
    url = make_url(settings.database_url)
    test_url = url.set(database=f"{url.database}{TEST_DB_SUFFIX}")
    admin = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        exists = connection.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": test_url.database},
        ).scalar()
        if not exists:
            connection.execute(text(f'CREATE DATABASE "{test_url.database}"'))
    admin.dispose()
    return create_engine(test_url, future=True)


@pytest.fixture(scope="session")
def engine():
    try:
        engine = _test_engine()
        with engine.connect():
            pass
    except OperationalError as exc:  # pragma: no cover - depends on the machine
        pytest.skip(f"Postgres not reachable: {exc}")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session(engine) -> Session:
    connection = engine.connect()
    transaction = connection.begin()
    session = sessionmaker(bind=connection, expire_on_commit=False, future=True)()
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


@pytest.fixture
def service_date() -> date:
    """Today in the agency's timezone: ingestion always works against the current service
    date, so the fixture schedule has to be today's schedule."""
    return service_date_now(AGENCY_TIMEZONE)


@pytest.fixture
def schedule(session: Session, service_date: date):
    """A single route with two trips, four stops each - enough to join realtime against.

    The service runs every day so the fixture does not depend on which weekday the tests
    happen to run; calendar logic is tested separately with its own service rows.
    """
    session.add(
        Service(
            agency_key="test",
            service_id="WEEKDAY",
            monday=True,
            tuesday=True,
            wednesday=True,
            thursday=True,
            friday=True,
            saturday=True,
            sunday=True,
            start_date=service_date - timedelta(days=30),
            end_date=service_date + timedelta(days=30),
        )
    )
    session.add(
        Route(
            agency_key="test",
            route_id="4108",
            short_name="R12",
            long_name="Rapid 12",
            route_type=3,
        )
    )
    for index, name in enumerate(["Gateway Plaza", "Hilgard", "Weyburn", "Wilshire"], start=1):
        session.add(
            Stop(
                agency_key="test",
                stop_id=f"S{index}",
                name=name,
                latitude=34.0 + index / 1000,
                longitude=-118.44,
            )
        )
    for trip_id, start_seconds, direction in (("T1", 16 * 3600, 0), ("T2", 17 * 3600, 1)):
        session.add(
            Trip(
                agency_key="test",
                trip_id=trip_id,
                route_id="4108",
                service_id="WEEKDAY",
                direction_id=direction,
                headsign="UCLA",
                start_seconds=start_seconds,
            )
        )
        for sequence in range(1, 5):
            session.add(
                StopTime(
                    agency_key="test",
                    trip_id=trip_id,
                    stop_sequence=sequence,
                    stop_id=f"S{sequence}",
                    arrival_seconds=start_seconds + sequence * 300,
                    departure_seconds=start_seconds + sequence * 300,
                )
            )
    session.flush()
    return service_date


@pytest.fixture
def now() -> datetime:
    return datetime(2025, 3, 10, 23, 0, tzinfo=timezone.utc)
