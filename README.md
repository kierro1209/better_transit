# better_transit

A small realtime transit system for the Big Blue Bus routes around UCLA: it polls the
agency's GTFS-Realtime feed, joins it to the published GTFS schedule, keeps every
observation, and serves arrivals over an API, a minimal web page, and a Shortcut-friendly
endpoint.

## Architecture

```text
GTFS static zip  ──►  routes / stops / trips / stop_times / calendar
(current.zip)                          │
                                       │  join key: (route short name, start time)
GTFS-Realtime    ──►  decode protobuf ─┤
(tripupdates.bin)     normalize        │
                                       ▼
                         PostgreSQL  ├─ arrival_predictions  (append-only)
                                     ├─ vehicle_observations (append-only)
                                     └─ ingest_runs          (one row per poll)
                                       │
                                       ▼
                                    FastAPI
                                 ┌─────┴──────┐
                                 ▼            ▼
                              web UI      /shortcut/next
```

One process polls (`ingest-loop`), one process serves (`serve`), one Postgres between them.
No queue, no cache, no scheduler: a poll is a few hundred milliseconds of work every 30
seconds, and the API's slowest query touches an indexed table.

## Running it

```bash
docker compose up -d                       # Postgres 16
cp .env.example .env
pip install -e ".[dev]"
alembic upgrade head                       # create the schema
python -m transit.cli load-static          # download + load the GTFS zip (~124k stop_times)
python -m transit.cli seed-favorites       # config/favorites.json -> favorite_stops
python -m transit.cli ingest-once          # one poll, prints a JSON summary
python -m transit.cli ingest-loop          # poll every POLL_INTERVAL_SECONDS
python -m transit.cli serve                # http://127.0.0.1:8000  (UI at /, docs at /docs)
```

Tests need the same Postgres running; they create and use a `transit_test` database and
skip themselves if Postgres is unreachable.

```bash
pytest
ruff check . && ruff format --check .
```

## API

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | database, schedule size, per-feed freshness, last poll result |
| `GET /routes` | routes in the loaded schedule |
| `GET /favorites` | the stops the UI and Shortcut care about |
| `GET /stops/{stop_id}/arrivals` | next arrivals, newest prediction per trip, each with its age |
| `GET /routes/{route_id}/vehicles` | latest observation per vehicle on a route |
| `GET /vehicles/{vehicle_id}?since=&until=` | one vehicle's history window |
| `GET /shortcut/next?label=UCLA` | flat "when should I leave" payload for iOS Shortcuts |

Every realtime response carries `observed_at`, `age_seconds` and `stale`, because a number
with no age is not usable information.

## What the data actually looks like

Findings from the live feeds, all of which shaped the design:

* **VehiclePositions is dead.** `vehiclepositions.bin` returns HTTP 200 with zero entities
  and a header timestamp frozen at 2025-04-09. TripUpdates is live (~100 entities, ~30s
  refresh). So the system knows *when the bus will arrive*, not *where it is*; latitude and
  longitude are nullable and the ingester is ready for the day the feed comes back.
* **The realtime feed uses different identifiers than the schedule.** `trip.route_id` is
  the route *short name* ("R12", not "4108"); `trip.trip_id` comes from the AVL system and
  appears nowhere in trips.txt; `direction_id` is inverted; and `stop_time_update.stop_id`
  is a third id space, only ~55% of which exists in stops.txt and which names the wrong
  places when it does.
* **What is reliable is the scheduled start.** `(route short name, service date, start
  time)` identifies exactly one static trip, and `stop_sequence` is consistent between the
  feeds, so stops are resolved as `(matched trip, stop_sequence) -> stops.txt`. Measured
  over one poll that mapping is a bijection: 660 realtime stop ids onto 660 distinct static
  stops with no collisions. ~98% of trips in a poll match; the rest are counted, not hidden.

## Design decisions

**Postgres, not SQLite.** The point of the project is months of accumulated history and an
ingester writing while the API reads. Postgres gives real `timestamptz`, `ON CONFLICT DO
NOTHING` for de-duplication and `DISTINCT ON` for "newest row per trip"; SQLite would have
meant a writer lock during every poll and hand-written de-duplication. The cost is Docker.

**Predictions and positions are separate tables.** A `TripUpdate` is a claim about the
future; a `VehiclePosition` is a fact about the past. Storing successive predictions for the
same (trip, stop) is exactly the dataset needed later to ask "how wrong was the 20-minute
prediction?" — collapsing them into one table would destroy that.

**Append-only, de-duplicated in the database.** The feed is a snapshot, not a stream: every
poll re-sends readings we already have. Unique constraints define what "the same
observation" means and `ON CONFLICT DO NOTHING` enforces it, so a re-poll persists zero rows
while a genuinely new reading is appended next to the old one rather than replacing it.

**Freshness is part of every payload.** Nothing is interpolated. If the agency stops
predicting, the ETA stops moving and the reported age grows, which is the honest answer.

**Synchronous handlers.** psycopg blocks; FastAPI runs `def` handlers in a threadpool, so
blocking there is safe, whereas blocking inside `async def` would stall the event loop for
every other request. An async driver would buy nothing measurable at this request volume.

## Open questions

* Should unmatched realtime trips (~2%) be resolved another way, or is counting them enough?
* Is a learned `rt_stop_id -> stop_id` mapping table worth it, to resolve stops even for
  trips that fail to match?
* How much storage does a month of polling actually cost? (measure, then decide on retention)
* When BBB's VehiclePositions feed recovers, do the positions agree with the TripUpdates?
