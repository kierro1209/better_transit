# Milestone 1 — Design decisions (before any code)

## What I verified live (not assumptions)

I probed the candidate LA feeds directly from this machine tonight:

| Feed | Result |
|---|---|
| `https://gtfs.bigbluebus.com/current.zip` (BBB static GTFS) | 200, 1.0 MB zip |
| `https://gtfs.bigbluebus.com/tripupdates.bin` (GTFS-RT) | 200, ~50 KB, **103 live entities**, header timestamp = now, protobuf v2.0 |
| `https://gtfs.bigbluebus.com/alerts.bin` | 200, live |
| `https://gtfs.bigbluebus.com/vehiclepositions.bin` | 200 but **0 entities**, header timestamp = `1744211721` = 2025-04-09. Dead/frozen for ~16 months. BBB's own index page and Transitland both show the same stale date. |
| LA Metro `api.metro.net` realtime (`/LACMTA/trip_detail/route_code/720`, `/2`) | 200 but returns `[]` — the realtime portion of Metro's v2 API is returning nothing right now. Static endpoints (`route_overview`, `agency`) work fine. |
| LA Metro raw GTFS-RT protobuf URLs (several known/guessed) | 404 |
| Swiftly (`api.goswift.ly/real-time/lametro/...`) | 401 — works but needs an API key Metro issues |

**Consequence:** the "where is the physical vehicle (lat/lon)" half of the MVP is not available keyless in Westwood right now. TripUpdates ("when will it arrive") is available, live, and rich.

## Decision 1 — Which agency

**Recommendation: Big Blue Bus (BBB), single agency.**

- Simplest option: BBB. Keyless, permanent URLs, protobuf, and it is the agency that actually serves UCLA/Westwood (routes 1, 2, 3, 8, R12, 18 all touch campus or Westwood Village).
- Alternatives:
  1. **LA Metro via Swiftly key** — gives real VehiclePositions with lat/lon/bearing/speed. Cost: I have to request a key from Metro (turnaround unknown, blocks the weekend), and you'd depend on a credential.
  2. **Transitland cached RT** — free-tier API key, redistributes BBB/Metro RT. Adds a middleman between you and the agency, which is exactly the abstraction you said you want to see through.
  3. **Metro static + JSON realtime API** — no protobuf learning, and the realtime part is currently returning empty.
- Tradeoff of choosing BBB: you get GTFS-Schedule + protobuf GTFS-RT + real UCLA relevance with zero credentials, but **no lat/lon** until BBB fixes its VP feed. We keep `latitude`/`longitude` nullable in the model, keep polling `vehiclepositions.bin` every cycle, and log it as a stale feed — so the day it comes back, the pipeline populates positions with no schema change.
- What would make us reconsider: BBB's VP feed staying dead past this weekend and you wanting a live map → request a Swiftly key for Metro and add it as a *second agency* behind the same normalized model (which is exactly why the model is agency-neutral).

**Important consequence for the data model:** with only TripUpdates, an "observation" is *a prediction observed at time T*, not *a position observed at time T*. Those are different things and I want to store them in two different tables rather than pretending a prediction is a position:

- `vehicle_observations` — physical facts (lat, lon, bearing, speed, current stop). Populated when VP works. Also partially populated from TripUpdates (vehicle_id + trip_id + next stop_sequence, no coordinates).
- `arrival_predictions` — "at `observed_at`, the agency predicted vehicle V on trip T reaches stop S at time X". This is append-only and is the actual ML training substrate: predicted-vs-actual is computable purely from this table plus the schedule.

This is a deviation from your §6 sketch (one `VehicleObservation` entity), and I want your sign-off before writing it. The reason: Rule 5 — a prediction and a position are different events, and collapsing them destroys the label we need later.

## Decision 2 — Postgres vs SQLite

**Recommendation: PostgreSQL 16 in Docker Compose.** I verified `docker run postgres:16` works on this machine in ~12 s, so the setup friction you were worried about is not real here.

- What SQLite would gain: zero processes, file is the database, trivially copyable.
- What SQLite would lose: single-writer locking (our ingester writes every 20–30 s while the API reads — SQLite handles this with WAL, but it gets ugly the moment we add a second writer), no real `TIMESTAMPTZ`, no partial/covering index behaviors we'll want on a growing observation table, and no `COPY` for bulk loading the static GTFS.
- Why Postgres now: the whole point of the project is an append-only historical dataset that grows for months. Migrating 10 M rows and a schema from SQLite to Postgres later is real work; starting on Postgres costs one `docker compose up`.
- What would make us reconsider: if you want the whole thing to run on a Raspberry Pi with no Docker, SQLite is fine and the SQLAlchemy layer makes the switch mostly mechanical.

## Decision 3 — How the schedule and realtime data meet

GTFS-RT is deliberately anemic: `tripupdates.bin` gives you `trip_id`, `stop_id`, `stop_sequence` and a Unix timestamp. It does **not** tell you the route name, the stop name, or the scheduled time. Those live in the static GTFS zip. So Milestone 1 must load the static feed into Postgres first (`agency`, `routes`, `trips`, `stops`, `stop_times`, `calendar`), and the realtime ingester then joins against it.

`route → trip → stop_times → stops`: a **route** is a branded service ("BBB Route 1"); a **trip** is one specific vehicle run along that route at one specific time on one specific service day; `stop_times` is the ordered list of (trip, stop, scheduled arrival, sequence). This is why "when is the next Route 1" is a question about *trips*, not about the route.

## Milestone plan

1. **M1 — schedule spine.** Repo skeleton, Docker Compose Postgres, SQLAlchemy models + Alembic migration, GTFS static loader, verified by querying the real stop IDs you use.
2. **M2 — realtime ingestion.** Polling loop, protobuf decode, per-cycle `ingest_runs` metadata (latency, header timestamp, entity counts, rejects), normalization, append-only writes, stale-feed detection.
3. **M3 — API.** FastAPI: `/health`, `/routes`, `/stops/{stop_id}/arrivals`, `/routes/{route_id}/vehicles`, `/vehicles/{vehicle_id}`, every response carrying explicit freshness fields.
4. **M4 — minimal UI + Shortcut-friendly JSON.** Favorite stops, ETA + freshness, no polish.
5. **M5 — let it run**, then review the accumulated data together.

Each milestone ends with 2–4 questions for you to answer before I move on.

## Open questions for you

1. **Which stops do you actually use?** Names are enough ("Westwood/Le Conte eastbound", "Hilgard/Manning", "Wilshire/Westwood"). I'll resolve them to BBB `stop_id`s from the static feed and seed them as your favorites. If you'd rather, I can pick a sensible UCLA default set and you can correct it later.
2. Do you accept the two-table split (`vehicle_observations` vs `arrival_predictions`) over the single `VehicleObservation` in your spec?
3. Are you fine with Docker Compose Postgres, or do you specifically want SQLite for the first weekend?
