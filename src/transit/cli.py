"""Command line entry points.

python -m transit.cli load-static        # download + load the static GTFS zip
python -m transit.cli seed-favorites     # load config/favorites.json
python -m transit.cli show-favorites     # sanity check the loaded schedule
python -m transit.cli ingest-once        # one realtime poll of every feed
python -m transit.cli ingest-loop        # poll forever
python -m transit.cli serve              # run the API + UI
python -m transit.cli serve --with-ingest  # run the API + UI + realtime poller
"""

import argparse
import json
import logging
import os
from pathlib import Path

from sqlalchemy import delete, select

from transit.db import session_scope
from transit.gtfs.static_loader import refresh_static_feed
from transit.ingest import run_forever, run_once
from transit.logging_setup import configure_logging
from transit.models import FavoriteStop, Route, Stop, StopTime, Trip

log = logging.getLogger("transit.cli")


def cmd_load_static(args: argparse.Namespace) -> None:
    with session_scope() as session:
        report = refresh_static_feed(session)
    log.info("static feed loaded", extra={"fields": report.as_dict()})


def cmd_seed_favorites(args: argparse.Namespace) -> None:
    entries = json.loads(Path(args.path).read_text())
    with session_scope() as session:
        session.execute(delete(FavoriteStop))
        for entry in entries:
            stop = session.get(Stop, (entry["agency_key"], entry["stop_id"]))
            if stop is None:
                raise SystemExit(f"stop {entry['stop_id']} not in the schedule; load-static first")
            session.add(
                FavoriteStop(
                    label=entry["label"],
                    agency_key=entry["agency_key"],
                    stop_id=entry["stop_id"],
                    walk_minutes=entry.get("walk_minutes"),
                )
            )
    log.info("favorites seeded", extra={"fields": {"count": len(entries)}})


def cmd_show_favorites(args: argparse.Namespace) -> None:
    with session_scope() as session:
        favorites = session.scalars(select(FavoriteStop).order_by(FavoriteStop.label)).all()
        for favorite in favorites:
            stop = session.get(Stop, (favorite.agency_key, favorite.stop_id))
            routes = session.execute(
                select(Route.short_name, Route.long_name)
                .join(
                    Trip, (Trip.route_id == Route.route_id) & (Trip.agency_key == Route.agency_key)
                )
                .join(
                    StopTime,
                    (StopTime.trip_id == Trip.trip_id) & (StopTime.agency_key == Trip.agency_key),
                )
                .where(
                    StopTime.agency_key == favorite.agency_key,
                    StopTime.stop_id == favorite.stop_id,
                )
                .distinct()
            ).all()
            served = ", ".join(sorted(r.short_name or "?" for r in routes))
            print(f"{favorite.label:10} {favorite.stop_id:>6}  {stop.name}")
            print(f"{'':10} {'':>6}  routes: {served or '(none scheduled)'}")


def cmd_ingest_once(args: argparse.Namespace) -> None:
    run_once()


def cmd_ingest_loop(args: argparse.Namespace) -> None:
    run_forever(args.interval)


def cmd_serve(args: argparse.Namespace) -> None:
    import uvicorn

    if args.with_ingest:
        # Uvicorn's reload worker is a separate process, so pass this through the environment
        # rather than changing only the settings object already loaded in this process.
        os.environ["INGEST_IN_SERVER"] = "true"
    uvicorn.run("transit.api.main:app", host=args.host, port=args.port, reload=args.reload)


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(prog="transit")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("load-static").set_defaults(func=cmd_load_static)

    seed = sub.add_parser("seed-favorites")
    seed.add_argument("--path", default="config/favorites.json")
    seed.set_defaults(func=cmd_seed_favorites)

    sub.add_parser("show-favorites").set_defaults(func=cmd_show_favorites)

    sub.add_parser("ingest-once").set_defaults(func=cmd_ingest_once)

    loop = sub.add_parser("ingest-loop")
    loop.add_argument("--interval", type=int, default=None, help="seconds between polls")
    loop.set_defaults(func=cmd_ingest_loop)

    serve = sub.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--reload", action="store_true")
    serve.add_argument("--with-ingest", action="store_true")
    serve.set_defaults(func=cmd_serve)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
