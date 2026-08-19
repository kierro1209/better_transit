from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process configuration, read from the environment (and a local .env file).

    pydantic-settings just reads os.environ, coerces the strings into the annotated
    types, and raises if something required is missing. Nothing magical happens.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://transit:transit@localhost:5432/transit"

    bbb_static_url: str = "https://gtfs.bigbluebus.com/current.zip"
    bbb_trip_updates_url: str = "https://gtfs.bigbluebus.com/tripupdates.bin"
    bbb_vehicle_positions_url: str = "https://gtfs.bigbluebus.com/vehiclepositions.bin"

    poll_interval_seconds: int = 30
    ingest_in_server: bool = False
    feed_stale_after_seconds: int = 180

    http_timeout_seconds: float = 20.0


settings = Settings()

AGENCY_KEY = "bigbluebus"
AGENCY_TIMEZONE = "America/Los_Angeles"
