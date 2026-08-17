"""Freshness: how old is the information we are about to show a rider?

Three different clocks exist in this system and conflating them is the classic transit-data
bug:

* ``feed_timestamp`` - when the agency generated the feed.
* ``observed_at``    - when the agency says a particular reading/prediction was made.
* ``ingested_at``    - when we wrote it down.

Every API response reports the first two so the consumer can decide whether to trust it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

DEFAULT_STALE_AFTER_SECONDS = 180


def age_seconds(moment: datetime | None, now: datetime | None = None) -> int | None:
    if moment is None:
        return None
    now = now or datetime.now(timezone.utc)
    return int((now - moment).total_seconds())


def is_stale(
    moment: datetime | None,
    now: datetime | None = None,
    stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS,
) -> bool:
    """Unknown age counts as stale: absence of a timestamp is not evidence of freshness."""
    age = age_seconds(moment, now)
    if age is None:
        return True
    return age > stale_after_seconds


@dataclass
class Freshness:
    observed_at: datetime | None
    age_seconds: int | None
    stale: bool

    @classmethod
    def of(
        cls,
        moment: datetime | None,
        now: datetime | None = None,
        stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS,
    ) -> Freshness:
        return cls(
            observed_at=moment,
            age_seconds=age_seconds(moment, now),
            stale=is_stale(moment, now, stale_after_seconds),
        )
