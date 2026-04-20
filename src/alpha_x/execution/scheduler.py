from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from datetime import time as dt_time
from zoneinfo import ZoneInfo

from alpha_x.config.settings import Settings, get_settings
from alpha_x.execution.runner import run_daily_review

LOGGER = logging.getLogger(__name__)


def parse_run_time(value: str) -> dt_time:
    try:
        hour_text, minute_text = value.split(":", maxsplit=1)
        return dt_time(hour=int(hour_text), minute=int(minute_text))
    except ValueError as error:
        raise ValueError("Daily run time must use HH:MM format.") from error


def next_run_at(now: datetime, run_time: dt_time) -> datetime:
    candidate = datetime.combine(now.date(), run_time, tzinfo=now.tzinfo)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def run_daily_scheduler(
    *,
    settings: Settings | None = None,
    timezone: str = "Europe/Amsterdam",
    sleep_seconds: int = 30,
    dry_run: bool = False,
) -> None:
    active_settings = settings or get_settings()
    run_time = parse_run_time(active_settings.distance_buffer_daily_run_time)
    tz = ZoneInfo(timezone)
    last_run_date: date | None = None

    LOGGER.info(
        "Distance buffer scheduler started daily_run_time=%s timezone=%s",
        active_settings.distance_buffer_daily_run_time,
        timezone,
    )
    while True:
        now = datetime.now(tz)
        due = now.time() >= run_time and last_run_date != now.date()
        if due:
            try:
                run_daily_review(settings=active_settings, dry_run=dry_run)
                last_run_date = now.date()
            except Exception:
                LOGGER.exception("Distance buffer daily review failed.")
        upcoming = next_run_at(now, run_time)
        seconds = max(1, min(sleep_seconds, int((upcoming - now).total_seconds())))
        time.sleep(seconds)
