from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, tzinfo


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _require_hour_boundary(value: datetime, name: str) -> None:
    _require_aware(value, name)
    if value.minute or value.second or value.microsecond:
        raise ValueError(f"{name} must be aligned to an hour")


@dataclass(frozen=True, slots=True)
class HourWindow:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        _require_hour_boundary(self.start, "start")
        _require_hour_boundary(self.end, "end")
        if self.end.astimezone(UTC) - self.start.astimezone(UTC) != timedelta(hours=1):
            raise ValueError("hour window must span exactly one hour")


def complete_hour_end(now: datetime, schedule_timezone: tzinfo) -> datetime:
    _require_aware(now, "now")
    local_now = now.astimezone(schedule_timezone)
    return local_now.replace(minute=0, second=0, microsecond=0).astimezone(UTC)


def iter_hour_windows(
    start: datetime,
    end: datetime,
    *,
    limit: int | None = None,
) -> Iterator[HourWindow]:
    _require_hour_boundary(start, "start")
    _require_hour_boundary(end, "end")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    current = start.astimezone(UTC)
    exclusive_end = end.astimezone(UTC)
    emitted = 0
    while current < exclusive_end and (limit is None or emitted < limit):
        next_hour = current + timedelta(hours=1)
        yield HourWindow(start=current, end=next_hour)
        current = next_hour
        emitted += 1


def hourly_object_key(window: HourWindow, schedule_timezone: tzinfo, *, prefix: str) -> str:
    normalized_prefix = prefix.strip("/")
    if not normalized_prefix:
        raise ValueError("prefix must not be empty")

    local_date = window.start.astimezone(schedule_timezone).date().isoformat()
    utc_start = window.start.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"{normalized_prefix}/ingestion_date={local_date}/hour_start={utc_start}.parquet"


def retention_cutoff(
    now: datetime,
    export_cursor: datetime,
    *,
    retention: timedelta = timedelta(hours=72),
) -> datetime:
    _require_aware(now, "now")
    _require_hour_boundary(export_cursor, "export_cursor")
    if retention <= timedelta(0):
        raise ValueError("retention must be positive")
    return min(now.astimezone(UTC) - retention, export_cursor.astimezone(UTC))
