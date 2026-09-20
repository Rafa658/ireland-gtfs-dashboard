from datetime import UTC, datetime, timedelta, tzinfo


def _as_aware(value: datetime) -> datetime:
    """Treat naive inputs as UTC so manual runs match scheduled ones."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def previous_hour_window(now: datetime, zone: tzinfo) -> tuple[datetime, datetime]:
    """Return the closed-open window covering the hour before ``now`` in ``zone``."""
    local_now = _as_aware(now).astimezone(zone)
    end = local_now.replace(minute=0, second=0, microsecond=0)
    return end - timedelta(hours=1), end


def retention_cutoff(now: datetime, zone: tzinfo, retention_days: int) -> datetime:
    """Return the local midnight before which rows fall outside the retention window."""
    local_now = _as_aware(now).astimezone(zone)
    midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight - timedelta(days=retention_days)


def object_key(prefix: str, window_start: datetime, zone: tzinfo) -> str:
    """Build the partitioned object key for an exported hour."""
    local_start = _as_aware(window_start).astimezone(zone)
    return (
        f"{prefix.strip('/')}"
        f"/ingestion_date={local_start:%Y-%m-%d}"
        f"/snapshots_{local_start:%Y%m%dT%H}.parquet"
    )
