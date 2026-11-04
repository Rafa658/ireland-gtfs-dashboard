from datetime import UTC, datetime

from prefect import flow

from pipelines import archive
from pipelines.config import PipelineConfig
from pipelines.windows import hour_windows


@flow(name="backfill-hourly-snapshots")
def backfill_hourly_snapshots(
    start: datetime | None = None,
    end: datetime | None = None,
    max_hours: int | None = None,
) -> dict[str, object]:
    """Re-export every whole hour in a range, one object per hour.

    Defaults to ``ARCHIVE_INITIAL_START`` through the last complete hour. Each hour is
    written exactly as the scheduled export would, so reruns are idempotent and simply
    overwrite. ``max_hours`` caps a single run and defaults to ``ARCHIVE_MAX_CATCHUP_HOURS``;
    pass ``0`` to process the whole range at once. When a run is capped it returns
    ``next_start``, which must be passed as ``start`` to continue.
    """
    logger = archive.get_flow_logger()
    config = PipelineConfig.from_env()

    window_start = start or config.archive_initial_start
    if window_start is None:
        raise ValueError("Set ARCHIVE_INITIAL_START or pass an explicit start")

    limit = max_hours if max_hours is not None else config.archive_max_catchup_hours
    windows = hour_windows(window_start, end or datetime.now(tz=UTC), config.tzinfo)

    total = len(windows)
    if limit:
        windows = windows[:limit]

    remaining = total - len(windows)
    next_start = windows[-1][1].isoformat() if remaining and windows else None
    logger.info("Backfilling %s of %s hour(s) from %s", len(windows), total, window_start)

    exported_rows = 0
    written: list[str] = []
    empty_hours = 0

    with archive.duckdb_connection(config) as connection:
        for start_at, end_at in windows:
            rows, target = archive.export_window(connection, config, start_at, end_at, logger)
            if target is None:
                empty_hours += 1
                continue
            exported_rows += rows
            written.append(target)

    if remaining:
        logger.warning(
            "Stopped after %s hour(s); %s remaining. Continue with start=%s or raise max_hours.",
            len(windows),
            remaining,
            next_start,
        )

    return {
        "hours_processed": len(windows),
        "hours_remaining": remaining,
        "hours_empty": empty_hours,
        "objects_written": len(written),
        "rows": exported_rows,
        "next_start": next_start,
    }
