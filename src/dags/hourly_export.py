from datetime import UTC, datetime

from prefect import flow

from pipelines import archive
from pipelines.config import PipelineConfig
from pipelines.windows import previous_hour_window


@flow(name="export-hourly-snapshots")
def export_hourly_snapshots(run_at: datetime | None = None) -> dict[str, object]:
    """Export the previous full local hour of snapshots to MinIO as Parquet.

    ``run_at`` defaults to now; pass an explicit instant to re-export an earlier hour.
    """
    logger = archive.get_flow_logger()
    config = PipelineConfig.from_env()

    now = run_at or datetime.now(tz=UTC)
    window_start, window_end = previous_hour_window(now, config.tzinfo)

    with archive.archive_slot(), archive.duckdb_connection(config) as connection:
        row_count, target = archive.export_window(
            connection, config, window_start, window_end, logger
        )

    return {"rows": row_count, "target": target, "window_start": window_start.isoformat()}
