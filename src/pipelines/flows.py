from datetime import UTC, datetime
from logging import Logger, LoggerAdapter

import duckdb
import psycopg
from prefect import flow, get_run_logger
from prefect.exceptions import MissingContextError
from prefect.logging import get_logger
from psycopg import sql

from pipelines.config import PipelineConfig
from pipelines.windows import hour_windows, object_key, previous_hour_window, retention_cutoff


def _logger() -> Logger | LoggerAdapter:
    """Return the flow-run logger, falling back to a module logger outside a run."""
    try:
        return get_run_logger()
    except MissingContextError:
        return get_logger("pipelines")


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _timestamp_literal(value: datetime) -> str:
    return f"TIMESTAMPTZ {_quote_literal(value.isoformat())}"


def _duckdb_connection(config: PipelineConfig) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    for extension in ("postgres", "httpfs"):
        connection.execute(f"INSTALL {extension}")
        connection.execute(f"LOAD {extension}")

    _create_object_store_secret(connection, config)
    connection.execute(
        f"ATTACH {_quote_literal(config.postgres_dsn)} AS source (TYPE postgres, READ_ONLY)"
    )
    return connection


def _create_object_store_secret(
    connection: duckdb.DuckDBPyConnection, config: PipelineConfig
) -> None:
    connection.execute(
        f"""
        CREATE OR REPLACE SECRET object_store (
            TYPE s3,
            KEY_ID {_quote_literal(config.minio_access_key)},
            SECRET {_quote_literal(config.minio_secret_key)},
            ENDPOINT {_quote_literal(config.minio_endpoint)},
            USE_SSL {"true" if config.minio_use_ssl else "false"},
            URL_STYLE 'path',
            REGION {_quote_literal(config.minio_region)},
            SCOPE {_quote_literal(f"s3://{config.minio_bucket}")}
        )
        """
    )


def _export_window(
    connection: duckdb.DuckDBPyConnection,
    config: PipelineConfig,
    window_start: datetime,
    window_end: datetime,
    logger: Logger | LoggerAdapter,
) -> tuple[int, str | None]:
    """Export one hour. Returns the row count and the object written, if any.

    Shared by the scheduled export and the backfill so both produce identical objects.
    """
    key = object_key(config.archive_prefix, window_start, config.tzinfo)
    target = f"s3://{config.minio_bucket}/{key}"

    relation = (
        f"source.{_quote_identifier(config.postgres_schema)}"
        f".{_quote_identifier(config.postgres_table)}"
    )
    predicate = (
        f'"timestamp" >= {_timestamp_literal(window_start)} '
        f'AND "timestamp" < {_timestamp_literal(window_end)}'
    )

    (row_count,) = connection.execute(
        f"SELECT count(*) FROM {relation} WHERE {predicate}"
    ).fetchone()

    if row_count == 0:
        logger.warning("No snapshots found for window %s -> %s", window_start, window_end)
        return 0, None

    connection.execute(
        f"""
        COPY (
            SELECT * FROM {relation}
            WHERE {predicate}
            ORDER BY "timestamp"
        ) TO {_quote_literal(target)} (FORMAT parquet, COMPRESSION zstd)
        """
    )
    logger.info("Exported %s rows to %s", row_count, target)
    return row_count, target


@flow(name="export-hourly-snapshots")
def export_hourly_snapshots(run_at: datetime | None = None) -> dict[str, object]:
    """Export the previous full local hour of snapshots to MinIO as Parquet.

    ``run_at`` defaults to now; pass an explicit instant to re-export an earlier hour.
    """
    logger = _logger()
    config = PipelineConfig.from_env()

    now = run_at or datetime.now(tz=UTC)
    window_start, window_end = previous_hour_window(now, config.tzinfo)

    with _duckdb_connection(config) as connection:
        row_count, target = _export_window(connection, config, window_start, window_end, logger)

    return {"rows": row_count, "target": target, "window_start": window_start.isoformat()}


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
    logger = _logger()
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

    with _duckdb_connection(config) as connection:
        for start_at, end_at in windows:
            rows, target = _export_window(connection, config, start_at, end_at, logger)
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


@flow(name="enforce-retention")
def enforce_retention(run_at: datetime | None = None) -> dict[str, object]:
    """Delete snapshots older than the retention window, measured in whole local days."""
    logger = _logger()
    config = PipelineConfig.from_env()

    now = run_at or datetime.now(tz=UTC)
    cutoff = retention_cutoff(now, config.tzinfo, config.retention_days)

    statement = sql.SQL('DELETE FROM {}.{} WHERE "timestamp" < %s').format(
        sql.Identifier(config.postgres_schema),
        sql.Identifier(config.postgres_table),
    )

    with psycopg.connect(config.postgres_dsn) as connection, connection.cursor() as cursor:
        cursor.execute(statement, (cutoff,))
        deleted = cursor.rowcount

    logger.info("Deleted %s snapshots older than %s", deleted, cutoff.isoformat())
    return {"deleted": deleted, "cutoff": cutoff.isoformat()}
