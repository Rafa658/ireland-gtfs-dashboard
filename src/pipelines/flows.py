from datetime import UTC, datetime
from logging import Logger, LoggerAdapter

import duckdb
import psycopg
from prefect import flow, get_run_logger
from prefect.exceptions import MissingContextError
from prefect.logging import get_logger
from psycopg import sql

from pipelines.config import PipelineConfig
from pipelines.windows import object_key, previous_hour_window, retention_cutoff


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


@flow(name="export-hourly-snapshots")
def export_hourly_snapshots(run_at: datetime | None = None) -> dict[str, object]:
    """Export the previous full local hour of snapshots to MinIO as Parquet.

    ``run_at`` defaults to now; pass an explicit instant to re-export an earlier hour.
    """
    logger = _logger()
    config = PipelineConfig.from_env()

    now = run_at or datetime.now(tz=UTC)
    window_start, window_end = previous_hour_window(now, config.tzinfo)
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

    with _duckdb_connection(config) as connection:
        (row_count,) = connection.execute(
            f"SELECT count(*) FROM {relation} WHERE {predicate}"
        ).fetchone()

        if row_count == 0:
            logger.warning("No snapshots found for window %s -> %s", window_start, window_end)
            return {"rows": 0, "target": None, "window_start": window_start.isoformat()}

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
    return {"rows": row_count, "target": target, "window_start": window_start.isoformat()}


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
