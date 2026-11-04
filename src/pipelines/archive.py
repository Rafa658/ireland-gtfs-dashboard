from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from logging import Logger, LoggerAdapter

import duckdb
from prefect import get_run_logger
from prefect.concurrency.sync import concurrency
from prefect.exceptions import MissingContextError
from prefect.logging import get_logger

from pipelines.config import PipelineConfig
from pipelines.windows import object_key

# Global concurrency limit shared by every flow that reads or deletes archive data.
ARCHIVE_LIMIT_NAME = "gtfs-archive"


def get_flow_logger() -> Logger | LoggerAdapter:
    """Return the flow-run logger, falling back to a module logger outside a run."""
    try:
        return get_run_logger()
    except MissingContextError:
        return get_logger("pipelines")


@contextmanager
def archive_slot() -> Iterator[None]:
    """Serialize archive flows; strict so a missing limit fails instead of silently overlapping."""
    with concurrency(ARCHIVE_LIMIT_NAME, strict=True):
        yield


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def timestamp_literal(value: datetime) -> str:
    return f"TIMESTAMPTZ {quote_literal(value.isoformat())}"


def duckdb_connection(config: PipelineConfig) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    for extension in ("postgres", "httpfs"):
        connection.execute(f"INSTALL {extension}")
        connection.execute(f"LOAD {extension}")

    create_object_store_secret(connection, config)
    connection.execute(
        f"ATTACH {quote_literal(config.postgres_dsn)} AS source (TYPE postgres, READ_ONLY)"
    )
    return connection


def create_object_store_secret(
    connection: duckdb.DuckDBPyConnection, config: PipelineConfig
) -> None:
    connection.execute(
        f"""
        CREATE OR REPLACE SECRET object_store (
            TYPE s3,
            KEY_ID {quote_literal(config.minio_access_key)},
            SECRET {quote_literal(config.minio_secret_key)},
            ENDPOINT {quote_literal(config.minio_endpoint)},
            USE_SSL {"true" if config.minio_use_ssl else "false"},
            URL_STYLE 'path',
            REGION {quote_literal(config.minio_region)},
            SCOPE {quote_literal(f"s3://{config.minio_bucket}")}
        )
        """
    )


def export_window(
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
        f"source.{quote_identifier(config.postgres_schema)}"
        f".{quote_identifier(config.postgres_table)}"
    )
    predicate = (
        f'"timestamp" >= {timestamp_literal(window_start)} '
        f'AND "timestamp" < {timestamp_literal(window_end)}'
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
        ) TO {quote_literal(target)} (FORMAT parquet, COMPRESSION zstd)
        """
    )
    logger.info("Exported %s rows to %s", row_count, target)
    return row_count, target
