import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import boto3
import psycopg
import pyarrow as pa
import pyarrow.parquet as parquet
from botocore.config import Config as BotoConfig
from psycopg import sql

from orchestration.config import OrchestrationConfig
from orchestration.time_windows import HourWindow, hourly_object_key

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("header", pa.string(), nullable=False),
        pa.field("entity", pa.string(), nullable=True),
    ]
)


@dataclass(frozen=True, slots=True)
class ArchiveRow:
    timestamp: datetime
    header: str
    entity: str | None


class HourBatchRepository(Protocol):
    def iter_hour_batches(
        self,
        window: HourWindow,
        *,
        batch_size: int,
    ) -> Iterator[list[ArchiveRow]]: ...


class PostgresArchiveRepository:
    def __init__(
        self,
        config: OrchestrationConfig,
        connect: Callable[..., Any] = psycopg.connect,
    ) -> None:
        self._config = config
        self._connect = connect

    def _connection(self) -> Any:
        return self._connect(
            host=self._config.postgres_host,
            port=self._config.postgres_port,
            dbname=self._config.postgres_db,
            user=self._config.postgres_user,
            password=self._config.postgres_password,
            sslmode=self._config.postgres_sslmode,
            connect_timeout=self._config.postgres_connect_timeout_seconds,
        )

    def iter_hour_batches(
        self,
        window: HourWindow,
        *,
        batch_size: int,
    ) -> Iterator[list[ArchiveRow]]:
        query = sql.SQL(
            """
                SELECT timestamp, header::text, entity::text
                FROM {}.{}
                WHERE timestamp >= %s AND timestamp < %s
                ORDER BY timestamp
                """
        ).format(
            sql.Identifier(self._config.postgres_schema),
            sql.Identifier(self._config.postgres_table),
        )

        with (
            self._connection() as connection,
            connection.cursor(name="gtfs_archive_export") as cursor,
        ):
            cursor.execute(query, (window.start, window.end))
            while rows := cursor.fetchmany(batch_size):
                yield [ArchiveRow(*row) for row in rows]

    def earliest_timestamp(self) -> datetime | None:
        query = sql.SQL("SELECT MIN(timestamp) FROM {}.{}").format(
            sql.Identifier(self._config.postgres_schema),
            sql.Identifier(self._config.postgres_table),
        )
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
        return row[0]

    def delete_before(self, cutoff: datetime) -> int:
        query = sql.SQL("DELETE FROM {}.{} WHERE timestamp < %s").format(
            sql.Identifier(self._config.postgres_schema),
            sql.Identifier(self._config.postgres_table),
        )
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (cutoff,))
            return cursor.rowcount


class MinioParquetExporter:
    def __init__(
        self,
        config: OrchestrationConfig,
        repository: HourBatchRepository,
        s3_client: Any | None = None,
        *,
        temporary_directory: Path | None = None,
        batch_size: int = 100,
    ) -> None:
        self._config = config
        self._repository = repository
        self._s3_client = s3_client or boto3.client(
            "s3",
            endpoint_url=config.minio_endpoint_url,
            aws_access_key_id=config.minio_access_key,
            aws_secret_access_key=config.minio_secret_key,
            region_name=config.minio_region,
            config=BotoConfig(
                s3={"addressing_style": "path"},
                retries={"mode": "standard", "max_attempts": 4},
            ),
        )
        self._temporary_directory = temporary_directory
        self._batch_size = batch_size

    def export(self, window: HourWindow) -> int:
        with tempfile.NamedTemporaryFile(
            dir=self._temporary_directory,
            suffix=".parquet",
            delete=False,
        ) as temporary:
            path = Path(temporary.name)
        writer: parquet.ParquetWriter | None = None
        row_count = 0

        try:
            for batch in self._repository.iter_hour_batches(
                window,
                batch_size=self._batch_size,
            ):
                table = _rows_to_table(batch)
                if writer is None:
                    writer = parquet.ParquetWriter(
                        path,
                        PARQUET_SCHEMA,
                        compression="zstd",
                    )
                writer.write_table(table)
                row_count += len(batch)

            if writer is not None:
                writer.close()
                writer = None

            if row_count == 0:
                return 0

            key = hourly_object_key(
                window,
                self._config.archive_timezone,
                prefix=self._config.archive_prefix,
            )
            self._s3_client.upload_file(
                str(path),
                self._config.minio_bucket,
                key,
                ExtraArgs={"ContentType": "application/vnd.apache.parquet"},
            )
            return row_count
        finally:
            if writer is not None:
                writer.close()
            path.unlink(missing_ok=True)


def _rows_to_table(rows: list[ArchiveRow]) -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array(
                [row.timestamp.astimezone(UTC) for row in rows],
                type=PARQUET_SCHEMA.field("timestamp").type,
            ),
            pa.array([row.header for row in rows], type=pa.string()),
            pa.array([row.entity for row in rows], type=pa.string()),
        ],
        schema=PARQUET_SCHEMA,
    )
