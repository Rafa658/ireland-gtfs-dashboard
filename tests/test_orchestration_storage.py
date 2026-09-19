from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as parquet
from psycopg import sql

from orchestration.config import OrchestrationConfig
from orchestration.storage import (
    ArchiveRow,
    MinioParquetExporter,
    PostgresArchiveRepository,
)
from orchestration.time_windows import HourWindow


class FakeCursor(AbstractContextManager["FakeCursor"]):
    def __init__(
        self,
        batches: list[list[tuple[Any, ...]]],
        *,
        row: tuple[Any, ...] | None = None,
        rowcount: int = 0,
    ) -> None:
        self.batches = iter(batches)
        self.row = row
        self.rowcount = rowcount
        self.executions: list[tuple[Any, Any]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, query: Any, params: Any = None) -> None:
        self.executions.append((query, params))

    def fetchmany(self, _: int) -> list[tuple[Any, ...]]:
        return next(self.batches, [])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.row


class FakeConnection(AbstractContextManager["FakeConnection"]):
    def __init__(self, cursor: FakeCursor) -> None:
        self.cursor_instance = cursor
        self.cursor_names: list[str | None] = []

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def cursor(self, name: str | None = None) -> FakeCursor:
        self.cursor_names.append(name)
        return self.cursor_instance


class FakeS3Client:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str, str, dict[str, str]]] = []
        self.uploaded_bytes: bytes | None = None

    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
        ExtraArgs: dict[str, str],
    ) -> None:
        self.uploads.append((filename, bucket, key, ExtraArgs))
        self.uploaded_bytes = Path(filename).read_bytes()


def config() -> OrchestrationConfig:
    return OrchestrationConfig(
        postgres_connect_timeout_seconds=10,
        postgres_host="postgres",
        postgres_port=5432,
        postgres_db="transport",
        postgres_user="poller",
        postgres_password="password",
        postgres_schema="gtfs-data",
        postgres_table="vehicle snapshots",
        postgres_sslmode="prefer",
        minio_endpoint_url="http://minio:9000",
        minio_access_key="access",
        minio_secret_key="secret",
        minio_bucket="gtfs-ireland",
        minio_region="us-east-1",
        archive_prefix="realtime",
        archive_timezone_name="America/Sao_Paulo",
        archive_initial_start=datetime(2026, 9, 19, 3, tzinfo=UTC),
        archive_max_catchup_hours=24,
        failure_webhook_url=None,
    )


def test_streams_rows_from_a_parameterized_hour_query() -> None:
    timestamp = datetime(2026, 9, 19, 14, 2, tzinfo=UTC)
    cursor = FakeCursor([[(timestamp, '{"version": "2"}', None)], []])
    connection = FakeConnection(cursor)
    repository = PostgresArchiveRepository(config(), connect=lambda **_: connection)
    window = HourWindow(
        start=datetime(2026, 9, 19, 14, tzinfo=UTC),
        end=datetime(2026, 9, 19, 15, tzinfo=UTC),
    )

    batches = list(repository.iter_hour_batches(window, batch_size=100))

    assert batches == [[ArchiveRow(timestamp, '{"version": "2"}', None)]]
    assert connection.cursor_names == ["gtfs_archive_export"]
    assert cursor.executions == [
        (
            sql.SQL(
                """
                SELECT timestamp, header::text, entity::text
                FROM {}.{}
                WHERE timestamp >= %s AND timestamp < %s
                ORDER BY timestamp
                """
            ).format(sql.Identifier("gtfs-data"), sql.Identifier("vehicle snapshots")),
            (window.start, window.end),
        )
    ]


def test_deletes_only_rows_before_the_parameterized_cutoff() -> None:
    cursor = FakeCursor([], rowcount=5)
    connection = FakeConnection(cursor)
    repository = PostgresArchiveRepository(config(), connect=lambda **_: connection)
    cutoff = datetime(2026, 9, 16, 3, tzinfo=UTC)

    assert repository.delete_before(cutoff) == 5
    assert cursor.executions == [
        (
            sql.SQL("DELETE FROM {}.{} WHERE timestamp < %s").format(
                sql.Identifier("gtfs-data"),
                sql.Identifier("vehicle snapshots"),
            ),
            (cutoff,),
        )
    ]


def test_reads_the_earliest_source_timestamp() -> None:
    earliest = datetime(2026, 9, 19, 3, tzinfo=UTC)
    cursor = FakeCursor([], row=(earliest,))
    connection = FakeConnection(cursor)
    repository = PostgresArchiveRepository(config(), connect=lambda **_: connection)

    assert repository.earliest_timestamp() == earliest
    assert cursor.executions == [
        (
            sql.SQL("SELECT MIN(timestamp) FROM {}.{}").format(
                sql.Identifier("gtfs-data"),
                sql.Identifier("vehicle snapshots"),
            ),
            None,
        )
    ]


def test_writes_zstd_parquet_and_uploads_the_deterministic_key(tmp_path: Path) -> None:
    timestamp = datetime(2026, 9, 19, 14, 2, tzinfo=UTC)

    class FakeRepository:
        def iter_hour_batches(
            self,
            _: HourWindow,
            *,
            batch_size: int,
        ) -> list[list[ArchiveRow]]:
            assert batch_size == 100
            return [
                [
                    ArchiveRow(timestamp, '{"version":"2"}', None),
                    ArchiveRow(timestamp, '{"version":"2"}', "null"),
                ]
            ]

    s3_client = FakeS3Client()
    exporter = MinioParquetExporter(
        config(),
        FakeRepository(),
        s3_client,
        temporary_directory=tmp_path,
    )
    window = HourWindow(
        start=datetime(2026, 9, 19, 14, tzinfo=UTC),
        end=datetime(2026, 9, 19, 15, tzinfo=UTC),
    )

    assert exporter.export(window) == 2

    _, bucket, key, extra_args = s3_client.uploads[0]
    assert s3_client.uploaded_bytes is not None
    table = parquet.read_table(pa.BufferReader(s3_client.uploaded_bytes))
    assert bucket == "gtfs-ireland"
    assert key == ("realtime/ingestion_date=2026-09-19/hour_start=2026-09-19T14-00-00Z.parquet")
    assert extra_args == {"ContentType": "application/vnd.apache.parquet"}
    assert table.column("timestamp").to_pylist() == [timestamp, timestamp]
    assert table.column("header").to_pylist() == ['{"version":"2"}', '{"version":"2"}']
    assert table.column("entity").to_pylist() == [None, "null"]
    assert table.schema.field("timestamp").type.tz == "UTC"
    assert (
        parquet.ParquetFile(pa.BufferReader(s3_client.uploaded_bytes))
        .metadata.row_group(0)
        .column(0)
        .compression
        == "ZSTD"
    )


def test_skips_upload_for_an_empty_hour(tmp_path: Path) -> None:
    class EmptyRepository:
        def iter_hour_batches(
            self,
            _: HourWindow,
            *,
            batch_size: int,
        ) -> list[list[ArchiveRow]]:
            return []

    s3_client = FakeS3Client()
    exporter = MinioParquetExporter(
        config(),
        EmptyRepository(),
        s3_client,
        temporary_directory=tmp_path,
    )
    window = HourWindow(
        start=datetime(2026, 9, 19, 14, tzinfo=UTC),
        end=datetime(2026, 9, 19, 15, tzinfo=UTC),
    )

    assert exporter.export(window) == 0
    assert s3_client.uploads == []
