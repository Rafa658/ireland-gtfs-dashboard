from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pytest

from pipelines.config import PipelineConfig
from pipelines.flows import (
    _create_object_store_secret,
    _quote_identifier,
    _quote_literal,
    _timestamp_literal,
    enforce_retention,
    export_hourly_snapshots,
)

SAO_PAULO = ZoneInfo("America/Sao_Paulo")

PIPELINE_ENV = {
    "POSTGRES_HOST": "db.internal",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "gtfs",
    "POSTGRES_USER": "gtfs",
    "POSTGRES_PASSWORD": "secret",
    "POSTGRES_SCHEMA": "gtfs",
    "POSTGRES_TABLE": "snapshots",
    "POSTGRES_SSLMODE": "prefer",
    "POSTGRES_CONNECT_TIMEOUT_SECONDS": "10",
    "MINIO_ENDPOINT_URL": "http://minio:9000",
    "MINIO_ACCESS_KEY": "access",
    "MINIO_SECRET_KEY": "secret",
    "MINIO_BUCKET": "gtfs-ireland",
    "MINIO_REGION": "us-east-1",
    "ARCHIVE_PREFIX": "realtime",
    "ARCHIVE_TIMEZONE": "America/Sao_Paulo",
    "RETENTION_DAYS": "3",
}


class FakeDuckDBConnection:
    def __init__(self, row_count: int) -> None:
        self.statements: list[str] = []
        self._row_count = row_count

    def execute(self, statement: str) -> "FakeDuckDBConnection":
        self.statements.append(" ".join(statement.split()))
        return self

    def fetchone(self) -> tuple[int]:
        return (self._row_count,)

    def __enter__(self) -> "FakeDuckDBConnection":
        return self

    def __exit__(self, *_: object) -> None:
        return None


class FakeCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[Any, Any]] = []
        self.rowcount = 7

    def execute(self, statement: Any, parameters: Any = None) -> None:
        self.executed.append((statement, parameters))

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None


class FakePostgresConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def __enter__(self) -> "FakePostgresConnection":
        return self

    def __exit__(self, *_: object) -> None:
        return None


@pytest.fixture
def pipeline_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in PIPELINE_ENV.items():
        monkeypatch.setenv(key, value)


def test_quote_literal_escapes_embedded_quotes() -> None:
    assert _quote_literal("se'cret") == "'se''cret'"


def test_quote_identifier_escapes_embedded_quotes() -> None:
    assert _quote_identifier('tab"le') == '"tab""le"'


def test_timestamp_literal_keeps_the_local_utc_offset() -> None:
    value = datetime(2026, 9, 19, 18, 0, tzinfo=SAO_PAULO)

    assert _timestamp_literal(value) == "TIMESTAMPTZ '2026-09-19T18:00:00-03:00'"


def test_object_store_secret_is_scoped_to_the_configured_bucket(pipeline_env: None) -> None:
    config = PipelineConfig.from_env()
    connection = duckdb.connect()
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")

    _create_object_store_secret(connection, config)

    secrets = connection.execute(
        "SELECT name, type, scope FROM duckdb_secrets() WHERE name = 'object_store'"
    ).fetchall()
    assert secrets == [("object_store", "s3", ["s3://gtfs-ireland"])]


def test_export_writes_the_previous_hour_to_a_partitioned_parquet_object(
    pipeline_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = FakeDuckDBConnection(row_count=42)
    monkeypatch.setattr("pipelines.flows._duckdb_connection", lambda _config: connection)

    result = export_hourly_snapshots.fn(run_at=datetime(2026, 9, 19, 22, 5, tzinfo=UTC))

    assert result == {
        "rows": 42,
        "target": (
            "s3://gtfs-ireland/realtime/ingestion_date=2026-09-19/snapshots_20260919T18.parquet"
        ),
        "window_start": "2026-09-19T18:00:00-03:00",
    }

    copy_statement = connection.statements[-1]
    assert copy_statement.startswith("COPY (")
    assert 'FROM source."gtfs"."snapshots"' in copy_statement
    assert "\"timestamp\" >= TIMESTAMPTZ '2026-09-19T18:00:00-03:00'" in copy_statement
    assert "\"timestamp\" < TIMESTAMPTZ '2026-09-19T19:00:00-03:00'" in copy_statement
    assert "(FORMAT parquet, COMPRESSION zstd)" in copy_statement


def test_export_skips_writing_when_the_window_is_empty(
    pipeline_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = FakeDuckDBConnection(row_count=0)
    monkeypatch.setattr("pipelines.flows._duckdb_connection", lambda _config: connection)

    result = export_hourly_snapshots.fn(run_at=datetime(2026, 9, 19, 22, 5, tzinfo=UTC))

    assert result == {"rows": 0, "target": None, "window_start": "2026-09-19T18:00:00-03:00"}
    assert not any(statement.startswith("COPY") for statement in connection.statements)


def test_retention_deletes_rows_older_than_three_local_days(
    pipeline_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = FakePostgresConnection()
    monkeypatch.setattr("pipelines.flows.psycopg.connect", lambda _dsn: connection)

    result = enforce_retention.fn(run_at=datetime(2026, 9, 19, 4, 30, tzinfo=UTC))

    cutoff = datetime(2026, 9, 16, 0, 0, tzinfo=SAO_PAULO)
    assert result == {"deleted": 7, "cutoff": cutoff.isoformat()}

    statement, parameters = connection.cursor_instance.executed[0]
    assert parameters == (cutoff,)
    assert statement.as_string(None) == 'DELETE FROM "gtfs"."snapshots" WHERE "timestamp" < %s'
