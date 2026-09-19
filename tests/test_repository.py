from contextlib import AbstractContextManager
from datetime import UTC, datetime
from typing import Any

from psycopg import sql
from psycopg.types.json import Jsonb

from poller.api_client import ApiPayload
from poller.config import Config
from worker.repository import PostgresRepository


class FakeCursor(AbstractContextManager["FakeCursor"]):
    def __init__(self) -> None:
        self.executions: list[tuple[Any, Any]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, query: Any, params: Any = None) -> None:
        self.executions.append((query, params))


class FakeConnection(AbstractContextManager["FakeConnection"]):
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self.cursor_instance


def config() -> Config:
    return Config(
        api_key="secret",
        app_port=1000,
        poll_interval_seconds=120,
        api_timeout_seconds=30,
        postgres_connect_timeout_seconds=10,
        postgres_host="db.example.com",
        postgres_port=5432,
        postgres_db="transport",
        postgres_user="poller",
        postgres_password="password",
        postgres_schema="gtfs-data",
        postgres_table="vehicle snapshots",
        postgres_sslmode="prefer",
        log_level="INFO",
    )


def test_creates_missing_schema_and_table_with_safe_identifiers() -> None:
    connection = FakeConnection()
    connect_calls: list[dict[str, Any]] = []

    def connect(**kwargs: Any) -> FakeConnection:
        connect_calls.append(kwargs)
        return connection

    repository = PostgresRepository(config(), connect=connect)

    repository.ensure_storage()

    assert connect_calls == [
        {
            "host": "db.example.com",
            "port": 5432,
            "dbname": "transport",
            "user": "poller",
            "password": "password",
            "sslmode": "prefer",
            "connect_timeout": 10,
        }
    ]
    assert connection.cursor_instance.executions == [
        (
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier("gtfs-data")),
            None,
        ),
        (
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    timestamp TIMESTAMPTZ NOT NULL,
                    header JSONB NOT NULL,
                    entity JSONB NULL
                )
                """
            ).format(sql.Identifier("gtfs-data"), sql.Identifier("vehicle snapshots")),
            None,
        ),
    ]


def test_inserts_missing_entity_as_sql_null() -> None:
    connection = FakeConnection()
    repository = PostgresRepository(config(), connect=lambda **_: connection)
    ingested_at = datetime(2026, 9, 19, tzinfo=UTC)

    repository.insert(
        ingested_at,
        ApiPayload(header={"version": "2"}, entity=None, entity_present=False),
    )

    _, params = connection.cursor_instance.executions[0]
    assert params[0] == ingested_at
    assert isinstance(params[1], Jsonb)
    assert params[1].obj == {"version": "2"}
    assert params[2] is None


def test_inserts_explicit_json_null_as_jsonb_null() -> None:
    connection = FakeConnection()
    repository = PostgresRepository(config(), connect=lambda **_: connection)

    repository.insert(
        datetime(2026, 9, 19, tzinfo=UTC),
        ApiPayload(header=None, entity=None, entity_present=True),
    )

    _, params = connection.cursor_instance.executions[0]
    assert isinstance(params[1], Jsonb)
    assert params[1].obj is None
    assert isinstance(params[2], Jsonb)
    assert params[2].obj is None
