from collections.abc import Callable
from datetime import datetime
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from poller.api_client import ApiPayload
from poller.config import Config


class PostgresRepository:
    def __init__(
        self,
        config: Config,
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

    def ensure_storage(self) -> None:
        create_schema = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(self._config.postgres_schema)
        )
        create_table = sql.SQL(
            """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    timestamp TIMESTAMPTZ NOT NULL,
                    header JSONB NOT NULL,
                    entity JSONB NULL
                )
                """
        ).format(
            sql.Identifier(self._config.postgres_schema),
            sql.Identifier(self._config.postgres_table),
        )

        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(create_schema)
            cursor.execute(create_table)

    def insert(self, ingested_at: datetime, payload: ApiPayload) -> None:
        insert_row = sql.SQL(
            """
            INSERT INTO {}.{} (timestamp, header, entity)
            VALUES (%s, %s, %s)
            """
        ).format(
            sql.Identifier(self._config.postgres_schema),
            sql.Identifier(self._config.postgres_table),
        )
        entity = Jsonb(payload.entity) if payload.entity_present else None

        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(insert_row, (ingested_at, Jsonb(payload.header), entity))
