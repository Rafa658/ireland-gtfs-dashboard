from datetime import UTC, datetime

import psycopg
from prefect import flow
from psycopg import sql

from pipelines import archive
from pipelines.config import PipelineConfig
from pipelines.windows import retention_cutoff


@flow(name="enforce-retention")
def enforce_retention(run_at: datetime | None = None) -> dict[str, object]:
    """Delete snapshots older than the retention window, measured in whole local days."""
    logger = archive.get_flow_logger()
    config = PipelineConfig.from_env()

    now = run_at or datetime.now(tz=UTC)
    cutoff = retention_cutoff(now, config.tzinfo, config.retention_days)

    statement = sql.SQL('DELETE FROM {}.{} WHERE "timestamp" < %s').format(
        sql.Identifier(config.postgres_schema),
        sql.Identifier(config.postgres_table),
    )

    with (
        archive.archive_slot(),
        psycopg.connect(config.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(statement, (cutoff,))
        deleted = cursor.rowcount

    logger.info("Deleted %s snapshots older than %s", deleted, cutoff.isoformat())
    return {"deleted": deleted, "cutoff": cutoff.isoformat()}
