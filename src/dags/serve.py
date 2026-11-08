from datetime import timedelta

from dotenv import load_dotenv
from prefect import get_client, serve
from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
from prefect.deployments.runner import EntrypointType
from prefect.schedules import Cron, Interval

from dags.dbt import DBT_LIMIT_NAME, dbt_ci, dbt_prod
from dags.hourly_export import export_hourly_snapshots
from dags.poll_gtfs import poll_gtfs
from dags.retention import enforce_retention
from pipelines.archive import ARCHIVE_LIMIT_NAME
from pipelines.config import PipelineConfig
from poller.config import Config

DBT_TIMEZONE = "America/Sao_Paulo"
# Polling, one archive job, dbt (prod or CI), and headroom for jobs waiting on their slot.
MAX_CONCURRENT_RUNS = 6


def ensure_concurrency_limits() -> None:
    with get_client(sync_client=True) as client:
        for name in (ARCHIVE_LIMIT_NAME, DBT_LIMIT_NAME):
            client.upsert_global_concurrency_limit_by_name(name, limit=1)


def build_deployments(poll_interval_seconds: int, archive_timezone: str) -> list:
    module_path = EntrypointType.MODULE_PATH
    return [
        poll_gtfs.to_deployment(
            name="gtfs-poll",
            schedule=Interval(timedelta(seconds=poll_interval_seconds)),
            # A run that is still going when the next slot arrives drops the new slot.
            concurrency_limit=ConcurrencyLimitConfig(
                limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
            ),
            entrypoint_type=module_path,
        ),
        export_hourly_snapshots.to_deployment(
            name="hourly-parquet-export",
            schedule=Cron("5 * * * *", timezone=archive_timezone),
            entrypoint_type=module_path,
        ),
        enforce_retention.to_deployment(
            name="daily-retention",
            schedule=Cron("30 1 * * *", timezone=archive_timezone),
            entrypoint_type=module_path,
        ),
        dbt_prod.to_deployment(
            name="dbt-prod",
            schedule=Cron("0 4 * * *", timezone=DBT_TIMEZONE),
            entrypoint_type=module_path,
        ),
        dbt_ci.to_deployment(name="dbt-ci", entrypoint_type=module_path),
    ]


def run() -> None:
    load_dotenv(dotenv_path=".env")
    # Fail fast at startup rather than on the first scheduled run.
    poll_interval_seconds = Config.from_env().poll_interval_seconds
    archive_timezone = PipelineConfig.from_env().archive_timezone

    ensure_concurrency_limits()
    serve(
        *build_deployments(poll_interval_seconds, archive_timezone),
        limit=MAX_CONCURRENT_RUNS,
    )


if __name__ == "__main__":
    run()
