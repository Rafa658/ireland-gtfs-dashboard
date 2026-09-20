from prefect import serve
from prefect.deployments.runner import EntrypointType
from prefect.schedules import Cron

from pipelines.config import PipelineConfig
from pipelines.flows import enforce_retention, export_hourly_snapshots


def run() -> None:
    # Fail fast at startup rather than on the first scheduled run.
    timezone = PipelineConfig.from_env().archive_timezone

    serve(
        export_hourly_snapshots.to_deployment(
            name="hourly-parquet-export",
            schedule=Cron("5 * * * *", timezone=timezone),
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        enforce_retention.to_deployment(
            name="daily-retention",
            schedule=Cron("30 1 * * *", timezone=timezone),
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        limit=1,
    )


if __name__ == "__main__":
    run()
