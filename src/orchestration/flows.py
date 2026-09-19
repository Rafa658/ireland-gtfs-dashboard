import os
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

import requests
from prefect import flow, get_run_logger, task
from prefect.tasks import exponential_backoff

from orchestration.config import OrchestrationConfig
from orchestration.service import ArchiveService, ExportResult
from orchestration.state import PrefectCursorStore
from orchestration.storage import MinioParquetExporter, PostgresArchiveRepository


def validate_manual_range(
    start: datetime | None,
    end: datetime | None,
) -> tuple[datetime, datetime] | None:
    if start is None and end is None:
        return None
    if start is None or end is None:
        raise ValueError("start and end must both be provided for a manual export")
    if start >= end:
        raise ValueError("start must be before end")
    return start, end


def _build_service(
    config: OrchestrationConfig,
) -> ArchiveService:
    repository = PostgresArchiveRepository(config)
    exporter = MinioParquetExporter(config, repository)
    return ArchiveService(
        repository,
        exporter,
        PrefectCursorStore(),
        config.archive_timezone,
    )


def _notification_url() -> str | None:
    value = os.getenv("FAILURE_WEBHOOK_URL", "").strip()
    if not value:
        return None
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("FAILURE_WEBHOOK_URL must be an HTTPS URL")
    if parsed.username or parsed.password:
        raise ValueError("FAILURE_WEBHOOK_URL must not contain credentials")
    return value


def notify_failure(flow: Any, flow_run: Any, state: Any) -> None:
    webhook_url = _notification_url()
    if webhook_url is None:
        return

    response = requests.post(
        webhook_url,
        json={
            "event": "prefect_flow_failed",
            "flow": flow.name,
            "flow_run_id": str(flow_run.id),
            "flow_run_name": flow_run.name,
            "state": state.name,
        },
        timeout=10,
    )
    response.raise_for_status()


@task(
    name="export-gtfs-hours",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=300),
)
def export_hours_task(
    start: datetime | None = None,
    end: datetime | None = None,
) -> ExportResult:
    config = OrchestrationConfig.from_env()
    service = _build_service(config)
    manual_range = validate_manual_range(start, end)
    if manual_range is not None:
        return service.export_range(*manual_range)
    return service.export_scheduled(
        datetime.now(UTC),
        max_hours=config.archive_max_catchup_hours,
    )


@task(name="delete-expired-gtfs-rows", retries=2, retry_delay_seconds=900)
def retention_task() -> int:
    config = OrchestrationConfig.from_env()
    return _build_service(config).apply_retention(datetime.now(UTC))


@flow(name="gtfs-hourly-export", on_failure=[notify_failure])
def hourly_export_flow(
    start: datetime | None = None,
    end: datetime | None = None,
) -> dict[str, int]:
    result = export_hours_task(start, end)
    get_run_logger().info(
        "Export completed: %d windows, %d rows",
        result.processed_windows,
        result.exported_rows,
    )
    return {
        "processed_windows": result.processed_windows,
        "exported_rows": result.exported_rows,
    }


@flow(name="gtfs-daily-retention", on_failure=[notify_failure])
def retention_flow() -> dict[str, int]:
    deleted_rows = retention_task()
    get_run_logger().info("Retention completed: %d rows deleted", deleted_rows)
    return {"deleted_rows": deleted_rows}
