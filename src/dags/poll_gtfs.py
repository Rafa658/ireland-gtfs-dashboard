from datetime import UTC, datetime

from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from prefect.states import Completed, State

from poller.api_client import ApiClient
from poller.config import Config
from worker.repository import PostgresRepository


def _lateness_seconds() -> float:
    """How long after its scheduled slot this run started; manual runs are never late."""
    scheduled = flow_run.scheduled_start_time
    if scheduled is None:
        return 0.0
    return (datetime.now(UTC) - scheduled).total_seconds()


@flow(name="poll-gtfs", retries=0)
def poll_gtfs() -> dict[str, str] | State:
    """Fetch the GTFS-realtime feed once and persist it as one snapshot.

    Slots delayed by a full polling interval (runner downtime, saturation) are skipped
    rather than replayed, so an outage never produces a burst of near-identical snapshots.
    """
    logger = get_run_logger()
    config = Config.from_env()

    lateness = _lateness_seconds()
    if lateness >= config.poll_interval_seconds:
        message = f"Skipped stale poll; its slot was {lateness:.0f}s late"
        logger.warning(message)
        return Completed(name="Skipped", message=message)

    repository = PostgresRepository(config)
    operation = "database_setup"
    try:
        repository.ensure_storage()
        operation = "api_fetch"
        payload = ApiClient(config.api_key, config.api_timeout_seconds).fetch()
        operation = "database_insert"
        ingested_at = datetime.now(UTC)
        repository.insert(ingested_at, payload)
    except Exception as error:
        logger.error("Poll failed during %s (%s)", operation, type(error).__name__)
        raise

    logger.info("Poll cycle completed")
    return {"ingested_at": ingested_at.isoformat()}
