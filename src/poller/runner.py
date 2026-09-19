from collections.abc import Callable
from datetime import UTC, datetime
from threading import Event
from typing import Protocol

from loguru import logger

from poller.api_client import ApiPayload
from poller.health import HealthState


class ApiClientProtocol(Protocol):
    def fetch(self) -> ApiPayload: ...


class RepositoryProtocol(Protocol):
    def ensure_storage(self) -> None: ...

    def insert(self, ingested_at: datetime, payload: ApiPayload) -> None: ...


class PollingRunner:
    def __init__(
        self,
        api_client: ApiClientProtocol,
        repository: RepositoryProtocol,
        health_state: HealthState,
        poll_interval_seconds: int,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._api_client = api_client
        self._repository = repository
        self._health_state = health_state
        self._poll_interval_seconds = poll_interval_seconds
        self._clock = clock or (lambda: datetime.now(UTC))
        self._stop_event = Event()
        self._storage_ready = False

    def run(self) -> None:
        logger.bind(event="worker_started").info("Polling worker started")
        while not self._stop_event.is_set():
            self.run_cycle()
            self._stop_event.wait(self._poll_interval_seconds)
        logger.bind(event="worker_stopped").info("Polling worker stopped")

    def stop(self) -> None:
        self._stop_event.set()

    def run_cycle(self) -> bool:
        attempted_at = self._clock()
        self._health_state.mark_attempt(attempted_at)

        if not self._storage_ready:
            try:
                self._repository.ensure_storage()
                self._storage_ready = True
            except Exception as error:
                self._mark_failure("database_setup", error)
                return False

        try:
            payload = self._api_client.fetch()
        except Exception as error:
            self._mark_failure("api_fetch", error)
            return False

        ingested_at = self._clock()
        try:
            self._repository.insert(ingested_at, payload)
        except Exception as error:
            self._storage_ready = False
            self._mark_failure("database_insert", error)
            return False

        self._health_state.mark_success(ingested_at)
        logger.bind(event="poll_succeeded", ingested_at=ingested_at.isoformat()).info(
            "Poll cycle completed"
        )
        return True

    def _mark_failure(self, operation: str, error: Exception) -> None:
        self._health_state.mark_failure()
        logger.bind(
            event="poll_failed",
            operation=operation,
            error_type=type(error).__name__,
        ).error("Poll cycle failed")
