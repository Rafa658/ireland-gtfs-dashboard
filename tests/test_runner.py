from datetime import UTC, datetime

from poller.api_client import ApiPayload
from poller.health import HealthState
from poller.runner import PollingRunner


class StubApiClient:
    def __init__(self, payload: ApiPayload) -> None:
        self.payload = payload
        self.fetch_count = 0

    def fetch(self) -> ApiPayload:
        self.fetch_count += 1
        return self.payload


class StubRepository:
    def __init__(self, setup_error: Exception | None = None) -> None:
        self.setup_error = setup_error
        self.ensure_count = 0
        self.insertions: list[tuple[datetime, ApiPayload]] = []

    def ensure_storage(self) -> None:
        self.ensure_count += 1
        if self.setup_error is not None:
            raise self.setup_error

    def insert(self, ingested_at: datetime, payload: ApiPayload) -> None:
        self.insertions.append((ingested_at, payload))


def test_successful_cycle_creates_storage_fetches_and_inserts() -> None:
    payload = ApiPayload(header={"version": "2"}, entity=[], entity_present=True)
    api_client = StubApiClient(payload)
    repository = StubRepository()
    health = HealthState()
    now = datetime(2026, 9, 19, 3, 39, tzinfo=UTC)
    runner = PollingRunner(
        api_client,
        repository,
        health,
        poll_interval_seconds=120,
        clock=lambda: now,
    )

    succeeded = runner.run_cycle()

    assert succeeded is True
    assert repository.ensure_count == 1
    assert api_client.fetch_count == 1
    assert repository.insertions == [(now, payload)]
    assert health.snapshot() == {
        "status": "healthy",
        "last_attempt_at": "2026-09-19T03:39:00+00:00",
        "last_success_at": "2026-09-19T03:39:00+00:00",
    }


def test_database_setup_failure_skips_api_call_and_marks_unhealthy() -> None:
    api_client = StubApiClient(ApiPayload(header={}, entity=None, entity_present=False))
    repository = StubRepository(setup_error=ConnectionError("database unavailable"))
    health = HealthState()
    runner = PollingRunner(
        api_client,
        repository,
        health,
        poll_interval_seconds=120,
        clock=lambda: datetime(2026, 9, 19, 3, 39, tzinfo=UTC),
    )

    succeeded = runner.run_cycle()

    assert succeeded is False
    assert api_client.fetch_count == 0
    assert repository.insertions == []
    assert health.snapshot()["status"] == "unhealthy"


def test_storage_setup_runs_once_after_becoming_ready() -> None:
    payload = ApiPayload(header={}, entity=None, entity_present=False)
    repository = StubRepository()
    runner = PollingRunner(
        StubApiClient(payload),
        repository,
        HealthState(),
        poll_interval_seconds=120,
    )

    runner.run_cycle()
    runner.run_cycle()

    assert repository.ensure_count == 1
    assert len(repository.insertions) == 2
