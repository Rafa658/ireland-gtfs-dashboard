from datetime import UTC, datetime
from threading import Event

from fastapi.testclient import TestClient

from poller.health import HealthState, create_app


class FakeRunner:
    def __init__(self) -> None:
        self.started = Event()
        self.stopped = Event()

    def run(self) -> None:
        self.started.set()
        self.stopped.wait(timeout=1)

    def stop(self) -> None:
        self.stopped.set()


def test_health_is_healthy_before_first_cycle() -> None:
    state = HealthState()
    app = create_app(FakeRunner(), state)

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "starting",
        "last_attempt_at": None,
        "last_success_at": None,
    }


def test_health_returns_503_after_latest_cycle_failure() -> None:
    state = HealthState()
    attempted_at = datetime(2026, 9, 19, 3, 39, tzinfo=UTC)
    state.mark_attempt(attempted_at)
    state.mark_failure()
    app = create_app(FakeRunner(), state)

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unhealthy",
        "last_attempt_at": "2026-09-19T03:39:00+00:00",
        "last_success_at": None,
    }
    assert "error" not in response.json()


def test_fastapi_lifespan_starts_and_stops_the_runner() -> None:
    runner = FakeRunner()
    app = create_app(runner, HealthState())

    with TestClient(app):
        assert runner.started.wait(timeout=1)

    assert runner.stopped.is_set()
