from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from orchestration.flows import (
    export_hours_task,
    notify_failure,
    retention_task,
    validate_manual_range,
)


def test_accepts_scheduled_mode_when_no_manual_range_is_given() -> None:
    assert validate_manual_range(None, None) is None


def test_accepts_an_explicit_half_open_manual_range() -> None:
    start = datetime(2026, 9, 18, 3, tzinfo=UTC)
    end = datetime(2026, 9, 19, 3, tzinfo=UTC)

    assert validate_manual_range(start, end) == (start, end)


def test_rejects_a_partial_manual_range() -> None:
    with pytest.raises(ValueError, match="start and end"):
        validate_manual_range(datetime(2026, 9, 18, 3, tzinfo=UTC), None)


def test_rejects_an_empty_manual_range() -> None:
    instant = datetime(2026, 9, 19, 3, tzinfo=UTC)

    with pytest.raises(ValueError, match="before"):
        validate_manual_range(instant, instant)


def test_tasks_use_the_agreed_retry_policies() -> None:
    assert export_hours_task.retries == 3
    assert export_hours_task.retry_delay_seconds == [300, 600, 1200]
    assert retention_task.retries == 2
    assert retention_task.retry_delay_seconds == 900


def test_failure_notification_contains_no_exception_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, dict[str, str], int]] = []

    class Response:
        def raise_for_status(self) -> None:
            return None

    def post(url: str, *, json: dict[str, str], timeout: int) -> Response:
        posted.append((url, json, timeout))
        return Response()

    monkeypatch.setenv("FAILURE_WEBHOOK_URL", "https://alerts.example.com/prefect")
    monkeypatch.setattr("orchestration.flows.requests.post", post)

    notify_failure(
        SimpleNamespace(name="gtfs-hourly-export"),
        SimpleNamespace(id="run-id", name="run-name"),
        SimpleNamespace(name="Failed", message="database password leaked here"),
    )

    assert posted == [
        (
            "https://alerts.example.com/prefect",
            {
                "event": "prefect_flow_failed",
                "flow": "gtfs-hourly-export",
                "flow_run_id": "run-id",
                "flow_run_name": "run-name",
                "state": "Failed",
            },
            10,
        )
    ]
