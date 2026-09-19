from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from orchestration.time_windows import (
    HourWindow,
    complete_hour_end,
    hourly_object_key,
    iter_hour_windows,
    retention_cutoff,
)

SAO_PAULO = ZoneInfo("America/Sao_Paulo")


def test_complete_hour_end_rounds_down_in_schedule_timezone() -> None:
    now = datetime(2026, 9, 19, 15, 5, 37, tzinfo=UTC)

    assert complete_hour_end(now, SAO_PAULO) == datetime(2026, 9, 19, 15, tzinfo=UTC)


def test_iter_hour_windows_returns_a_bounded_sequence() -> None:
    start = datetime(2026, 9, 19, 3, tzinfo=UTC)
    end = datetime(2026, 9, 19, 7, tzinfo=UTC)

    assert list(iter_hour_windows(start, end, limit=2)) == [
        HourWindow(start=start, end=start + timedelta(hours=1)),
        HourWindow(
            start=start + timedelta(hours=1),
            end=start + timedelta(hours=2),
        ),
    ]


@pytest.mark.parametrize("limit", [0, -1])
def test_iter_hour_windows_rejects_non_positive_limits(limit: int) -> None:
    start = datetime(2026, 9, 19, 3, tzinfo=UTC)

    with pytest.raises(ValueError, match="limit"):
        list(iter_hour_windows(start, start + timedelta(hours=1), limit=limit))


def test_hourly_object_key_uses_local_date_and_utc_instant() -> None:
    window = HourWindow(
        start=datetime(2026, 9, 19, 2, tzinfo=UTC),
        end=datetime(2026, 9, 19, 3, tzinfo=UTC),
    )

    assert hourly_object_key(window, SAO_PAULO, prefix="realtime") == (
        "realtime/ingestion_date=2026-09-18/hour_start=2026-09-19T02-00-00Z.parquet"
    )


def test_retention_cutoff_uses_rolling_72_hours_when_export_is_current() -> None:
    now = datetime(2026, 9, 22, 3, tzinfo=UTC)
    export_cursor = datetime(2026, 9, 22, 2, tzinfo=UTC)

    assert retention_cutoff(now, export_cursor) == datetime(2026, 9, 19, 3, tzinfo=UTC)


def test_retention_cutoff_stops_at_lagging_export_cursor() -> None:
    now = datetime(2026, 9, 22, 3, tzinfo=UTC)
    export_cursor = datetime(2026, 9, 18, 18, tzinfo=UTC)

    assert retention_cutoff(now, export_cursor) == export_cursor


@pytest.mark.parametrize(
    "value",
    [
        datetime(2026, 9, 19, 3),
        datetime(2026, 9, 19, 3, 1, tzinfo=UTC),
    ],
)
def test_hour_window_rejects_naive_or_unaligned_boundaries(value: datetime) -> None:
    with pytest.raises(ValueError):
        HourWindow(start=value, end=datetime(2026, 9, 19, 4, tzinfo=UTC))
