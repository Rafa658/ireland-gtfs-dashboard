from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from pipelines.windows import (
    hour_windows,
    object_key,
    previous_hour_window,
    retention_cutoff,
)

SAO_PAULO = ZoneInfo("America/Sao_Paulo")


def test_previous_hour_window_covers_the_last_completed_local_hour() -> None:
    now = datetime(2026, 9, 19, 19, 5, tzinfo=SAO_PAULO)

    start, end = previous_hour_window(now, SAO_PAULO)

    assert start == datetime(2026, 9, 19, 18, 0, tzinfo=SAO_PAULO)
    assert end == datetime(2026, 9, 19, 19, 0, tzinfo=SAO_PAULO)
    assert end - start == timedelta(hours=1)


def test_previous_hour_window_converts_utc_inputs_to_the_local_zone() -> None:
    now = datetime(2026, 9, 19, 22, 5, tzinfo=UTC)

    start, end = previous_hour_window(now, SAO_PAULO)

    assert start == datetime(2026, 9, 19, 18, 0, tzinfo=SAO_PAULO)
    assert end == datetime(2026, 9, 19, 19, 0, tzinfo=SAO_PAULO)
    assert start.utcoffset() == timedelta(hours=-3)


def test_previous_hour_window_rolls_back_across_local_midnight() -> None:
    now = datetime(2026, 9, 20, 0, 5, tzinfo=SAO_PAULO)

    start, end = previous_hour_window(now, SAO_PAULO)

    assert start == datetime(2026, 9, 19, 23, 0, tzinfo=SAO_PAULO)
    assert end == datetime(2026, 9, 20, 0, 0, tzinfo=SAO_PAULO)


def test_every_hour_of_the_day_exports_its_own_preceding_hour() -> None:
    """The window is derived from run time, so :05 of any hour covers that hour - 1."""
    for hour in range(24):
        now = datetime(2026, 9, 19, hour, 5, tzinfo=SAO_PAULO)

        start, end = previous_hour_window(now, SAO_PAULO)

        assert end == now.replace(minute=0)
        assert end - start == timedelta(hours=1)
        assert start.hour == (hour - 1) % 24


def test_retention_cutoff_keeps_the_configured_number_of_local_days() -> None:
    now = datetime(2026, 9, 19, 1, 30, tzinfo=SAO_PAULO)

    cutoff = retention_cutoff(now, SAO_PAULO, 3)

    assert cutoff == datetime(2026, 9, 16, 0, 0, tzinfo=SAO_PAULO)


def test_object_key_is_partitioned_by_local_ingestion_date() -> None:
    start = datetime(2026, 9, 19, 23, 0, tzinfo=SAO_PAULO)

    key = object_key("realtime", start, SAO_PAULO)

    assert key == "realtime/ingestion_date=2026-09-19/snapshots_20260919T23.parquet"


def test_object_key_uses_the_local_date_for_utc_inputs() -> None:
    start = datetime(2026, 9, 20, 2, 0, tzinfo=UTC)

    key = object_key("/realtime/", start, SAO_PAULO)

    assert key == "realtime/ingestion_date=2026-09-19/snapshots_20260919T23.parquet"


def test_naive_inputs_are_treated_as_utc() -> None:
    naive = datetime(2026, 9, 19, 22, 5)
    aware = datetime(2026, 9, 19, 22, 5, tzinfo=UTC)

    assert previous_hour_window(naive, SAO_PAULO) == previous_hour_window(aware, SAO_PAULO)
    assert retention_cutoff(naive, SAO_PAULO, 3) == retention_cutoff(aware, SAO_PAULO, 3)
    assert object_key("realtime", naive, SAO_PAULO) == object_key("realtime", aware, SAO_PAULO)


def test_hour_windows_are_contiguous_and_exclude_the_partial_final_hour() -> None:
    start = datetime(2026, 9, 19, 0, 0, tzinfo=SAO_PAULO)
    end = datetime(2026, 9, 19, 3, 40, tzinfo=SAO_PAULO)

    windows = hour_windows(start, end, SAO_PAULO)

    assert windows == [
        (start, datetime(2026, 9, 19, 1, 0, tzinfo=SAO_PAULO)),
        (
            datetime(2026, 9, 19, 1, 0, tzinfo=SAO_PAULO),
            datetime(2026, 9, 19, 2, 0, tzinfo=SAO_PAULO),
        ),
        (
            datetime(2026, 9, 19, 2, 0, tzinfo=SAO_PAULO),
            datetime(2026, 9, 19, 3, 0, tzinfo=SAO_PAULO),
        ),
    ]
    for (_, first_end), (second_start, _) in zip(windows, windows[1:], strict=False):
        assert first_end == second_start


def test_hour_windows_floor_the_start_to_align_with_the_schedule() -> None:
    windows = hour_windows(
        datetime(2026, 9, 19, 0, 47, tzinfo=SAO_PAULO),
        datetime(2026, 9, 19, 2, 0, tzinfo=SAO_PAULO),
        SAO_PAULO,
    )

    assert windows[0][0] == datetime(2026, 9, 19, 0, 0, tzinfo=SAO_PAULO)
    assert len(windows) == 2


def test_hour_windows_is_empty_when_no_whole_hour_has_elapsed() -> None:
    start = datetime(2026, 9, 19, 5, 0, tzinfo=SAO_PAULO)

    assert hour_windows(start, datetime(2026, 9, 19, 5, 59, tzinfo=SAO_PAULO), SAO_PAULO) == []
    assert hour_windows(start, datetime(2026, 9, 19, 4, 0, tzinfo=SAO_PAULO), SAO_PAULO) == []


def test_hour_windows_match_what_the_scheduled_export_would_have_produced() -> None:
    """Backfilled hours must be identical to the hours the :05 cron would have exported."""
    windows = hour_windows(
        datetime(2026, 9, 19, 0, 0, tzinfo=SAO_PAULO),
        datetime(2026, 9, 19, 6, 0, tzinfo=SAO_PAULO),
        SAO_PAULO,
    )

    for start, end in windows:
        scheduled_run = end + timedelta(minutes=5)
        assert previous_hour_window(scheduled_run, SAO_PAULO) == (start, end)
