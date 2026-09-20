from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from pipelines.windows import object_key, previous_hour_window, retention_cutoff

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
