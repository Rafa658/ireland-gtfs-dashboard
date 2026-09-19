from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from orchestration.service import ArchiveService, InitializationResult
from orchestration.time_windows import HourWindow

SAO_PAULO = ZoneInfo("America/Sao_Paulo")
INITIAL_START = datetime(2026, 9, 19, 3, tzinfo=UTC)


class FakeCursorStore:
    def __init__(
        self,
        *,
        cursor: datetime | None = None,
        retention_enabled: bool | None = None,
    ) -> None:
        self.cursor = cursor
        self.retention_enabled = retention_enabled
        self.cursor_updates: list[datetime] = []

    def get_cursor(self) -> datetime | None:
        return self.cursor

    def set_cursor(self, value: datetime) -> None:
        self.cursor = value
        self.cursor_updates.append(value)

    def get_retention_enabled(self) -> bool | None:
        return self.retention_enabled

    def set_retention_enabled(self, value: bool) -> None:
        self.retention_enabled = value


class FakeExporter:
    def __init__(self, *, fail_at: datetime | None = None) -> None:
        self.fail_at = fail_at
        self.windows: list[HourWindow] = []

    def export(self, window: HourWindow) -> int:
        if window.start == self.fail_at:
            raise RuntimeError("export failed")
        self.windows.append(window)
        return 2


class FakeRepository:
    def __init__(self, earliest: datetime | None = None) -> None:
        self.earliest = earliest
        self.deleted_before: list[datetime] = []

    def earliest_timestamp(self) -> datetime | None:
        return self.earliest

    def delete_before(self, cutoff: datetime) -> int:
        self.deleted_before.append(cutoff)
        return 7


def test_initialization_sets_cursor_and_enables_retention_when_no_older_rows() -> None:
    cursor_store = FakeCursorStore()
    repository = FakeRepository(earliest=INITIAL_START)
    service = ArchiveService(repository, FakeExporter(), cursor_store, SAO_PAULO)

    result = service.initialize(INITIAL_START)

    assert result == InitializationResult(cursor=INITIAL_START, retention_enabled=True)
    assert cursor_store.cursor == INITIAL_START
    assert cursor_store.retention_enabled is True


def test_initialization_keeps_retention_disabled_when_older_rows_exist() -> None:
    cursor_store = FakeCursorStore()
    repository = FakeRepository(earliest=INITIAL_START - timedelta(hours=1))
    service = ArchiveService(repository, FakeExporter(), cursor_store, SAO_PAULO)

    result = service.initialize(INITIAL_START)

    assert result.retention_enabled is False
    assert cursor_store.retention_enabled is False


def test_initialization_preserves_a_manually_disabled_retention_flag() -> None:
    cursor_store = FakeCursorStore(
        cursor=INITIAL_START,
        retention_enabled=False,
    )
    repository = FakeRepository(earliest=INITIAL_START)
    service = ArchiveService(repository, FakeExporter(), cursor_store, SAO_PAULO)

    result = service.initialize(INITIAL_START)

    assert result.retention_enabled is False
    assert cursor_store.retention_enabled is False


def test_recreated_cursor_forces_a_fresh_retention_safety_check() -> None:
    cursor_store = FakeCursorStore(
        cursor=None,
        retention_enabled=True,
    )
    repository = FakeRepository(earliest=INITIAL_START - timedelta(hours=1))
    service = ArchiveService(repository, FakeExporter(), cursor_store, SAO_PAULO)

    result = service.initialize(INITIAL_START)

    assert result.retention_enabled is False
    assert cursor_store.cursor == INITIAL_START
    assert cursor_store.retention_enabled is False


def test_scheduled_export_processes_at_most_24_hours_and_advances_each_window() -> None:
    cursor_store = FakeCursorStore(cursor=INITIAL_START)
    exporter = FakeExporter()
    service = ArchiveService(FakeRepository(), exporter, cursor_store, SAO_PAULO)
    now = INITIAL_START + timedelta(hours=30, minutes=5)

    result = service.export_scheduled(now, max_hours=24)

    assert result.processed_windows == 24
    assert result.exported_rows == 48
    assert cursor_store.cursor == INITIAL_START + timedelta(hours=24)
    assert len(cursor_store.cursor_updates) == 24


def test_scheduled_export_does_not_advance_past_a_failed_window() -> None:
    failed_start = INITIAL_START + timedelta(hours=1)
    cursor_store = FakeCursorStore(cursor=INITIAL_START)
    exporter = FakeExporter(fail_at=failed_start)
    service = ArchiveService(FakeRepository(), exporter, cursor_store, SAO_PAULO)

    with pytest.raises(RuntimeError, match="export failed"):
        service.export_scheduled(INITIAL_START + timedelta(hours=3, minutes=5), max_hours=24)

    assert cursor_store.cursor == failed_start
    assert cursor_store.cursor_updates == [failed_start]


def test_manual_export_does_not_change_the_automatic_cursor() -> None:
    cursor_store = FakeCursorStore(cursor=INITIAL_START)
    exporter = FakeExporter()
    service = ArchiveService(FakeRepository(), exporter, cursor_store, SAO_PAULO)

    result = service.export_range(
        INITIAL_START - timedelta(hours=2),
        INITIAL_START,
    )

    assert result.processed_windows == 2
    assert cursor_store.cursor == INITIAL_START
    assert cursor_store.cursor_updates == []


def test_retention_is_a_noop_while_disabled() -> None:
    cursor_store = FakeCursorStore(cursor=INITIAL_START, retention_enabled=False)
    repository = FakeRepository()
    service = ArchiveService(repository, FakeExporter(), cursor_store, SAO_PAULO)

    assert service.apply_retention(INITIAL_START + timedelta(days=10)) == 0
    assert repository.deleted_before == []


def test_retention_uses_the_earlier_of_age_cutoff_and_export_cursor() -> None:
    cursor_store = FakeCursorStore(
        cursor=INITIAL_START + timedelta(hours=12),
        retention_enabled=True,
    )
    repository = FakeRepository()
    service = ArchiveService(repository, FakeExporter(), cursor_store, SAO_PAULO)
    now = INITIAL_START + timedelta(days=10)

    assert service.apply_retention(now) == 7
    assert repository.deleted_before == [INITIAL_START + timedelta(hours=12)]
