from dataclasses import dataclass
from datetime import datetime, tzinfo
from typing import Protocol

from orchestration.time_windows import (
    HourWindow,
    complete_hour_end,
    iter_hour_windows,
    retention_cutoff,
)


class CursorStore(Protocol):
    def get_cursor(self) -> datetime | None: ...

    def set_cursor(self, value: datetime) -> None: ...

    def is_retention_enabled(self) -> bool: ...

    def set_retention_enabled(self, value: bool) -> None: ...


class HourExporter(Protocol):
    def export(self, window: HourWindow) -> int: ...


class RetentionRepository(Protocol):
    def earliest_timestamp(self) -> datetime | None: ...

    def delete_before(self, cutoff: datetime) -> int: ...


@dataclass(frozen=True, slots=True)
class InitializationResult:
    cursor: datetime
    retention_enabled: bool


@dataclass(frozen=True, slots=True)
class ExportResult:
    processed_windows: int
    exported_rows: int


class ArchiveService:
    def __init__(
        self,
        repository: RetentionRepository,
        exporter: HourExporter,
        cursor_store: CursorStore,
        schedule_timezone: tzinfo,
    ) -> None:
        self._repository = repository
        self._exporter = exporter
        self._cursor_store = cursor_store
        self._schedule_timezone = schedule_timezone

    def initialize(self, initial_start: datetime) -> InitializationResult:
        return initialize_archive_state(
            self._repository,
            self._cursor_store,
            initial_start,
        )

    def export_scheduled(self, now: datetime, *, max_hours: int) -> ExportResult:
        cursor = self._cursor_store.get_cursor()
        if cursor is None:
            raise RuntimeError("export cursor is not initialized")

        end = complete_hour_end(now, self._schedule_timezone)
        processed_windows = 0
        exported_rows = 0
        for window in iter_hour_windows(cursor, end, limit=max_hours):
            exported_rows += self._exporter.export(window)
            self._cursor_store.set_cursor(window.end)
            processed_windows += 1

        return ExportResult(
            processed_windows=processed_windows,
            exported_rows=exported_rows,
        )

    def export_range(self, start: datetime, end: datetime) -> ExportResult:
        processed_windows = 0
        exported_rows = 0
        for window in iter_hour_windows(start, end):
            exported_rows += self._exporter.export(window)
            processed_windows += 1

        return ExportResult(
            processed_windows=processed_windows,
            exported_rows=exported_rows,
        )

    def apply_retention(self, now: datetime) -> int:
        if not self._cursor_store.is_retention_enabled():
            return 0

        cursor = self._cursor_store.get_cursor()
        if cursor is None:
            raise RuntimeError("export cursor is not initialized")

        return self._repository.delete_before(retention_cutoff(now, cursor))


def initialize_archive_state(
    repository: RetentionRepository,
    cursor_store: CursorStore,
    initial_start: datetime,
) -> InitializationResult:
    cursor = cursor_store.get_cursor()
    if cursor is None:
        cursor = initial_start
        cursor_store.set_cursor(cursor)

    earliest = repository.earliest_timestamp()
    retention_enabled = earliest is None or earliest >= initial_start
    cursor_store.set_retention_enabled(retention_enabled)
    return InitializationResult(cursor=cursor, retention_enabled=retention_enabled)
