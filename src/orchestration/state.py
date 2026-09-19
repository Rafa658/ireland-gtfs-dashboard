from datetime import UTC, datetime
from typing import Any, Protocol

from prefect.variables import Variable

EXPORT_CURSOR_VARIABLE = "gtfs_export_next_hour"
RETENTION_ENABLED_VARIABLE = "gtfs_retention_enabled"


class VariableApi(Protocol):
    @classmethod
    def get(cls, name: str, default: Any = None) -> Any: ...

    @classmethod
    def set(
        cls,
        name: str,
        value: Any,
        *,
        overwrite: bool = False,
    ) -> Any: ...


class PrefectCursorStore:
    def __init__(self, variable_api: type[VariableApi] = Variable) -> None:
        self._variables = variable_api

    def get_cursor(self) -> datetime | None:
        raw_value = self._variables.get(EXPORT_CURSOR_VARIABLE, default=None)
        if raw_value is None:
            return None
        if not isinstance(raw_value, str):
            raise ValueError(f"{EXPORT_CURSOR_VARIABLE} must be an ISO-8601 string")

        try:
            value = datetime.fromisoformat(raw_value)
        except ValueError as error:
            raise ValueError(
                f"{EXPORT_CURSOR_VARIABLE} must be an ISO-8601 timestamp"
            ) from error
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{EXPORT_CURSOR_VARIABLE} must include a UTC offset")
        if value.minute or value.second or value.microsecond:
            raise ValueError(f"{EXPORT_CURSOR_VARIABLE} must be aligned to an hour")
        return value.astimezone(UTC)

    def set_cursor(self, value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("export cursor must be timezone-aware")
        if value.minute or value.second or value.microsecond:
            raise ValueError("export cursor must be aligned to an hour")
        self._variables.set(
            EXPORT_CURSOR_VARIABLE,
            value.astimezone(UTC).isoformat(),
            overwrite=True,
        )

    def is_retention_enabled(self) -> bool:
        value = self._variables.get(RETENTION_ENABLED_VARIABLE, default=False)
        if not isinstance(value, bool):
            raise ValueError(f"{RETENTION_ENABLED_VARIABLE} must be a boolean")
        return value

    def set_retention_enabled(self, value: bool) -> None:
        self._variables.set(
            RETENTION_ENABLED_VARIABLE,
            value,
            overwrite=True,
        )
