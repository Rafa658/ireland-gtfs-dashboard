from datetime import UTC, datetime
from typing import Any

import pytest

from orchestration.state import (
    EXPORT_CURSOR_VARIABLE,
    RETENTION_ENABLED_VARIABLE,
    PrefectCursorStore,
)


class FakeVariables:
    values: dict[str, Any] = {}

    @classmethod
    def get(cls, name: str, default: Any = None) -> Any:
        return cls.values.get(name, default)

    @classmethod
    def set(
        cls,
        name: str,
        value: Any,
        *,
        overwrite: bool = False,
    ) -> None:
        if name in cls.values and not overwrite:
            raise ValueError("already exists")
        cls.values[name] = value


@pytest.fixture(autouse=True)
def clear_variables() -> None:
    FakeVariables.values = {}


def test_round_trips_the_export_cursor_as_a_utc_iso_timestamp() -> None:
    store = PrefectCursorStore(variable_api=FakeVariables)
    cursor = datetime(2026, 9, 19, 3, tzinfo=UTC)

    store.set_cursor(cursor)

    assert FakeVariables.values[EXPORT_CURSOR_VARIABLE] == "2026-09-19T03:00:00+00:00"
    assert store.get_cursor() == cursor


def test_returns_none_when_export_cursor_is_missing() -> None:
    store = PrefectCursorStore(variable_api=FakeVariables)

    assert store.get_cursor() is None


def test_rejects_an_invalid_export_cursor_value() -> None:
    FakeVariables.values[EXPORT_CURSOR_VARIABLE] = "not-a-timestamp"
    store = PrefectCursorStore(variable_api=FakeVariables)

    with pytest.raises(ValueError, match=EXPORT_CURSOR_VARIABLE):
        store.get_cursor()


def test_round_trips_retention_enabled_as_a_boolean() -> None:
    store = PrefectCursorStore(variable_api=FakeVariables)

    store.set_retention_enabled(True)

    assert FakeVariables.values[RETENTION_ENABLED_VARIABLE] is True
    assert store.is_retention_enabled() is True


def test_rejects_a_non_boolean_retention_flag() -> None:
    FakeVariables.values[RETENTION_ENABLED_VARIABLE] = "true"
    store = PrefectCursorStore(variable_api=FakeVariables)

    with pytest.raises(ValueError, match=RETENTION_ENABLED_VARIABLE):
        store.is_retention_enabled()
