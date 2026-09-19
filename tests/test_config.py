import pytest

from poller.config import Config, ConfigError

REQUIRED_ENV = {
    "API_KEY": "secret-key",
    "APP_PORT": "1000",
    "POLL_INTERVAL_SECONDS": "120",
    "API_TIMEOUT_SECONDS": "30",
    "POSTGRES_CONNECT_TIMEOUT_SECONDS": "10",
    "POSTGRES_HOST": "db.example.com",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "transport",
    "POSTGRES_USER": "poller",
    "POSTGRES_PASSWORD": "secret-password",
    "POSTGRES_SCHEMA": "gtfs",
    "POSTGRES_TABLE": "snapshots",
    "POSTGRES_SSLMODE": "prefer",
    "LOG_LEVEL": "INFO",
}


def set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)


def test_loads_valid_environment_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    set_required_env(monkeypatch)

    config = Config.from_env()

    assert config.api_key == "secret-key"
    assert config.app_port == 1000
    assert config.poll_interval_seconds == 120
    assert config.postgres_schema == "gtfs"
    assert config.postgres_table == "snapshots"


def test_rejects_a_missing_required_value(monkeypatch: pytest.MonkeyPatch) -> None:
    set_required_env(monkeypatch)
    monkeypatch.delenv("API_KEY")

    with pytest.raises(ConfigError, match="API_KEY"):
        Config.from_env()


@pytest.mark.parametrize(
    "name",
    [
        "API_KEY",
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ],
)
def test_rejects_unchanged_template_placeholders(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    set_required_env(monkeypatch)
    monkeypatch.setenv(name, "CHANGE_ME")

    with pytest.raises(ConfigError, match=name):
        Config.from_env()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("APP_PORT", "0"),
        ("POLL_INTERVAL_SECONDS", "-1"),
        ("API_TIMEOUT_SECONDS", "not-a-number"),
        ("POSTGRES_CONNECT_TIMEOUT_SECONDS", "0"),
        ("POSTGRES_PORT", "70000"),
    ],
)
def test_rejects_invalid_numeric_values(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    set_required_env(monkeypatch)
    monkeypatch.setenv(name, value)

    with pytest.raises(ConfigError, match=name):
        Config.from_env()
