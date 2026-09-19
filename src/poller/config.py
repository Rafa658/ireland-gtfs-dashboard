import os
from dataclasses import dataclass


class ConfigError(ValueError):
    """Raised when required application configuration is invalid."""


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"{name} is required")
    if value == "CHANGE_ME":
        raise ConfigError(f"{name} must be configured")
    return value


def _positive_int(name: str, *, maximum: int | None = None) -> int:
    raw_value = _required(name)
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ConfigError(f"{name} must be an integer") from error

    if value <= 0 or (maximum is not None and value > maximum):
        constraint = f" between 1 and {maximum}" if maximum is not None else " positive"
        raise ConfigError(f"{name} must be{constraint}")
    return value


@dataclass(frozen=True, slots=True)
class Config:
    api_key: str
    app_port: int
    poll_interval_seconds: int
    api_timeout_seconds: int
    postgres_connect_timeout_seconds: int
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_schema: str
    postgres_table: str
    postgres_sslmode: str
    log_level: str

    @classmethod
    def from_env(cls) -> "Config":
        log_level = _required("LOG_LEVEL").upper()
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ConfigError("LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")

        return cls(
            api_key=_required("API_KEY"),
            app_port=_positive_int("APP_PORT", maximum=65535),
            poll_interval_seconds=_positive_int("POLL_INTERVAL_SECONDS"),
            api_timeout_seconds=_positive_int("API_TIMEOUT_SECONDS"),
            postgres_connect_timeout_seconds=_positive_int("POSTGRES_CONNECT_TIMEOUT_SECONDS"),
            postgres_host=_required("POSTGRES_HOST"),
            postgres_port=_positive_int("POSTGRES_PORT", maximum=65535),
            postgres_db=_required("POSTGRES_DB"),
            postgres_user=_required("POSTGRES_USER"),
            postgres_password=_required("POSTGRES_PASSWORD"),
            postgres_schema=_required("POSTGRES_SCHEMA"),
            postgres_table=_required("POSTGRES_TABLE"),
            postgres_sslmode=_required("POSTGRES_SSLMODE"),
            log_level=log_level,
        )
