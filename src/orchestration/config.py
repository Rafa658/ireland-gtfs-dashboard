import os
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class OrchestrationConfigError(ValueError):
    """Raised when orchestration configuration is invalid."""


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise OrchestrationConfigError(f"{name} is required")
    if value == "CHANGE_ME":
        raise OrchestrationConfigError(f"{name} must be configured")
    return value


def _positive_int(name: str, *, maximum: int | None = None) -> int:
    raw_value = _required(name)
    try:
        value = int(raw_value)
    except ValueError as error:
        raise OrchestrationConfigError(f"{name} must be an integer") from error

    if value <= 0 or (maximum is not None and value > maximum):
        constraint = f" between 1 and {maximum}" if maximum is not None else " positive"
        raise OrchestrationConfigError(f"{name} must be{constraint}")
    return value


def _endpoint_url(name: str) -> str:
    value = _required(name)
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise OrchestrationConfigError(f"{name} must be an HTTP or HTTPS URL")
    if parsed.username or parsed.password:
        raise OrchestrationConfigError(f"{name} must not contain credentials")
    return value.rstrip("/")


def _optional_endpoint_url(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    if not value:
        return None
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise OrchestrationConfigError(f"{name} must be an HTTP or HTTPS URL")
    if parsed.username or parsed.password:
        raise OrchestrationConfigError(f"{name} must not contain credentials")
    return value


def _aware_datetime(name: str) -> datetime:
    raw_value = _required(name)
    try:
        value = datetime.fromisoformat(raw_value)
    except ValueError as error:
        raise OrchestrationConfigError(f"{name} must be an ISO-8601 timestamp") from error
    if value.tzinfo is None or value.utcoffset() is None:
        raise OrchestrationConfigError(f"{name} must include a UTC offset")
    if value.minute or value.second or value.microsecond:
        raise OrchestrationConfigError(f"{name} must be aligned to an hour")
    return value


@dataclass(frozen=True, slots=True)
class OrchestrationConfig:
    postgres_connect_timeout_seconds: int
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_schema: str
    postgres_table: str
    postgres_sslmode: str
    minio_endpoint_url: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_region: str
    archive_prefix: str
    archive_timezone_name: str
    archive_initial_start: datetime
    archive_max_catchup_hours: int
    failure_webhook_url: str | None

    @property
    def archive_timezone(self) -> ZoneInfo:
        try:
            return ZoneInfo(self.archive_timezone_name)
        except ZoneInfoNotFoundError as error:
            raise OrchestrationConfigError(
                f"ARCHIVE_TIMEZONE is unknown: {self.archive_timezone_name}"
            ) from error

    @classmethod
    def from_env(cls) -> "OrchestrationConfig":
        timezone_name = _required("ARCHIVE_TIMEZONE")
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as error:
            raise OrchestrationConfigError(
                f"ARCHIVE_TIMEZONE is unknown: {timezone_name}"
            ) from error

        return cls(
            postgres_connect_timeout_seconds=_positive_int(
                "POSTGRES_CONNECT_TIMEOUT_SECONDS"
            ),
            postgres_host=_required("POSTGRES_HOST"),
            postgres_port=_positive_int("POSTGRES_PORT", maximum=65535),
            postgres_db=_required("POSTGRES_DB"),
            postgres_user=_required("POSTGRES_USER"),
            postgres_password=_required("POSTGRES_PASSWORD"),
            postgres_schema=_required("POSTGRES_SCHEMA"),
            postgres_table=_required("POSTGRES_TABLE"),
            postgres_sslmode=_required("POSTGRES_SSLMODE"),
            minio_endpoint_url=_endpoint_url("MINIO_ENDPOINT_URL"),
            minio_access_key=_required("MINIO_ACCESS_KEY"),
            minio_secret_key=_required("MINIO_SECRET_KEY"),
            minio_bucket=_required("MINIO_BUCKET"),
            minio_region=os.getenv("MINIO_REGION", "us-east-1").strip() or "us-east-1",
            archive_prefix=_required("ARCHIVE_PREFIX").strip("/"),
            archive_timezone_name=timezone_name,
            archive_initial_start=_aware_datetime("ARCHIVE_INITIAL_START"),
            archive_max_catchup_hours=_positive_int("ARCHIVE_MAX_CATCHUP_HOURS"),
            failure_webhook_url=_optional_endpoint_url("FAILURE_WEBHOOK_URL"),
        )
