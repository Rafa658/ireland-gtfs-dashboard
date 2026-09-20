import os
from dataclasses import dataclass
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_ARCHIVE_TIMEZONE = "America/Sao_Paulo"
DEFAULT_RETENTION_DAYS = 3


class ConfigError(ValueError):
    """Raised when required pipeline configuration is invalid."""


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"{name} is required")
    if value == "CHANGE_ME":
        raise ConfigError(f"{name} must be configured")
    return value


def _optional(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value or default


def _positive_int(name: str, *, default: int | None = None, maximum: int | None = None) -> int:
    raw_value = os.getenv(name, "").strip()
    if not raw_value and default is not None:
        return default

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
class PipelineConfig:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_schema: str
    postgres_table: str
    postgres_sslmode: str
    postgres_connect_timeout_seconds: int
    minio_endpoint: str
    minio_use_ssl: bool
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_region: str
    archive_prefix: str
    archive_timezone: str
    retention_days: int

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        endpoint_url = _required("MINIO_ENDPOINT_URL")
        parsed = urlparse(endpoint_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ConfigError("MINIO_ENDPOINT_URL must be an http(s) URL, e.g. http://minio:9000")

        archive_timezone = _optional("ARCHIVE_TIMEZONE", DEFAULT_ARCHIVE_TIMEZONE)
        try:
            ZoneInfo(archive_timezone)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ConfigError(
                f"ARCHIVE_TIMEZONE {archive_timezone!r} is not a known zone"
            ) from error

        return cls(
            postgres_host=_required("POSTGRES_HOST"),
            postgres_port=_positive_int("POSTGRES_PORT", maximum=65535),
            postgres_db=_required("POSTGRES_DB"),
            postgres_user=_required("POSTGRES_USER"),
            postgres_password=_required("POSTGRES_PASSWORD"),
            postgres_schema=_required("POSTGRES_SCHEMA"),
            postgres_table=_required("POSTGRES_TABLE"),
            postgres_sslmode=_required("POSTGRES_SSLMODE"),
            postgres_connect_timeout_seconds=_positive_int(
                "POSTGRES_CONNECT_TIMEOUT_SECONDS", default=10
            ),
            minio_endpoint=parsed.netloc,
            minio_use_ssl=parsed.scheme == "https",
            minio_access_key=_required("MINIO_ACCESS_KEY"),
            minio_secret_key=_required("MINIO_SECRET_KEY"),
            minio_bucket=_required("MINIO_BUCKET"),
            minio_region=_optional("MINIO_REGION", "us-east-1"),
            archive_prefix=_required("ARCHIVE_PREFIX"),
            archive_timezone=archive_timezone,
            retention_days=_positive_int("RETENTION_DAYS", default=DEFAULT_RETENTION_DAYS),
        )

    @property
    def tzinfo(self) -> ZoneInfo:
        return ZoneInfo(self.archive_timezone)

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password} "
            f"sslmode={self.postgres_sslmode} "
            f"connect_timeout={self.postgres_connect_timeout_seconds}"
        )
