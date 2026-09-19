import pytest

from orchestration.config import OrchestrationConfig, OrchestrationConfigError

REQUIRED_ENV = {
    "POSTGRES_CONNECT_TIMEOUT_SECONDS": "10",
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "transport",
    "POSTGRES_USER": "poller",
    "POSTGRES_PASSWORD": "secret-password",
    "POSTGRES_SCHEMA": "gtfs",
    "POSTGRES_TABLE": "snapshots",
    "POSTGRES_SSLMODE": "prefer",
    "MINIO_ENDPOINT_URL": "http://minio:9000",
    "MINIO_ACCESS_KEY": "minio-access",
    "MINIO_SECRET_KEY": "minio-secret",
    "MINIO_BUCKET": "gtfs-ireland",
    "ARCHIVE_PREFIX": "realtime",
    "ARCHIVE_TIMEZONE": "America/Sao_Paulo",
    "ARCHIVE_INITIAL_START": "2026-09-19T00:00:00-03:00",
    "ARCHIVE_MAX_CATCHUP_HOURS": "24",
}


def set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)


def test_loads_orchestration_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    set_required_env(monkeypatch)
    monkeypatch.setenv("FAILURE_WEBHOOK_URL", "https://alerts.example.com/prefect")

    config = OrchestrationConfig.from_env()

    assert config.minio_endpoint_url == "http://minio:9000"
    assert config.minio_bucket == "gtfs-ireland"
    assert config.archive_prefix == "realtime"
    assert config.archive_timezone.key == "America/Sao_Paulo"
    assert config.archive_initial_start.isoformat() == "2026-09-19T00:00:00-03:00"
    assert config.archive_max_catchup_hours == 24
    assert config.failure_webhook_url == "https://alerts.example.com/prefect"


def test_rejects_a_naive_initial_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    set_required_env(monkeypatch)
    monkeypatch.setenv("ARCHIVE_INITIAL_START", "2026-09-19T00:00:00")

    with pytest.raises(OrchestrationConfigError, match="ARCHIVE_INITIAL_START"):
        OrchestrationConfig.from_env()


def test_rejects_an_endpoint_with_embedded_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_required_env(monkeypatch)
    monkeypatch.setenv("MINIO_ENDPOINT_URL", "http://user:password@minio:9000")

    with pytest.raises(OrchestrationConfigError, match="MINIO_ENDPOINT_URL"):
        OrchestrationConfig.from_env()


def test_rejects_non_positive_catchup_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    set_required_env(monkeypatch)
    monkeypatch.setenv("ARCHIVE_MAX_CATCHUP_HOURS", "0")

    with pytest.raises(OrchestrationConfigError, match="ARCHIVE_MAX_CATCHUP_HOURS"):
        OrchestrationConfig.from_env()
