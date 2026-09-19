import logging
import sys

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from loguru import logger

from poller.api_client import ApiClient
from poller.config import Config, ConfigError
from poller.health import HealthState, create_app
from poller.runner import PollingRunner
from worker.repository import PostgresRepository


class LoguruHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        logger.opt(exception=record.exc_info).log(level, record.getMessage())


def configure_logging(level: str) -> None:
    logger.remove()
    logger.add(sys.stdout, level=level, serialize=True, backtrace=False, diagnose=False)

    handler = LoguruHandler()
    logging.basicConfig(handlers=[handler], level=level, force=True)
    for logger_name in ("uvicorn", "uvicorn.error", "fastapi"):
        standard_logger = logging.getLogger(logger_name)
        standard_logger.handlers = [handler]
        standard_logger.propagate = False


def build_app(config: Config) -> FastAPI:
    health_state = HealthState()
    repository = PostgresRepository(config)
    api_client = ApiClient(config.api_key, config.api_timeout_seconds)
    runner = PollingRunner(
        api_client,
        repository,
        health_state,
        poll_interval_seconds=config.poll_interval_seconds,
    )
    shutdown_timeout = config.api_timeout_seconds + config.postgres_connect_timeout_seconds + 5
    return create_app(
        runner,
        health_state,
        shutdown_timeout_seconds=shutdown_timeout,
    )


def run() -> None:
    load_dotenv(dotenv_path=".env")
    configure_logging("INFO")

    try:
        config = Config.from_env()
    except ConfigError as error:
        logger.bind(event="configuration_invalid", error_type=type(error).__name__).error(
            str(error)
        )
        raise SystemExit(2) from error

    configure_logging(config.log_level)
    logger.bind(event="application_starting", port=config.app_port).info("Starting GTFS poller")
    uvicorn.run(
        build_app(config),
        host="0.0.0.0",
        port=config.app_port,
        access_log=False,
        log_config=None,
    )


if __name__ == "__main__":
    run()
