from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from threading import Lock, Thread
from typing import Protocol

from fastapi import FastAPI
from fastapi.responses import JSONResponse


class Runner(Protocol):
    def run(self) -> None: ...

    def stop(self) -> None: ...


class HealthState:
    def __init__(self) -> None:
        self._lock = Lock()
        self._status = "starting"
        self._last_attempt_at: datetime | None = None
        self._last_success_at: datetime | None = None

    def mark_attempt(self, attempted_at: datetime) -> None:
        with self._lock:
            self._last_attempt_at = attempted_at

    def mark_success(self, succeeded_at: datetime) -> None:
        with self._lock:
            self._status = "healthy"
            self._last_success_at = succeeded_at

    def mark_failure(self) -> None:
        with self._lock:
            self._status = "unhealthy"

    def snapshot(self) -> dict[str, str | None]:
        with self._lock:
            return {
                "status": self._status,
                "last_attempt_at": self._serialize(self._last_attempt_at),
                "last_success_at": self._serialize(self._last_success_at),
            }

    @staticmethod
    def _serialize(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None


def create_app(
    runner: Runner,
    health_state: HealthState,
    *,
    shutdown_timeout_seconds: int = 35,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        worker_thread = Thread(target=runner.run, name="polling-worker", daemon=True)
        worker_thread.start()
        yield
        runner.stop()
        worker_thread.join(timeout=shutdown_timeout_seconds)

    app = FastAPI(
        title="Ireland GTFS Poller",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    @app.get("/health", include_in_schema=False)
    def health() -> JSONResponse:
        snapshot = health_state.snapshot()
        status_code = 503 if snapshot["status"] == "unhealthy" else 200
        return JSONResponse(
            content=snapshot,
            status_code=status_code,
            headers={
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
            },
        )

    return app
