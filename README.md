# Ireland GTFS Poller

A small Python service that polls the National Transport Authority GTFS JSON endpoint every
120 seconds and appends each successful response to PostgreSQL.

## Behavior

- Calls the fixed endpoint
  `https://api.nationaltransport.ie/gtfsr/v2/gtfsr?format=json` with an `x-api-key` header.
- Polls immediately at startup, then waits the configured interval after each completed attempt.
- Requires a top-level `header` key. The `entity` key is optional.
- Stores one row per successful poll using a UTC ingestion timestamp.
- Creates the configured schema and table when missing, but never modifies existing objects.
- Logs runtime failures as structured JSON and retries on the next polling interval.
- Exposes only `GET /health`.

The auto-created table has exactly this shape:

```sql
CREATE TABLE <schema>.<table> (
    timestamp TIMESTAMPTZ NOT NULL,
    header JSONB NOT NULL,
    entity JSONB NULL
);
```

An omitted `entity` is stored as SQL `NULL`. An explicit JSON `"entity": null` is stored as
JSONB `null`.

## Setup

Requirements: Python 3.13+, GNU Make, and optionally Docker with Docker Compose.

```bash
make init
```

Edit `.env` and replace all `CHANGE_ME` values. `POSTGRES_HOST` must be reachable from wherever
the application runs. For Docker Desktop, a database on the host can commonly be reached through
`host.docker.internal`; for another container, use its shared-network service name.

## Local development

```bash
make install
make lint
make test
make run
```

## Docker

```bash
make build
make up
```

Stop the service with:

```bash
make down
```

Compose maps the configured `APP_PORT` and restarts the container unless it is explicitly stopped.

## Health contract

`GET /health` returns:

```json
{
  "status": "healthy",
  "last_attempt_at": "2026-09-19T03:39:00+00:00",
  "last_success_at": "2026-09-19T03:39:00+00:00"
}
```

- `200 starting`: no polling cycle has completed yet.
- `200 healthy`: the latest cycle completed and was persisted.
- `503 unhealthy`: the latest setup, API, validation, or insert operation failed.

The endpoint never returns upstream payloads, credentials, or exception details.
