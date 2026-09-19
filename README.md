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

## Prefect archive and retention jobs

The repository also contains a separate, lightweight Prefect Compose stack:

- `gtfs-hourly-export/hourly-export` runs at minute 5 of every hour in
  `America/Sao_Paulo`.
- `gtfs-daily-retention/daily-retention` runs daily at midnight in
  `America/Sao_Paulo`.

The Prefect stack uses two containers with 1 GB memory limits:

- `prefect-server` provides the API, UI, scheduler, and background services.
- `prefect-worker` registers both deployments and executes one flow run at a time.

It uses a separate Prefect database on the existing PostgreSQL server and connects to the
existing PostgreSQL and MinIO containers through a shared external Docker network. Redis is not
required for this single-server deployment.

### Prerequisites

Create a dedicated Prefect database and user on PostgreSQL. Use a strong password and URL-encode
special characters when placing it in `PREFECT_SERVER_DATABASE_CONNECTION_URL`.

```sql
CREATE USER prefect WITH PASSWORD 'replace-with-a-strong-password';
CREATE DATABASE prefect OWNER prefect;
```

Create a private Docker network once, then attach the existing PostgreSQL and MinIO containers to
it. Replace the container names if they differ on the VPS.

```bash
docker network create gtfs-shared
docker network connect gtfs-shared postgres
docker network connect gtfs-shared minio
```

The MinIO bucket `gtfs-ireland` must already exist. The configured MinIO account needs object
read/write access under the `realtime/` prefix. It does not need bucket-administration permission.

Add the Prefect, MinIO, and archive values from `.env.example` to the existing `.env`. In
particular:

- `PREFECT_PRIVATE_IP` must be the VPS address reachable only from the trusted private network.
- `PREFECT_API_AUTH_STRING` must be a strong `username:password` value.
- `PREFECT_SERVER_DATABASE_CONNECTION_URL` must target the dedicated Prefect database.
- `POSTGRES_HOST` and `MINIO_ENDPOINT_URL` should use container DNS names on `gtfs-shared`.

Do not expose port 4200 to the public internet. The UI uses HTTP Basic authentication and should be
protected by the VPS firewall or a private VPN.

### Deploy

```bash
make prefect-build
make prefect-up
```

Open `http://<PREFECT_PRIVATE_IP>:4200` and enter the complete
`PREFECT_API_AUTH_STRING` value when prompted.

The worker startup is idempotent: it creates or updates the `gtfs-local` process work pool,
registers both deployments, initializes the export cursor, verifies that no source row predates
`ARCHIVE_INITIAL_START`, and enables retention only when that check succeeds.

Stop the Prefect stack independently of the poller:

```bash
make prefect-down
```

### Archive contract

Each scheduled export processes up to 24 missing complete hours in order, beginning at
`ARCHIVE_INITIAL_START`. Progress is stored in the Prefect Variable
`gtfs_export_next_hour`, so a later run resumes after downtime.

Each non-empty hour overwrites one deterministic object:

```text
s3://gtfs-ireland/realtime/ingestion_date=YYYY-MM-DD/hour_start=YYYY-MM-DDTHH-MM-SSZ.parquet
```

`ingestion_date` uses `America/Sao_Paulo`. The Parquet columns are:

| Column | Type | Notes |
|---|---|---|
| `timestamp` | timezone-aware timestamp | Normalized to UTC |
| `header` | string | PostgreSQL JSONB text |
| `entity` | nullable string | Preserves SQL `NULL` versus JSON `null` |

Files use Zstandard compression. Empty hours create no object but still advance the cursor.

To rerun or backfill a range, run the hourly deployment from the Prefect UI with timezone-aware
`start` and exclusive `end` parameters aligned to hour boundaries. Manual ranges overwrite the
same deterministic keys and do not change the automatic cursor.

### Retention safety

Retention deletes rows older than a rolling 72 hours, but never deletes at or after the export
cursor. The Prefect Variable `gtfs_retention_enabled` is initialized to `false` and is enabled only
after startup verifies that no source row predates `ARCHIVE_INITIAL_START`. Set it back to `false`
to pause deletion without stopping exports.

If `FAILURE_WEBHOOK_URL` is configured with an HTTPS URL, a final failed flow run sends a small
notification without database errors, payloads, or credentials.

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
