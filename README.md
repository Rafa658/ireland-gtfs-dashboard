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

## Prefect pipelines

A separate Prefect 3 stack ships two scheduled flows that operate on the same table. DuckDB
reads Postgres directly (`postgres` extension) and writes Parquet straight to MinIO
(`httpfs` extension), so no data is staged on local disk.

| Deployment | Schedule (UTC-3) | Behavior |
| --- | --- | --- |
| `export-hourly-snapshots/hourly-parquet-export` | `5 * * * *` | Exports the whole preceding hour, every hour. A run at 05:05 exports `04:00 <= timestamp < 05:00`; a run at 19:05 exports `18:00 <= timestamp < 19:00`. |
| `enforce-retention/daily-retention` | `30 1 * * *` | Deletes rows older than `RETENTION_DAYS` whole local days. |

Exports land at:

```
s3://<MINIO_BUCKET>/<ARCHIVE_PREFIX>/ingestion_date=YYYY-MM-DD/snapshots_YYYYMMDDTHH.parquet
```

`ingestion_date` and the hour boundaries are computed in `ARCHIVE_TIMEZONE`
(`America/Sao_Paulo`, permanently UTC-3). Writes are ZSTD-compressed and deterministic: re-running
an hour overwrites the same object. An empty hour is logged and no object is written.

Retention keeps `RETENTION_DAYS` whole local days. Running on 19 Sep with `RETENTION_DAYS=3`
deletes everything before `2026-09-16T00:00:00-03:00`.

### Configuration

The stack reads `.env`. Beyond the poller's `POSTGRES_*` values it uses:

| Variable | Purpose |
| --- | --- |
| `GTFS_SHARED_NETWORK` | Existing external Docker network that hosts Postgres and MinIO. |
| `MINIO_ENDPOINT_URL` | Full URL, e.g. `http://minio:9000`. The scheme selects TLS. |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` / `MINIO_BUCKET` / `MINIO_REGION` | Object store credentials and target. |
| `ARCHIVE_PREFIX` | Key prefix under the bucket (`realtime`). |
| `ARCHIVE_TIMEZONE` | Zone used for hour windows, partition dates, and cron. |
| `RETENTION_DAYS` | Retention window in whole local days. Defaults to `3`. |
| `PREFECT_PORT` | Host port for the UI and API. Defaults to `4200`. |
| `PREFECT_PRIVATE_IP` | Address your browser uses to reach the server. |
| `PREFECT_SERVER_DATABASE_CONNECTION_URL` | `postgresql+asyncpg://...` for Prefect's own metadata database. |

Two prerequisites: the `gtfs-shared` network must already exist, and the database named in
`PREFECT_SERVER_DATABASE_CONNECTION_URL` must be created before first start (Prefect runs its own
migrations but does not create the database).

### Creating the shared network

`MINIO_ENDPOINT_URL=http://minio:9000` resolves by container name, which only works if MinIO and
the Prefect containers share a user-defined Docker network. Postgres is reached over
`host.docker.internal` instead, so it does not need to join.

```bash
docker network create gtfs-shared
docker network connect --alias minio gtfs-shared <your-minio-container>
```

The explicit `--alias minio` matters: `MINIO_ENDPOINT_URL` resolves the name `minio`, and without
the alias the container answers only to its own name.

Confirm the alias resolves and MinIO answers:

```bash
docker run --rm --network gtfs-shared curlimages/curl -sf http://minio:9000/minio/health/live
```

If MinIO is managed by its own Compose file, attach it there instead of with
`docker network connect`, so the membership survives a recreate:

```yaml
services:
  minio:
    networks:
      gtfs-shared:
        aliases: [minio]
networks:
  gtfs-shared:
    external: true
```

### Creating the Prefect metadata database

```bash
psql -h <your-postgres-host> -U postgres -c 'CREATE DATABASE prefect'
```

Point the URL's host at the same reachable address you use for `POSTGRES_HOST`. A container
cannot reach a database on another machine via `host.docker.internal`; that name resolves to the
local Docker gateway and fails with `Connect call failed ('192.168.65.254', 5432)`.

Set `PREFECT_PRIVATE_IP` to the address you browse from, such as the homelab LAN IP. It is baked
into `PREFECT_UI_API_URL`, which the **browser** calls directly, so it must resolve from your
machine and not just from inside Docker. `host.docker.internal` and `prefect-server` only resolve
between containers; using either makes the page load but show *"Unable to connect to Prefect
server"*. Verify with `curl http://$PREFECT_PRIVATE_IP:4200/api/health` from the machine you browse
from. Changing it requires `make prefect-up` again, since the value is read at container start.

The UI and API run without authentication, so keep port 4200 on your private network.

### Deploy

```bash
make prefect-build
make prefect-up
```

The UI is then served on `http://<PREFECT_PRIVATE_IP>:4200`. `make prefect-logs` tails both
services, and `make prefect-down` stops them.

`prefect-server` runs the API and UI. `prefect-flows` runs `prefect.serve`, which registers both
deployments and executes their runs in-process, so no worker or work pool is required. Restarting
either container re-registers and resumes the schedules.

### Backfilling an hour

Both flows accept an optional `run_at` parameter and derive their window from it. To re-export a
missed hour, start a custom run from the UI with `run_at` set to any instant inside the hour
*after* the one you want, matching the normal `:05` behavior.
