# Ireland GTFS Poller

Prefect flows that poll the National Transport Authority GTFS JSON endpoint every 120 seconds,
append each successful response to PostgreSQL, archive it to MinIO, enforce retention, and run
the production dbt project. All flow entrypoints live in `src/dags`; a single runner serves them
against the Prefect server.

## Behavior

- `poll-gtfs` calls the fixed endpoint
  `https://api.nationaltransport.ie/gtfsr/v2/gtfsr?format=json` with an `x-api-key` header.
- Each run stores one snapshot; the deployment schedules a run every `POLL_INTERVAL_SECONDS`.
- Requires a top-level `header` key. The `entity` key is optional.
- Stores one row per successful poll using a UTC ingestion timestamp.
- Creates the configured schema and table when missing, but never modifies existing objects.
- A timed-out or dropped request is retried once within the same run (`API_TIMEOUT_SECONDS` per
  attempt, so keep it well under the interval). Any other failure fails the Prefect run with
  no further retries; the next scheduled run tries again. A run
  still going when the next slot arrives cancels that new slot, and a slot that starts a full
  interval late is skipped (state `Skipped`) instead of ingested as a stale snapshot.
- There is no HTTP endpoint; monitor the `poll-gtfs` runs in the Prefect UI.

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

Requirements: Python 3.14, GNU Make, Poetry (for the dbt flow), and Docker with Docker Compose
for the Prefect server.

```bash
make init
```

Edit `.env` and replace all `CHANGE_ME` values. `POSTGRES_HOST` and `MINIO_ENDPOINT_URL` must be
reachable from the machine running the flows runner.

## Local development

```bash
make install
make lint
make test
make run   # starts the flows runner against PREFECT_API_URL
```

## Prefect pipelines

The Prefect 3 stack ships scheduled flows that operate on the same table. DuckDB
reads Postgres directly (`postgres` extension) and writes Parquet straight to MinIO
(`httpfs` extension), so no data is staged on local disk.

| Deployment | Schedule (UTC-3) | Behavior |
| --- | --- | --- |
| `export-hourly-snapshots/hourly-parquet-export` | `5 * * * *` | Exports the whole preceding hour, every hour. A run at 05:05 exports `04:00 <= timestamp < 05:00`; a run at 19:05 exports `18:00 <= timestamp < 19:00`. |
| `enforce-retention/daily-retention` | `30 1 * * *` | Deletes rows older than `RETENTION_DAYS` whole local days. |
| `poll-gtfs/gtfs-poll` | every `POLL_INTERVAL_SECONDS` | One snapshot per run (see Behavior). |
| `dbt-prod/dbt-prod` | `0 4 * * *` (`America/Sao_Paulo`) | `git pull`, `dbt run -t prod`, then copies `target/manifest.json` to `artifacts/prod`. |
| `dbt-ci/dbt-ci` | on demand (`ref` = commit SHA) | Runs modified models (`state:modified+`) into a throwaway `gtfs_stg.duckdb`, deferring to the prod manifest in `artifacts/prod` and attaching `gtfs.duckdb` read-only. |

`dbt-prod` and `dbt-ci` share the `gtfs-dbt` limit (1) so DuckDB files are never read and written at once. Export and retention share the `gtfs-archive` global concurrency limit (1), which the runner
creates at startup, so they never overlap. Polling and dbt are not blocked by them.

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

The UI is then served on `http://<PREFECT_PRIVATE_IP>:4200`. `make prefect-logs` tails the
server, and `make prefect-down` stops it. The container runs only the API and UI.

### Flows runner

One host process runs `gtfs-pipelines` (`src/dags/serve.py`). It registers all four deployments
and executes their runs in-process, so no worker or work pool is required. It reads `.env` from
its working directory and calls the dbt flow through Poetry, so run it from the repository root
on a machine where `poetry run dbt run -t prod` works. Restarting it re-registers the schedules.

For a checkout at `~/ireland-gtfs-dashboard` with Poetry at `~/.local/bin/poetry`:

```bash
poetry install --extras pipelines --no-root
mkdir -p ~/.config/systemd/user
cp deploy/gtfs-flows.service ~/.config/systemd/user/
sudo loginctl enable-linger "$USER"
systemctl --user daemon-reload
systemctl --user enable --now gtfs-flows.service
```

Lingering keeps the runner active after logout and starts it at boot. The service connects to
`http://127.0.0.1:4200/api`; use `journalctl --user -u gtfs-flows.service` for runner logs. Set
`POSTGRES_HOST` and `MINIO_ENDPOINT_URL` to addresses reachable from the host, since Docker-only
names such as `minio` do not resolve there. Stop any previous poller container or runner before
starting this one so two writers never poll at once.

### Backfilling

Both scheduled flows accept an optional `run_at` parameter and derive their window from it. To
re-export a single missed hour, start a custom run from the UI with `run_at` set to any instant
inside the hour *after* the one you want, matching the normal `:05` behavior.

To reprocess a whole range retroactively, run the backfill from the repository root on the runner host. It walks the
range in one-hour steps and writes each hour exactly as the scheduled export would, so it is
idempotent and safe to repeat:

```bash
poetry run gtfs-backfill --max-hours 0
```

With no arguments it covers `ARCHIVE_INITIAL_START` through the last complete hour. Hours with no
rows are skipped rather than written as empty files. It runs as a tracked flow run, so its logs
appear in the UI alongside the scheduled ones.

| Option | Meaning |
| --- | --- |
| `--start` | ISO 8601 with UTC offset. Defaults to `ARCHIVE_INITIAL_START`. Floored to the hour. |
| `--end` | ISO 8601 exclusive end. Defaults to now, so the in-progress hour is never written. |
| `--max-hours` | Hours to process in one run. Defaults to `ARCHIVE_MAX_CATCHUP_HOURS`. `0` means no cap. |

When a run is capped it stops early and reports `next_start`. Pass that value to `--start` to
continue, since re-running the same command would otherwise repeat the same hours:

```bash
poetry run gtfs-backfill --start 2026-09-20T00:00:00-03:00
```

Backfilling further back than `RETENTION_DAYS` has no effect, because the retention flow has
already deleted those rows from Postgres.
