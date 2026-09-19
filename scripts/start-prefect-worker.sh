#!/bin/sh
set -eu

prefect work-pool create "$PREFECT_WORK_POOL" \
    --type process \
    --overwrite \
    --no-prompt

prefect deploy \
    --all \
    --prefect-file /app/prefect.yaml \
    --no-prompt

python -m orchestration.bootstrap

exec prefect worker start \
    --pool "$PREFECT_WORK_POOL" \
    --limit 1 \
    --with-healthcheck \
    --install-policy never
