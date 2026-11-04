PYTHON ?= python3
VENV := .venv
BIN := $(VENV)/bin

.PHONY: init install lint lint-fix test run build up down prefect-build prefect-up prefect-down prefect-logs

init:
	@if [ -f .env ]; then \
		echo ".env already exists; leaving it unchanged"; \
	else \
		cp .env.example .env; \
		echo "Created .env from .env.example"; \
	fi

$(BIN)/python:
	$(PYTHON) -m venv $(VENV)

install: $(BIN)/python
	$(BIN)/python -m ensurepip --upgrade
	$(BIN)/python -m pip install -e ".[dev,pipelines]"

lint:
	set -a; . ./.env; set +a; poetry run sqlfluff lint models

lint-fix:
	set -a; . ./.env; set +a; poetry run sqlfluff fix models

test:
	$(BIN)/python -m pytest -q

run:
	$(BIN)/gtfs-pipelines

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

prefect-build:
	docker compose build prefect-server

prefect-up:
	docker compose up -d prefect-server

prefect-down:
	docker compose down prefect-server

prefect-logs:
	docker compose logs -f prefect-server

network-up:
	docker network create gtfs-shared
