PYTHON ?= python3
VENV := .venv
BIN := $(VENV)/bin

.PHONY: init install lint test run build up down prefect-build prefect-up prefect-down prefect-logs

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
	$(BIN)/python -m pip install -e ".[dev]"

lint:
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

test:
	$(BIN)/python -m pytest -q

run:
	$(BIN)/gtfs-poller

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

prefect-build:
	docker compose build prefect-server prefect-flows

prefect-up:
	docker compose up -d prefect-server prefect-flows

prefect-down:
	docker compose down prefect-server prefect-flows

prefect-logs:
	docker compose logs -f prefect-server prefect-flows

network-up:
	docker network create gtfs-shared
