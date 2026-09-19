PYTHON ?= python3
VENV := .venv
BIN := $(VENV)/bin

.PHONY: init install lint test run build up down prefect-build prefect-up prefect-down

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
	$(BIN)/python -m pip install -e ".[dev,orchestration]"

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
	docker compose -f compose.prefect.yaml build

prefect-up:
	docker compose -f compose.prefect.yaml up -d

prefect-down:
	docker compose -f compose.prefect.yaml down
