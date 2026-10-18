.PHONY: check lint test dags up down

check: lint test dags

lint:
	uv run ruff check .

test:
	uv run pytest

dags:
	bash scripts/check_dags.sh

up:
	docker compose -f deploy/compose.yaml up --build

down:
	docker compose -f deploy/compose.yaml down
