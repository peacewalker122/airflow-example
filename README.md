# data-platform

Airflow 3.1 project with orchestration kept in `dags/` and reusable ingestion, transform, quality, storage, and loading code in the installable `src/data_platform/` package. Airflow plugins are intentionally unused.

## Layout

- `dags/`: schedules, retries, task boundaries, and dependencies.
- `src/data_platform/`: pipeline implementations and configuration.
- `tests/dags/`: DAG import, ID, and dependency checks.
- `tests/unit/`: isolated transformation and validation checks.
- `tests/integration/`: tests requiring configured external services.
- `deploy/`: local container setup and Kubernetes Helm values.
- `docs/`: architecture, conventions, onboarding, and operational runbooks.
- `scripts/`: local developer checks.

## Local setup

Requires Python 3.12 and `uv`.

```sh
uv sync --locked
cp .env.example .env
```

Fill required pipeline settings in `.env`, create Airflow connections as described in `docs/onboarding.md`, then start local Airflow:

```sh
docker compose -f deploy/compose.yaml up --build
```

Open `http://localhost:8080`. `airflow standalone` prints initial login details in container logs.

## Checks

```sh
uv run ruff check .
uv run pytest
bash scripts/check_dags.sh
```

The ClickHouse integration test skips unless `DATA_PLATFORM_CLICKHOUSE_TEST_CONN_ID` points to a real test connection. It creates and drops a uniquely named in-memory table.
