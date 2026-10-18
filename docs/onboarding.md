# Onboarding

## Requirements

- Python 3.12
- `uv` 0.8 or newer
- Docker Compose for local Airflow

Install the locked environment and copy the settings template:

```sh
uv sync --locked
cp .env.example .env
```

Set `POSTGRES_PASSWORD` in `.env` to a private URL-safe value (letters, digits, `_`, or `-`); Compose uses it in Postgres bootstrap and Airflow's database URL. Do not commit `.env`. Leave non-secret pipeline settings as needed. Airflow standalone creates its local admin account on first start; SimpleAuthManager generates its password, logs it, and retains it in `airflow-logs/simple_auth_manager_passwords.json`. Airflow 3.1.0 SimpleAuthManager does not accept a configured password through its users setting, so clean resets generate a new password; retrieve it from logs or the retained password file. This is local development auth, not production credential management.


## Airflow Connections

Create these Connections in Airflow or provide them through a secret backend:

- `sftp_default`: SSH connection used by `SFTPHook`; set host, port, username, and password or private-key authentication.
- `api_default`: HTTP connection with API base URL and any required headers in connection extras.
- `clickhouse_default`: HTTP connection with ClickHouse hostname, port, and database in `schema` or `database` extra. Set `secure` to `true` in extras to use HTTPS; use HTTPS for remote hosts.
- `aws_default`: AWS connection for the S3 bucket. Prefer workload identity or a secret backend over static credentials.

Connection IDs can be changed using the matching `DATA_PLATFORM_*_CONN_ID` setting. The API connection endpoint setting is a path relative to the HTTP connection host. API responses must be a JSON array or an object whose `data` member is an array.

## Dependency updates

Change dependency declarations in `pyproject.toml`, run `uv lock --python 3.12`, review `uv.lock`, then run `uv sync --locked` and the project checks. CI rejects an out-of-date lock.

## Local Airflow lifecycle

Run from repository root, where Compose reads `.env`:

```sh
docker compose -f deploy/compose.yaml up --build
```

Compose starts Postgres and waits for its healthcheck, then starts Airflow `standalone`, which migrates the database before starting local admin auth and webserver/scheduler/API. Airflow healthcheck waits for the API. DAGs and package code are baked into the image; rebuild after source changes. `airflow-logs` persists task logs; dedicated `airflow-auth` volume retains generated local-auth password. All three named volumes survive ordinary restarts. Changing `POSTGRES_USER`, `POSTGRES_PASSWORD`, or `POSTGRES_DB` does not alter an initialized Postgres volume.

For an intentional clean reset only, stop Compose and remove its volumes:

```sh
docker compose -f deploy/compose.yaml down --volumes
```

Then run `up --build` again. This permanently deletes Postgres metadata, Airflow task logs, and generated local-auth password in `airflow-auth`; Airflow creates a new password on next initialization.

## Run and verify

```sh
uv run ruff check .
uv run pytest
bash scripts/check_dags.sh
docker compose -f deploy/compose.yaml up --build
```

Configure `DATA_PLATFORM_CLICKHOUSE_TEST_CONN_ID` only when running the integration test against a disposable ClickHouse database. That test creates and drops a random `Memory` table.
