# Conventions

| Concern | Convention |
| --- | --- |
| DAG filenames | Use `snake_case.py` and name each file for its pipeline. |
| DAG IDs | Use `<domain>.<pipeline>`; ingestion pipelines use `<domain>.<source>_to_<destination>`. Keep IDs unique. |
| Task IDs | Start with an action verb and name the operation, such as `extract`, `validate`, or `load`. |
| Ownership | Set an owner/team on every DAG. Current default is `data-platform`; replace it when another team owns the pipeline. |
| Timezone | Use UTC for schedules and internal timestamps. Convert to local time only at source or presentation boundaries. |
| Scheduling | Declare an explicit schedule and timezone-aware `start_date`. Use `schedule=None` only for intentionally manual DAGs. |
| Retries | Set bounded retries and delay for transient failures. Do not retry non-idempotent side effects. |
| Idempotency | Every task with side effects must tolerate retries and manual clears without duplicate effects. Use deterministic object keys or destination-side deduplication. |
| Secrets | Store credentials in Airflow Connections or a secret manager, never source or ordinary environment files. |
| Data passing | Put large payloads in object storage and pass object keys through XCom. |
| Testing | Cover deterministic logic with unit tests, DAG IDs/owners/schedules/edges with integrity tests, and external behavior with real-service integration tests. |
| Dependencies | Pin exact resolved versions in `uv.lock`. Update it with `pyproject.toml` and review lock changes before merge. |
| Documentation | Give every production DAG a runbook in `docs/runbooks/` covering configuration, normal operation, idempotency, and recovery. |

## DAG boundaries

- Keep scheduling, retries, task definitions, and dependency edges in `dags/`.
- Put reusable ingestion, transformation, validation, storage, and loading code in `src/data_platform/`.
- Keep DAG imports independent of credentials. Resolve required settings inside task execution.
- Do not put business logic in Airflow plugins.

## Current idempotency limits

`ingestion.api_to_parquet` overwrites a deterministic object key for each logical date. S3 cleanup and retention deletes are safe to repeat. `ingestion.sftp_to_clickhouse` uses append-only ClickHouse inserts and is not retry-safe without destination deduplication. `analytics.daily_summary` depends on the idempotency of configured SQL. Do not treat either as production-ready until those destination contracts are in place.
