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

Kafka ingestion consumes one UTF-8 JSON object per message. Run it every five minutes with one active run, a positive `DATA_PLATFORM_KAFKA_MAX_MESSAGES` limit (default `1000`), batches capped at `1000`, and a five-second poll timeout. Keep `group.id` stable and `enable.auto.commit=false`; commit offsets only after ClickHouse insert succeeds. Commit failure can replay an inserted batch, so retries require destination deduplication.

`ingestion.api_to_parquet` overwrites a deterministic object key for each logical date. S3 cleanup and retention deletes are safe to repeat. SFTP ClickHouse inserts use append semantics and are not retry-safe without destination deduplication. Kafka ingestion commits offsets synchronously after successful ClickHouse batch inserts; a commit failure can replay inserted messages, so destinations must deduplicate before production retries or manual clears. `analytics.daily_summary` depends on the idempotency of configured SQL. Do not treat these pipelines as production-ready until their destination contracts are in place.
