# Architecture

## Boundary

`dags/` owns Airflow concerns only: schedules, retries, task definitions, and dependency edges. `src/data_platform/` owns API/SFTP reads, validation, Parquet conversion, object storage, and ClickHouse requests. Reusable code lives in the uniquely named `data_platform` package, not `plugins/`.

## Pipelines

- `ingestion.sftp_to_clickhouse`: read UTF-8 CSV over SFTP, validate non-empty rows, insert as ClickHouse `JSONEachRow`.
- `ingestion.api_to_parquet`: fetch a JSON array (or `data` array), write temporary JSON to S3, convert to Parquet, write final object, then remove temporary object. S3 is the inter-task boundary; payloads do not pass through XCom.
- `analytics.daily_summary`: execute the configured `DATA_PLATFORM_DAILY_SUMMARY_SQL` against ClickHouse. The query is deployment-owned because source tables and business metrics are not specified here.
- `maintenance.retention_cleanup`: delete S3 objects older than the configured age under the configured prefix.

The API validation contract checks that records are a non-empty sequence of non-empty mappings with non-empty string field names. Add source-specific schema checks when a source schema is defined.

## Production requirements

`ingestion.api_to_parquet` overwrites its Parquet object for a logical date, so reruns do not create duplicate S3 outputs. S3 deletion tasks are safe to repeat. The SFTP ClickHouse insert appends rows and has no deduplication contract; configure destination-side deduplication before production retries or manual clears. `analytics.daily_summary` also requires idempotent SQL because Airflow retries failed tasks.


## External dependencies

Credentials belong in Airflow Connections or a secret backend. Non-secret paths, table names, endpoint paths, retention values, and SQL are environment settings. Use TLS for remote ClickHouse connections and scope retention to a dedicated bucket prefix.
