# Daily summary runbook

## Ownership and configuration

- DAG ID: `analytics.daily_summary`
- Owner: `data-platform`
- Schedule: daily at 06:00 UTC

Configure the `clickhouse_default` Airflow Connection and set `DATA_PLATFORM_DAILY_SUMMARY_SQL` to the query for the deployment's actual schema and metrics. This project does not define source tables or summary semantics.

## Normal operation

Task `execute_summary_query` posts configured SQL to ClickHouse. Airflow retries failures twice, five minutes apart. The SQL must be idempotent across retries and manual clears. For writes, use a stable logical-date boundary and destination-side replacement or deduplication; a plain append-only `INSERT` can duplicate output.

## Recovery

1. Inspect the task log and ClickHouse response.
2. Confirm connection access and query validity against the deployment schema.
3. Make the query idempotent before clearing a task that may have completed its write but lost its response.
4. Retry the task after correcting the failure.
