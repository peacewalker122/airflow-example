# SFTP to ClickHouse runbook

## Required configuration

- Airflow SSH connection ID in `DATA_PLATFORM_SFTP_CONN_ID` (default `sftp_default`).
- `DATA_PLATFORM_SFTP_REMOTE_PATH`: remote CSV path. The file must be UTF-8 or UTF-8 with BOM and include a header row.
- Airflow ClickHouse HTTP connection ID in `DATA_PLATFORM_CLICKHOUSE_CONN_ID` (default `clickhouse_default`).
- `DATA_PLATFORM_CLICKHOUSE_TABLE`: table name, optionally database-qualified. Use simple unquoted identifiers.

CSV headers must match target ClickHouse column names. The DAG inserts each row with ClickHouse `JSONEachRow`; target types and required/default columns remain ClickHouse's responsibility.

## Normal operation

DAG `ingestion.sftp_to_clickhouse` runs daily at 00:15 UTC with two retries, five minutes apart. Task `load_csv_to_clickhouse` reads the configured CSV, validates rows, and inserts them as ClickHouse `JSONEachRow`. Empty files and invalid field names fail before loading.

## Recovery

1. Inspect the failed task log and confirm the remote file and target table are available.
2. Correct the connection, file, or table issue.
3. Clear and rerun only after confirming the insert did not commit or destination deduplication is configured; otherwise reconcile possible duplicates first.

ClickHouse `INSERT` uses append semantics and has no retry deduplication. This DAG is not production-safe for retries or manual clears until the destination provides idempotent deduplication. A successful insert whose response is lost can otherwise create duplicate rows.
