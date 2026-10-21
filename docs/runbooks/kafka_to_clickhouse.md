# Kafka to ClickHouse runbook

## Required configuration

- Kafka Airflow Connection ID in `DATA_PLATFORM_KAFKA_CONN_ID` (default `kafka_default`). Set `bootstrap.servers` and a stable `group.id` in connection extras. Set `enable.auto.commit=false` and `auto.offset.reset=earliest`; include any authentication and TLS settings required by the broker.
- `DATA_PLATFORM_KAFKA_TOPIC`: required topic name.
- `DATA_PLATFORM_KAFKA_MAX_MESSAGES`: positive per-run maximum, default `1000`.
- ClickHouse connection ID in `DATA_PLATFORM_CLICKHOUSE_CONN_ID` (default `clickhouse_default`) and `DATA_PLATFORM_CLICKHOUSE_TABLE`, shared with SFTP ingestion.

Each Kafka message must contain one UTF-8 JSON object. The consumer validates records and inserts them as ClickHouse `JSONEachRow`. The DAG runs every five minutes, with one active run, batches of at most 1000 messages, and a five-second poll timeout. It processes at most the configured message maximum per run.

## Normal operation

DAG `ingestion.kafka_to_clickhouse` consumes finite batches. It inserts each batch into ClickHouse, then synchronously commits Kafka offsets only after the insert succeeds. Delivery is at-least-once, not exactly-once. A successful insert followed by a commit failure can replay messages, so configure destination-side idempotency or deduplication before production retries and clears.

No local Kafka broker runs in Compose. Use a reachable broker and configured Airflow Connection.

## Recovery

1. Inspect the failed task log and confirm Kafka, connection settings, topic, and ClickHouse table are available.
2. Fix the broker/connection or destination issue. Do not enable auto-commit.
3. Retry or clear the task only after checking whether its ClickHouse insert succeeded. If insert succeeded but offset commit failed, replay can create duplicates; reconcile rows or rely on destination deduplication before retrying.
4. If records remain unconsumed, check the stable consumer group and committed offsets. With `auto.offset.reset=earliest`, Kafka uses earliest available offsets when the group has no committed offset; it does not rewind an existing group.
