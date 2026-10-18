# Retention cleanup runbook

## Ownership and configuration

- DAG ID: `maintenance.retention_cleanup`
- Owner: `data-platform`
- Schedule: daily at 02:00 UTC

Configure `aws_default` and set `DATA_PLATFORM_BUCKET`, `DATA_PLATFORM_RETENTION_PREFIX`, and positive `DATA_PLATFORM_RETENTION_DAYS`. Use a dedicated prefix; the task deletes every object under that prefix whose S3 `LastModified` is older than the cutoff.

## Normal operation

Task `delete_expired_objects` paginates S3 listings and deletes expired keys in batches. Repeating the task is safe because deleting an already-removed key has no additional effect. Airflow retries failures twice, five minutes apart.

## Recovery

1. Confirm bucket, prefix, retention age, and AWS permissions.
2. Inspect the task log for S3 list or delete failures.
3. Retry after correcting the issue. Review the configured prefix before rerunning if it could include data outside this pipeline.
