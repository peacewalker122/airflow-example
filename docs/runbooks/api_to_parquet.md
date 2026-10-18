# API to Parquet runbook

## Ownership and configuration

- DAG ID: `ingestion.api_to_parquet`
- Owner: `data-platform`
- Schedule: daily at 00:30 UTC

Configure `api_default` as an Airflow HTTP connection, then set `DATA_PLATFORM_API_ENDPOINT`, `DATA_PLATFORM_BUCKET`, and the raw, Parquet, and retention prefixes. API response must be a JSON array or an object with a `data` array. Records must be non-empty mappings.

## Normal operation

Tasks run in order: `extract`, `transform`, `cleanup`. Extract stores response JSON under the run-specific staging prefix. Transform writes Parquet to `{DATA_PLATFORM_PARQUET_PREFIX}/{ds_nodash}.parquet`. Cleanup removes the staging object after transform succeeds. Task retries use a five-minute delay, up to two times.

Rerunning a logical date overwrites the same Parquet key. Cleanup is safe to repeat. If cleanup fails, final Parquet remains available and staging data remains until cleanup succeeds or retention removes it.

## Recovery

1. Inspect failed task logs and confirm API connection, endpoint, and S3 access.
2. For `extract` or `transform` failures, correct the source/configuration issue and retry the task.
3. For `cleanup` failures, retry cleanup; do not rerun extraction unless source data must be refreshed.
4. Check the configured S3 prefix before manual deletion or reruns.
