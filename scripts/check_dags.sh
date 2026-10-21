#!/usr/bin/env bash
set -euo pipefail

uv run python -c 'from airflow.models.dagbag import DagBag; bag = DagBag(dag_folder="dags", include_examples=False); expected = {"ingestion.sftp_to_clickhouse", "ingestion.api_to_parquet", "analytics.daily_summary", "maintenance.retention_cleanup", "ingestion.kafka_to_clickhouse"}; assert not bag.import_errors, bag.import_errors; assert set(bag.dag_ids) == expected, bag.dag_ids; print("DAG imports and IDs OK")'
