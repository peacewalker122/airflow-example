import json
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task

from data_platform.clients.storage import delete_object, get_object, put_object
from data_platform.config import setting
from data_platform.ingestion.api import fetch_api_records
from data_platform.quality.validation import validate_records
from data_platform.transforms.parquet import records_to_parquet_bytes


@dag(
    dag_id="ingestion.api_to_parquet",
    schedule="30 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ingestion"],
)
def api_to_parquet():
    @task
    def extract() -> str:
        connection_id = setting("DATA_PLATFORM_AWS_CONN_ID", "aws_default")
        bucket = setting("DATA_PLATFORM_BUCKET")
        prefix = setting("DATA_PLATFORM_RAW_PREFIX", "data-platform/staging/api").strip("/")
        records = fetch_api_records(
            setting("DATA_PLATFORM_API_CONN_ID", "api_default"),
            setting("DATA_PLATFORM_API_ENDPOINT"),
        )
        validate_records(records)
        run_id = get_current_context()["run_id"].replace("/", "_")
        key = f"{prefix}/{run_id}.json"
        put_object(
            connection_id,
            bucket,
            key,
            json.dumps(records, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            "application/json",
        )
        return key

    @task
    def transform(staging_key: str) -> str:
        connection_id = setting("DATA_PLATFORM_AWS_CONN_ID", "aws_default")
        bucket = setting("DATA_PLATFORM_BUCKET")
        records = json.loads(
            get_object(connection_id, bucket, staging_key).decode("utf-8")
        )
        validate_records(records)
        context = get_current_context()
        prefix = setting("DATA_PLATFORM_PARQUET_PREFIX", "data-platform/processed/api").strip("/")
        key = f"{prefix}/{context['ds_nodash']}.parquet"
        put_object(
            connection_id,
            bucket,
            key,
            records_to_parquet_bytes(records),
            "application/vnd.apache.parquet",
        )
        return key

    @task
    def cleanup(staging_key: str) -> None:
        delete_object(
            setting("DATA_PLATFORM_AWS_CONN_ID", "aws_default"),
            setting("DATA_PLATFORM_BUCKET"),
            staging_key,
        )

    staging_key = extract()
    parquet_key = transform(staging_key)
    cleanup_task = cleanup(staging_key)
    parquet_key >> cleanup_task


api_to_parquet()
