from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from data_platform.config import setting
from data_platform.ingestion.sftp import read_csv_records
from data_platform.loaders.clickhouse import insert_json_rows
from data_platform.quality.validation import validate_records


@dag(
    dag_id="ingestion.sftp_to_clickhouse",
    schedule="15 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ingestion"],
)
def sftp_to_clickhouse():
    @task
    def load_csv_to_clickhouse() -> None:
        records = read_csv_records(
            setting("DATA_PLATFORM_SFTP_CONN_ID", "sftp_default"),
            setting("DATA_PLATFORM_SFTP_REMOTE_PATH"),
        )
        validate_records(records)
        insert_json_rows(
            setting("DATA_PLATFORM_CLICKHOUSE_CONN_ID", "clickhouse_default"),
            setting("DATA_PLATFORM_CLICKHOUSE_TABLE"),
            records,
        )

    load_csv_to_clickhouse()


sftp_to_clickhouse()
