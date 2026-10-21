from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from data_platform.config import positive_int_setting, setting
from data_platform.ingestion.kafka import consume_to_clickhouse


@dag(
    dag_id="ingestion.kafka_to_clickhouse",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ingestion"],
)
def kafka_to_clickhouse():
    @task
    def load_kafka_to_clickhouse() -> None:
        consume_to_clickhouse(
            setting("DATA_PLATFORM_KAFKA_CONN_ID", "kafka_default"),
            setting("DATA_PLATFORM_CLICKHOUSE_CONN_ID", "clickhouse_default"),
            setting("DATA_PLATFORM_KAFKA_TOPIC"),
            setting("DATA_PLATFORM_CLICKHOUSE_TABLE"),
            positive_int_setting("DATA_PLATFORM_KAFKA_MAX_MESSAGES", 1000),
        )

    load_kafka_to_clickhouse()


kafka_to_clickhouse()
