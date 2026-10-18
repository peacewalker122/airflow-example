from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from data_platform.config import setting
from data_platform.loaders.clickhouse import execute_clickhouse_query


@dag(
    dag_id="analytics.daily_summary",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["analytics"],
)
def daily_summary():
    @task
    def execute_summary_query() -> None:
        execute_clickhouse_query(
            setting("DATA_PLATFORM_CLICKHOUSE_CONN_ID", "clickhouse_default"),
            setting("DATA_PLATFORM_DAILY_SUMMARY_SQL"),
        )

    execute_summary_query()


daily_summary()
