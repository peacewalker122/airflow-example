from datetime import UTC, datetime, timedelta

import pendulum
from airflow.sdk import dag, task

from data_platform.clients.storage import (
    delete_expired_objects as delete_expired_s3_objects,
)
from data_platform.config import positive_int_setting, setting


@dag(
    dag_id="maintenance.retention_cleanup",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["maintenance"],
)
def retention_cleanup():
    @task
    def delete_expired_objects() -> int:
        cutoff = datetime.now(UTC) - timedelta(
            days=positive_int_setting("DATA_PLATFORM_RETENTION_DAYS", 30)
        )
        return delete_expired_s3_objects(
            setting("DATA_PLATFORM_AWS_CONN_ID", "aws_default"),
            setting("DATA_PLATFORM_BUCKET"),
            setting("DATA_PLATFORM_RETENTION_PREFIX", "data-platform/"),
            cutoff,
        )

    delete_expired_objects()


retention_cleanup()
