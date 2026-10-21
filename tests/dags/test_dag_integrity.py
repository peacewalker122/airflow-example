from datetime import timedelta
from pathlib import Path

from airflow.models.dagbag import DagBag

DAG_IDS = {
    "ingestion.sftp_to_clickhouse",
    "ingestion.kafka_to_clickhouse",
    "ingestion.api_to_parquet",
    "analytics.daily_summary",
    "maintenance.retention_cleanup",
}
DAG_BAG = DagBag(
    dag_folder=str(Path(__file__).resolve().parents[2] / "dags"),
    include_examples=False,
)


def test_all_dags_import_and_have_expected_ids() -> None:
    assert not DAG_BAG.import_errors
    assert set(DAG_BAG.dag_ids) == DAG_IDS


def test_api_pipeline_orders_extract_transform_and_cleanup() -> None:
    dag = DAG_BAG.dags.get("ingestion.api_to_parquet")
    assert dag is not None
    assert dag.get_task("extract").downstream_task_ids == {"transform", "cleanup"}
    assert dag.get_task("transform").downstream_task_ids == {"cleanup"}


def test_every_dag_declares_owner_utc_schedule_and_retries() -> None:
    for dag in DAG_BAG.dags.values():
        assert dag.owner == "data-platform"
        assert dag.schedule is not None
        assert dag.start_date is not None
        assert dag.timezone.name == "UTC"
        assert dag.default_args["retries"] == 2
        assert dag.default_args["retry_delay"] == timedelta(minutes=5)

def test_task_ids_name_actions() -> None:
    expected = {
        "ingestion.sftp_to_clickhouse": {"load_csv_to_clickhouse"},
        "ingestion.kafka_to_clickhouse": {"load_kafka_to_clickhouse"},
        "ingestion.api_to_parquet": {"extract", "transform", "cleanup"},
        "analytics.daily_summary": {"execute_summary_query"},
        "maintenance.retention_cleanup": {"delete_expired_objects"},
    }
    actual = {
        dag_id: set(DAG_BAG.dags[dag_id].task_ids)
        for dag_id in expected
    }
    assert actual == expected
