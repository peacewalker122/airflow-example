import json
import os
import uuid

import pytest

from data_platform.loaders.clickhouse import execute_clickhouse_query, insert_json_rows


def test_clickhouse_json_insert_round_trip() -> None:
    connection_id = os.environ.get("DATA_PLATFORM_CLICKHOUSE_TEST_CONN_ID")
    if not connection_id:
        pytest.skip("set DATA_PLATFORM_CLICKHOUSE_TEST_CONN_ID to run ClickHouse integration test")

    table = f"data_platform_test_{uuid.uuid4().hex}"
    execute_clickhouse_query(
        connection_id,
        f"CREATE TABLE {table} (id UInt64, value String) ENGINE = Memory",
    )
    try:
        insert_json_rows(connection_id, table, [{"id": 7, "value": "airflow"}])
        result = execute_clickhouse_query(
            connection_id,
            f"SELECT id, value FROM {table} FORMAT JSONEachRow",
        )
        assert [json.loads(line) for line in result.decode().splitlines()] == [
            {"id": 7, "value": "airflow"}
        ]
    finally:
        execute_clickhouse_query(connection_id, f"DROP TABLE IF EXISTS {table}")
