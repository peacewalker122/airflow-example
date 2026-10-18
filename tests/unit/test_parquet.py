from io import BytesIO

import pyarrow.parquet as pq

from data_platform.transforms.parquet import records_to_parquet_bytes


def test_records_round_trip_through_parquet() -> None:
    records = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Lin"}]
    parquet_bytes = records_to_parquet_bytes(records)
    table = pq.read_table(BytesIO(parquet_bytes))
    assert table.to_pylist() == records
