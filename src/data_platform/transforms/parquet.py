from collections.abc import Mapping, Sequence
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq


def records_to_parquet_bytes(records: Sequence[Mapping[str, object]]) -> bytes:
    buffer = BytesIO()
    pq.write_table(pa.Table.from_pylist(records), buffer, compression="snappy")
    return buffer.getvalue()
