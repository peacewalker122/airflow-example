from collections.abc import Mapping, Sequence


def validate_records(records: Sequence[Mapping[str, object]]) -> None:
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)) or not records:
        raise ValueError("records must contain at least one row")

    for index, record in enumerate(records):
        if not isinstance(record, Mapping) or not record:
            raise ValueError(f"record {index} must be a non-empty mapping")
        if any(not isinstance(key, str) or not key.strip() for key in record):
            raise ValueError(f"record {index} contains an invalid field name")
