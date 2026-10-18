import pytest

from data_platform.quality.validation import validate_records


def test_validate_records_accepts_rows() -> None:
    validate_records([{"id": 1}, {"id": 2}])


@pytest.mark.parametrize("records", [[], [{}], [{None: "invalid"}], ["invalid"]])
def test_validate_records_rejects_invalid_rows(records: object) -> None:
    with pytest.raises(ValueError):
        validate_records(records)
