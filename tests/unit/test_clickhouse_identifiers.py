import pytest

from data_platform.loaders.clickhouse import _validate_table_name


def test_validate_table_name_accepts_database_qualified_identifier() -> None:
    assert _validate_table_name("analytics.events") == "analytics.events"


@pytest.mark.parametrize("table", ["", ".events", "events.", "events; DROP TABLE users"])
def test_validate_table_name_rejects_unsafe_identifiers(table: str) -> None:
    with pytest.raises(ValueError):
        _validate_table_name(table)
