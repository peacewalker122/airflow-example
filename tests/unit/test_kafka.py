import pytest

from data_platform.ingestion.kafka import parse_json_messages


def test_parse_json_messages_accepts_utf8_json_objects() -> None:
    assert parse_json_messages([b'{"id": 7}', b'{"name": "airflow"}']) == [
        {"id": 7},
        {"name": "airflow"},
    ]


@pytest.mark.parametrize("payload", [b"[]", b'"value"', b"null", b"{}"])
def test_parse_json_messages_rejects_non_row_objects(payload: bytes) -> None:
    with pytest.raises(ValueError):
        parse_json_messages([payload])


@pytest.mark.parametrize("payload", [b"not-json", b"\xff", b'{"": 1}'])
def test_parse_json_messages_rejects_invalid_payloads(payload: bytes) -> None:
    with pytest.raises((ValueError, UnicodeDecodeError)):
        parse_json_messages([payload])
