import base64
import json
import re
from collections.abc import Mapping, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from airflow.sdk.bases.hook import BaseHook

from data_platform.quality.validation import validate_records

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _validate_table_name(table: str) -> str:
    identifiers = table.split(".")
    if not all(_IDENTIFIER.fullmatch(identifier) for identifier in identifiers):
        raise ValueError(
            "table must be an unquoted ClickHouse identifier, "
            "optionally database-qualified"
        )
    return ".".join(identifiers)

def execute_clickhouse_query(connection_id: str, query: str) -> bytes:
    if not query.strip():
        raise ValueError("ClickHouse query must not be empty")

    connection = BaseHook.get_connection(connection_id)
    extras = connection.extra_dejson
    secure = str(extras.get("secure", "false")).lower() in {"1", "true", "yes"}
    scheme = "https" if secure else "http"
    host = connection.host
    if not host or "://" in host or "/" in host:
        raise ValueError("ClickHouse connection host must be a hostname")
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = connection.port or (8443 if secure else 8123)
    database = extras.get("database") or connection.schema
    query_string = urlencode({"database": database}) if database else ""
    url = f"{scheme}://{host}:{port}/"
    if query_string:
        url = f"{url}?{query_string}"

    headers = {"Content-Type": "text/plain; charset=utf-8"}
    if connection.login is not None:
        credentials = f"{connection.login}:{connection.password or ''}".encode()
        token = base64.b64encode(credentials).decode("ascii")
        headers["Authorization"] = f"Basic {token}"

    timeout = int(extras.get("timeout", 30))
    if timeout < 1:
        raise ValueError("ClickHouse connection timeout must be greater than zero")
    request = Request(url, data=query.encode("utf-8"), headers=headers, method="POST")
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def insert_json_rows(
    connection_id: str,
    table: str,
    records: Sequence[Mapping[str, object]],
) -> None:
    validate_records(records)
    table_name = _validate_table_name(table)

    rows = "\n".join(
        json.dumps(dict(record), ensure_ascii=False, separators=(",", ":"))
        for record in records
    )
    execute_clickhouse_query(
        connection_id,
        f"INSERT INTO {table_name} FORMAT JSONEachRow\n{rows}\n",
    )
