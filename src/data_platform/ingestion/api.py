from airflow.providers.http.hooks.http import HttpHook


def fetch_api_records(connection_id: str, endpoint: str) -> list[dict[str, object]]:
    response = HttpHook(method="GET", http_conn_id=connection_id).run(endpoint=endpoint)
    payload = response.json()

    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        records = payload.get("data")
    else:
        records = None

    if not isinstance(records, list):
        raise ValueError("API response must be a JSON array or an object with a data array")
    return records
