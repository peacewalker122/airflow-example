import json
from collections.abc import Sequence

from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook

from data_platform.loaders.clickhouse import insert_json_rows
from data_platform.quality.validation import validate_records


def parse_json_messages(messages: Sequence[bytes]) -> list[dict[str, object]]:
    records = []
    for message in messages:
        value = json.loads(message.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("Kafka message must contain a JSON object")
        records.append(value)
    validate_records(records)
    return records


def consume_to_clickhouse(
    connection_id: str,
    clickhouse_connection_id: str,
    topic: str,
    table: str,
    max_messages: int = 1000,
) -> None:
    if max_messages < 1:
        raise ValueError("max_messages must be greater than zero")

    hook = KafkaConsumerHook(topics=[topic], kafka_config_id=connection_id)
    connection = hook.get_connection(connection_id)
    config = connection.extra_dejson
    if not str(config.get("group.id", "")).strip():
        raise ValueError("Kafka Connection requires a stable group.id")
    auto_commit = config.get("enable.auto.commit")
    if not (
        auto_commit is False
        or isinstance(auto_commit, str)
        and auto_commit.lower() == "false"
    ):
        raise ValueError("Kafka Connection requires enable.auto.commit=false")

    consumer = hook.get_consumer()
    try:
        remaining = max_messages
        while remaining:
            messages = consumer.consume(
                num_messages=min(1000, remaining), timeout=5
            )
            if not messages:
                break
            payloads = [message.value() for message in messages]
            if any(payload is None for payload in payloads):
                raise ValueError("Kafka message payload cannot be None")
            records = parse_json_messages(payloads)
            insert_json_rows(clickhouse_connection_id, table, records)
            committed = consumer.commit(asynchronous=False)
            if any(partition.error is not None for partition in committed or ()):
                raise RuntimeError("Kafka offset commit failed")
            remaining -= len(records)
    finally:
        consumer.close()
