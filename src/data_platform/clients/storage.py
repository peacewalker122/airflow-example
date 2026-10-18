from datetime import datetime
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def put_object(
    connection_id: str,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
) -> None:
    S3Hook(aws_conn_id=connection_id).get_conn().put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


def get_object(connection_id: str, bucket: str, key: str) -> bytes:
    body = S3Hook(aws_conn_id=connection_id).get_conn().get_object(Bucket=bucket, Key=key)["Body"]
    try:
        return body.read()
    finally:
        body.close()


def delete_object(connection_id: str, bucket: str, key: str) -> None:
    S3Hook(aws_conn_id=connection_id).get_conn().delete_object(Bucket=bucket, Key=key)


def delete_expired_objects(
    connection_id: str,
    bucket: str,
    prefix: str,
    cutoff: datetime,
) -> int:
    client = S3Hook(aws_conn_id=connection_id).get_conn()
    paginator = client.get_paginator("list_objects_v2")
    deleted_count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        expired: list[dict[str, str]] = [
            {"Key": item["Key"]}
            for item in page.get("Contents", [])
            if item["LastModified"] < cutoff
        ]
        for offset in range(0, len(expired), 1000):
            response: dict[str, Any] = client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": expired[offset : offset + 1000], "Quiet": True},
            )
            errors = response.get("Errors", [])
            if errors:
                raise RuntimeError(f"S3 failed to delete {len(errors)} expired objects")
            deleted_count += len(expired[offset : offset + 1000])

    return deleted_count
