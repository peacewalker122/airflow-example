import csv
import io

from airflow.providers.sftp.hooks.sftp import SFTPHook


def read_csv_records(connection_id: str, remote_path: str) -> list[dict[str, str]]:
    hook = SFTPHook(ssh_conn_id=connection_id)
    sftp = hook.get_conn()
    try:
        with sftp.open(remote_path, "rb") as remote_file:
            with io.TextIOWrapper(remote_file, encoding="utf-8-sig", newline="") as csv_file:
                return list(csv.DictReader(csv_file))
    finally:
        hook.close_conn()
