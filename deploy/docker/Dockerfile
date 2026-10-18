FROM docker.io/apache/airflow:3.1.0-python3.12

COPY --from=ghcr.io/astral-sh/uv:0.8.23 /uv /uvx /usr/local/bin/

USER airflow
WORKDIR /opt/airflow

COPY --chown=airflow:root pyproject.toml uv.lock README.md ./
COPY --chown=airflow:root src/ ./src/
COPY --chown=airflow:root dags/ ./dags/

ENV PYTHONPATH="/opt/airflow/src:${PYTHONPATH}"

RUN uv sync --frozen --no-dev
ENV PATH="/opt/airflow/.venv/bin:${PATH}"
