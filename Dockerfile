FROM apache/airflow:3.1.3-python3.11
RUN pip install --no-cache-dir "apache-airflow==3.1.3" "duckdb==1.4.5" "pydantic>=2.12,<3" "PyYAML>=6,<7" "requests>=2.32,<3"
COPY --chown=airflow:root pyproject.toml /opt/yamlflow/pyproject.toml
COPY --chown=airflow:root yamlflow /opt/yamlflow/yamlflow
RUN pip install --no-cache-dir --no-deps /opt/yamlflow
