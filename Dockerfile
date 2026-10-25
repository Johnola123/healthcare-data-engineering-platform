FROM apache/airflow:2.10.5-python3.11
USER airflow
COPY --chown=airflow:root requirements.txt /opt/airflow/project/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/project/requirements.txt
