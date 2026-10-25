"""Airflow 2.x DAG; optional: local CLI runs without Airflow."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULTS = {'owner': 'data-engineering', 'retries': 2, 'retry_delay': timedelta(minutes=5)}
with DAG('synthetic_healthcare_lakehouse', default_args=DEFAULTS,
         start_date=datetime(2026, 1, 1), schedule='@daily', catchup=False,
         tags=['portfolio', 'etl', 'quality']) as dag:
    def step(name):
        return BashOperator(task_id=name, bash_command=f'cd /opt/airflow/project && python -m src.pipeline --stage {name}')
    step('ingest') >> step('bronze') >> step('model') >> step('quality')
