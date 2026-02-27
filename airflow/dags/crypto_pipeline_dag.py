from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from ingestion.ingest_crypto import run as run_ingestion

default_args = {
    'owner':            'kumaresh',
    'retries':          3,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='crypto_pipeline',
    description='End-to-end crypto market analytics pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    tags=['crypto', 'dbt', 'duckdb'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_crypto_data',
        python_callable=run_ingestion,
    )

    dbt_run_task = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {os.path.dirname(__file__)}/../../dbt_project && dbt run --profiles-dir .',
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {os.path.dirname(__file__)}/../../dbt_project && dbt test --profiles-dir .',
    )

    ingest_task >> dbt_run_task >> dbt_test_task
