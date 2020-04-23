import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

from sanitisation.service import main

from gcs.debugger import enable_cloud_debugger

enable_cloud_debugger()

default_args = {
    'owner': 'Flatten.ca',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

sanitisation_dag = DAG(
    dag_id='sanitise',
    start_date=datetime(2020,4,22),
    schedule_interval='*/10 * * * *',
    default_args=default_args,
    catchup=True
)

echo = BashOperator(
    task_id='Echo',
    bash_command='echo "Running Sanitisation Script"'
)

run_service = PythonOperator(
    task_id='sanitisation',
    python_callable=main,
    dag=sanitisation_dag
)

echo >> run_service