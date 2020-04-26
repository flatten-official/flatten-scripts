import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

from graph_scripts.icu import main

GCS_BUCKET = os.environ.get('GCS_SAVE_BUCKET')
upload_location = '/home/airflow/gcs/data'
icu_file = 'icu_capacity_composer.json'

default_args = {
    'owner': 'Flatten.ca',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

graph_scripts_dag = DAG(
    dag_id='graph_scripts',
    start_date=datetime(2020, 4, 24),
    schedule_interval='5 4,16 * * *',
    default_args=default_args,
    catchup=True
)

echo = BashOperator(
    task_id='Echo',
    bash_command='echo "Running Graph scripts"'
)

run_service = PythonOperator(
    task_id='run_graph_scripts',
    python_callable=main,
    dag=graph_scripts_dag
)

echo >> run_service
