import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

from confirmed_cases.service import main

GCS_BUCKET = os.environ.get('GCS_SAVE_BUCKET')
upload_location = '/home/airflow/gcs/data'
confirmed_file = 'confirmed_data_composer.json'
travel_file = 'travel_data_composer.json'
provincial_file = 'provincial_data_composer.json'

default_args = {
    'owner': 'Flatten.ca',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

confirmed_cases_dag = DAG(
    dag_id='confirmed_cases',
    start_date=datetime(2020, 4, 18),
    schedule_interval='5 4,16 * * *',
    default_args=default_args,
    catchup=True
)

echo = BashOperator(
    task_id='Echo',
    bash_command='echo "Running Confirmed Cases scripts"'
)

run_service = PythonOperator(
    task_id='get_confirmed_cases',
    python_callable=main,
    dag=confirmed_cases_dag
)

echo >> run_service