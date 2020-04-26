import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

from sanitisation.service import main
from form_data.service import main as main_form

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
    schedule_interval='*/30 * * * *',
    default_args=default_args,
    catchup=True
)

form_data_dag = DAG(
    dag_id='form_data',
    start_date=datetime(2020,4,22),
    schedule_interval='*/30 * * * *',
    default_args=default_args,
    catchup=True
)

echo = BashOperator(
    task_id='Echo',
    bash_command='echo "Running Sanitisation Script"'
)
echo_form = BashOperator(
    task_id='Echo',
    bash_command='echo "Running Form Data Script"'
)

sanitise = PythonOperator(
    task_id='sanitisation',
    python_callable=main,
    dag=sanitisation_dag
)

form = PythonOperator(
    task_id='form_data',
    python_callable=main_form,
    dag=form_data_dag
)

echo >> sanitise
echo_form >> form