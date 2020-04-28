import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

from somalia_data.main import main
from somalia_data.somalia_form_data import run_form_data_scraping

from gcs.debugger import enable_cloud_debugger

enable_cloud_debugger()

default_args = {
    'owner': 'Flatten.ca',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

somalia_confirmed_data_dag = DAG(
    dag_id='somalia-confirmed',
    start_date=datetime(2020,4,22),
    schedule_interval='5 4,16 * * *',
    default_args=default_args,
    catchup=True
)

somalia_form_data_dag = DAG(
    dag_id='somalia-form',
    start_date=datetime(2020,4,25),
    schedule_interval='0 * * * *', # every hour
    default_args=default_args,
    catchup=True
)

echo_confirmed = BashOperator(
    task_id='Echo',
    bash_command='echo "Getting Somalia Confirmed Data"'
)

echo_form = BashOperator(
    task_id='Echo',
    bash_command='echo "Getting Somalia Form Data"'
)

run_confirmed_service = PythonOperator(
    task_id='get_somalia_confirmed_data',
    python_callable=main,
    dag=somalia_confirmed_data_dag
)

run_form_service = PythonOperator(
    task_id='get_somalia_form_data',
    python_callable=run_form_data_scraping,
    dag=somalia_form_data_dag
)

echo_confirmed >> run_confirmed_service
echo_form >> run_form_service
