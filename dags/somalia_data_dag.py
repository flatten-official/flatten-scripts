import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

from somalia_data.main import main

def enable_cloud_debugger():
    """https://cloud.google.com/debugger/docs/setup/python?hl=en_GB&_ga=2.68834001.-1991847693.1585366893"""
    try:
        import googleclouddebugger
        googleclouddebugger.enable()
    except ImportError:
        pass

enable_cloud_debugger()

default_args = {
    'owner': 'Flatten.ca',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

somalia_data_dag = DAG(
    dag_id='somalia',
    start_date=datetime(2020,4,22),
    schedule_interval='5 4,16 * * *',
    default_args=default_args,
    catchup=True
)

echo = BashOperator(
    task_id='Echo',
    bash_command='echo "Getting Somalia Data"'
)

run_service = PythonOperator(
    task_id='get_somalia_data',
    python_callable=main,
    dag=somalia_data_dag
)

echo >> run_service