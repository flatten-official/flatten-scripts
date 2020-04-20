import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

from confirmed_cases.service import main


def enable_cloud_debugger():
    """https://cloud.google.com/debugger/docs/setup/python?hl=en_GB&_ga=2.68834001.-1991847693.1585366893"""
    try:
        import googleclouddebugger
        googleclouddebugger.enable()
    except ImportError:
        pass


enable_cloud_debugger()
GCS_BUCKET = os.environ.get('GCS_SAVE_BUCKET')
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
    schedule_interval='5 */4 * * *',
    default_args=default_args,
    catchup=True
)


run_service = PythonOperator(
    task_id='get_confirmed_cases',
    python_callable=main,
    dag=confirmed_cases_dag
)

upload_confirmed = FileToGoogleCloudStorageOperator(
    task_id='upload_confirmed',
    src=confirmed_file,
    dst=confirmed_file,
    bucket=GCS_BUCKET,
    dag=confirmed_cases_dag
)

upload_travel = FileToGoogleCloudStorageOperator(
    task_id='upload_travel',
    src=travel_file,
    dst=travel_file,
    bucket=GCS_BUCKET,
    dag=confirmed_cases_dag
)

upload_provincial = FileToGoogleCloudStorageOperator(
    task_id='upload_provincial',
    src=provincial_file,
    dst=provincial_file,
    bucket=GCS_BUCKET,
    dag=confirmed_cases_dag
)

run_service >> upload_confirmed >> upload_travel >> upload_provincial
