import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

import graph_scripts.icu as icu_capacity
import graph_scripts.gender_breakdown as gender_breakdown
import graph_scripts.ethnicity_breakdown as ethnicity_breakdown

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

icu_service = PythonOperator(
    task_id='run_icu_script',
    python_callable=icu_capacity.main,
    dag=graph_scripts_dag
)

ethnicity_service = PythonOperator(
    task_id='run_ethnicity_script',
    python_callable=gender_breakdown.main,
    dag=graph_scripts_dag
)

sex_service = PythonOperator(
    task_id='run_sex_script',
    python_callable=ethnicity_breakdown.main,
    dag=graph_scripts_dag
)

echo >> icu_service >> ethnicity_service >> sex_service
