from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from confirmed_cases.service import main

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

confirmed_cases_dag = DAG(
    dag_id='confirmed_cases',
    start_date=datetime(2020, 4, 13),
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    catchup=True
)

clone_repo = BashOperator(
    task_id='clone_scripts_repo',
    bash_command='cd /Users/shahzad/Development && rm -rf flatten-scripts && git clone https://github.com/flatten-official/flatten-scripts.git'
)

run_service = PythonOperator(
    task_id='get_confirmed_cases_and_write_to_bucket',
    python_callable=main,
    dag=confirmed_cases_dag
)

clone_repo >> run_service





