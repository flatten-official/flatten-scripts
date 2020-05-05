from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from sanitisation.service import main
from form_data.service import main as main_form
from svg_data.main import main as main_svg
from utils.dags import default_args

from utils.debugger import enable_cloud_debugger

enable_cloud_debugger()

sanitisation_dag = DAG(
    dag_id='sanitise',
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
    dag=sanitisation_dag
)

svg = PythonOperator(
    task_id='svg_data',
    python_callable=main_svg,
    dag=sanitisation_dag
)

echo >> sanitise >> form >> svg