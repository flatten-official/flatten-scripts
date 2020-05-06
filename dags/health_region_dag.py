from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from utils.dags import default_args

from utils.debugger import enable_cloud_debugger

from health_region.health_region import health_region_data

enable_cloud_debugger()

health_region_dag = DAG(
    dag_id='health_region',
    start_date=datetime(2020,5,6),
    schedule_interval='*/30 * * * *',
    default_args=default_args,
    catchup=True
)

echo = BashOperator(
    task_id='Echo',
    bash_command='echo "Running Health Region Script"'
)

get_region_data = PythonOperator(
    task_id='health_region_data',
    python_callable=health_region_data,
    dag=health_region_dag
)

echo >> get_region_data