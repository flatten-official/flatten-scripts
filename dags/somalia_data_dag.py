from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from somalia_data.main import main
from somalia_data.somalia_form_data import run_form_data_scraping
from somalia_data.somalia_sheet import upload_somalia_data_to_sheets
from utils.dags import default_args

from utils.debugger import enable_cloud_debugger

enable_cloud_debugger()


####################
# CONFIRMED SCRIPT #
####################
somalia_confirmed_data_dag = DAG(
    dag_id='somalia-confirmed',
    start_date=datetime(2020, 4, 22),
    schedule_interval='15 4,16 * * *',  # 15 minutes past hour 4 and 16.
    default_args=default_args,
    catchup=True
)

echo_confirmed = BashOperator(
    task_id='Echo',
    bash_command='echo "Getting Somalia Confirmed Data"'
)

run_confirmed_service = PythonOperator(
    task_id='get_somalia_confirmed_data',
    python_callable=main,
    dag=somalia_confirmed_data_dag
)

####################
#   FORM SCRIPT    #
####################
somalia_form_data_dag = DAG(
    dag_id='somalia-form',
    start_date=datetime(2020, 4, 25),
    schedule_interval='0 * * * *',  # every hour
    default_args=default_args,
    catchup=True
)

echo_form = BashOperator(
    task_id='Echo',
    bash_command='echo "Getting Somalia Form Data"'
)

run_form_service = PythonOperator(
    task_id='get_somalia_form_data',
    python_callable=run_form_data_scraping,
    dag=somalia_form_data_dag
)

####################
#   SHEETS SCRIPT  #
####################
somalia_sheets_upload = DAG(
    dag_id='somalia-sheets-upload',
    start_date=datetime(2020, 4, 25),
    schedule_interval='5 * * * *',  # 5 minutes past every hour
    default_args=default_args,
    catchup=True
)

echo_sheet = BashOperator(
    task_id='Echo',
    bash_command='echo "Getting Somalia Form Data"'
)

upload_sheet = PythonOperator(
    task_id='upload-to-sheets',
    python_callable=upload_somalia_data_to_sheets,
    dag=somalia_sheets_upload
)

# Three separate DAGs since none are dependant on the other
# Start times are offset to not overload the instances
echo_confirmed >> run_confirmed_service
echo_form >> run_form_service
echo_sheet >> upload_sheet
