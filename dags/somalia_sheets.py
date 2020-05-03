from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from somalia_data.main import main
from somalia_data.somalia_form_data import run_form_data_scraping
from somalia_data.somalia_sheet import main as sheets_main

from utils.debugger import enable_cloud_debugger

enable_cloud_debugger()