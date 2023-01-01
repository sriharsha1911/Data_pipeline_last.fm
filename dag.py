from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from extract_data import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'Dag_pull_data',
    default_args=default_args,
    description='Dag to run python script',
    schedule_interval=timedelta(days=1),
)

dag = DAG(
    'Dag_pull_data',
    default_args=default_args,
    description='run script'
)

run_etl = PythonOperator(
    task_id='pull_data',
    python_callable=main,
    dag=dag, 
)

run_etl