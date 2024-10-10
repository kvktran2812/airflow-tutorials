from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task, dag


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='dag_backfill',
    default_args=default_args,
    description='DAG with backfill',
    schedule_interval='@daily',
    start_date=datetime(2024, 10, 1),
    catchup=True,
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello, World!"'
    )

    task1