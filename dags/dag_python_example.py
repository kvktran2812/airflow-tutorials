# import airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator


# import time and datetime
from datetime import datetime
from datetime import timedelta

# define functions
def get_names():
    return "Donald"

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='python_dag',
    schedule_interval="@daily",
    start_date=datetime(2024, 10, 8),
    default_args=default_args,
) as dag:

    task1 = PythonOperator(
        task_id='task1',
        python_callable=lambda: print('hello world, this is Donald first task!')
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_names
    )

    task2