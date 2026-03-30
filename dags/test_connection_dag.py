from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'test_connection',
        default_args=default_args,
        description='Test DAG that prints hello',
        schedule_interval=None,
        start_date=datetime(2026, 3, 30),
        catchup=False
) as dag:

    t1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello from Airflow!"'
    )