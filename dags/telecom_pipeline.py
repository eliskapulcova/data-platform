import os
from pathlib import Path
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import importlib.util
import subprocess
from dotenv import load_dotenv

load_dotenv(dotenv_path='/opt/airflow/.env')  # mount .env in Airflow container

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Paths to scripts
SCRIPTS_BATCH = Path("/opt/airflow/ingestion/batch/extract_load.py")
SCRIPTS_STREAMING = Path("/opt/airflow/ingestion/streaming")
PRODUCER_PATH = SCRIPTS_STREAMING / "producer.py"
CONSUMER_PATH = SCRIPTS_STREAMING / "consumer.py"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_batch_ingestion():
    spec = importlib.util.spec_from_file_location("extract_load", SCRIPTS_BATCH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()

def produce_kafka_messages(num_messages: int = 50):
    spec = importlib.util.spec_from_file_location("producer", PRODUCER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main(num_messages=num_messages)

def consume_kafka_messages():
    spec = importlib.util.spec_from_file_location("consumer", CONSUMER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()

def run_dbt_models():
    subprocess.run([
        'dbt', 'run',
        '--project-dir', '/opt/airflow/dbt',   # <-- explicitly point to dbt project
        '--profiles-dir', '/home/airflow/.dbt' # <-- your mounted profiles.yml
    ], check=True)

def run_dbt_tests():
    subprocess.run([
        'dbt', 'test',
        '--project-dir', '/opt/airflow/dbt',
        '--profiles-dir', '/home/airflow/.dbt'
    ], check=True)

with DAG(
        "telecom_pipeline",
        default_args=default_args,
        description="Full telecom ETL pipeline",
        schedule_interval="@hourly",
        start_date=days_ago(1),
        catchup=False,
        max_active_runs=1,
) as dag:

    ingestion_task = PythonOperator(
        task_id="batch_ingestion",
        python_callable=run_batch_ingestion
    )

    kafka_producer_task = PythonOperator(
        task_id="kafka_producer",
        python_callable=produce_kafka_messages,
        op_kwargs={"num_messages": 50},
    )

    kafka_consumer_task = PythonOperator(
        task_id="kafka_consumer",
        python_callable=consume_kafka_messages
    )

    dbt_run_task = PythonOperator(
        task_id='dbt_run',
        python_callable=run_dbt_models
    )

    dbt_test_task = PythonOperator(
        task_id='dbt_test',
        python_callable=run_dbt_tests
    )

    # Define task order
    ingestion_task >> kafka_producer_task >> kafka_consumer_task >> dbt_run_task >> dbt_test_task