import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess
import importlib.util
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path='/opt/airflow/.env')  # mount .env in Airflow container

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = os.getenv("KAFKA_PORT")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SCRIPTS_FOLDER = Path("/opt/airflow/ingestion/batch")
EXTRACT_LOAD_PATH = SCRIPTS_FOLDER / "extract_load.py"

def run_batch_ingestion():
    spec = importlib.util.spec_from_file_location("extract_load", EXTRACT_LOAD_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["extract_load"] = module
    spec.loader.exec_module(module)
    module.main()  # run main function

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
        'telecom_pipeline',
        default_args=default_args,
        description='Full telecom ETL pipeline',
        schedule_interval='@hourly',
        start_date=days_ago(1),
        catchup=False,
        max_active_runs=1,
) as dag:

    ingestion_task = PythonOperator(
        task_id='batch_ingestion',
        python_callable=run_batch_ingestion
    )

    dbt_run_task = PythonOperator(
        task_id='dbt_run',
        python_callable=run_dbt_models
    )

    dbt_test_task = PythonOperator(
        task_id='dbt_test',
        python_callable=run_dbt_tests
    )

    ingestion_task >> dbt_run_task >> dbt_test_task