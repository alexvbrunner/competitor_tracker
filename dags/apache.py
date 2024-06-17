from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/Users/alexxbrunner/Desktop/scraper')  # Adjust the path to include the directory where UTILS is located
from UTILS.main import run_job 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'domain_processing',
    default_args=default_args,
    description='A simple DAG to process domain data',
    schedule_interval='0 16,18,22 * * *',  # Run at 16:00, 18:00, and 22:00 every day
)

process_domains = PythonOperator(
    task_id='process_all_domains',
    python_callable=run_job,
    dag=dag,
)

process_domains