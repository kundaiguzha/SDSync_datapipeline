from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta
import os

# Base API URL
BASE_URL = "https://commonchemistry.cas.org/api"

# Directory to store extracted JSON files
SAVE_DIR = "/opt/airflow/s3-drive/bronze_data/"

# (Optional) Ensure directory exists here or via a PythonOperator if needed:
os.makedirs(SAVE_DIR, exist_ok=True)

# Define default arguments for the DAG
default_args = {
    "owner": "Kundai",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 2,
    "retry_delay": timedelta(minutes=0)
}

# Define the DAG
dag = DAG(
    "extract_substances_dag",
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    access_control={"SparkJobRole": {"can_read", "can_edit"}}
)

# Task 1: Dummy start operator
start = EmptyOperator(
    task_id="start",
    dag=dag
)

# Task 2: Ingestion task using BashOperator
ingestion_task = BashOperator(
    task_id="run_ingestion",
    bash_command='python3 /opt/airflow/s3-drive/scripts/ingestion.py',
    dag=dag
)

# Task 3: Spark job using SparkSubmitOperator
# Update SparkSubmitOperator configuration
run_script = SparkSubmitOperator(
    task_id="run_pyspark_job",
    application="/opt/airflow/jobs/python/transform.py",  # Updated path
    conn_id="spark-conn",
    name='arrow-spark',
    application_args=['/opt/airflow/s3-drive/bronze_data/applicants.csv'],
    deploy_mode='client',
    dag=dag
)
# Set task dependencies
start >> ingestion_task >> run_script
