from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'retail_etl_batch',
    default_args=default_args,
    description='Batch ETL Retail dengan PySpark',
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 14),
    catchup=False,
) as dag:

    etl_task = BashOperator(
        task_id='run_pyspark_etl',
        bash_command='spark-submit --jars /opt/airflow/dags/jars/postgresql-42.7.7.jar /opt/airflow/dags/scripts/spark_retail_etl.py'
    )

    etl_task
