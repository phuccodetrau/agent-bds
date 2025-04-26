from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_job_dag',
    default_args=default_args,
    description='Schedule Spark batch processing job',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 13),
    catchup=False,
) as dag:

    run_spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/opt/airflow/jobs/batch_processing.py',
        conn_id='spark_default',
        jars='/opt/bitnami/spark/jars/gcs-connector-hadoop3-2.2.4-shaded.jar,/opt/bitnami/spark/jars/elasticsearch-spark-30_2.12-8.11.0.jar',  # Thêm JAR file
    )