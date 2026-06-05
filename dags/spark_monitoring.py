from datetime import datetime

from airflow import DAG
from operators.spark_operator import SparkHealthCheckOperator
from telegram_alert import on_failure_callback, on_success_callback


with DAG(
    dag_id='spark_monitoring',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['spark', 'yarn', 'monitoring'],
    default_args={
        'retries': 1,
        'retry_delay': 60,
        'on_failure_callback': on_failure_callback,
        'on_success_callback': on_success_callback,
    },
) as dag:

    check_spark_health = SparkHealthCheckOperator(
        task_id='check_spark_health',
        application_name='glamira-streaming',
        postgres_conn_id='postgres',
        output_table='fact_product_view',
        resource_manager_url='http://resourcemanager:8088',
        recent_window_minutes=30,
        min_recent_rows=1,
        max_ingest_lag_minutes=30,
    )