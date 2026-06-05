from datetime import datetime
from airflow import DAG
from operators.archival_operator import DataArchivalOperator
from telegram_alert import on_failure_callback, on_success_callback


with DAG(
    dag_id='archival',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['archival', 'data'],
    default_args={
        'retries': 3,
        'retry_delay': 30,
        'on_failure_callback': on_failure_callback,
        'on_success_callback': on_success_callback
    }
) as dag:

    archive_raw_data = DataArchivalOperator(
        task_id='archive_raw_data',
        postgres_conn_id='postgres',
        source_table='raw_product_view',
        archive_table='raw_product_view_archive',
        retention_days=30
    )