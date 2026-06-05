from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.transfer_operator import KafkaToPostgresOperator
from telegram_alert import on_failure_callback, on_success_callback

with DAG(
    dag_id='data_transfer',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['kafka', 'postgres', 'transfer'],
    default_args={
        'retries': 3,
        'retry_delay': 30,
        'on_failure_callback': on_failure_callback,
        'on_success_callback': on_success_callback
    }
) as dag:

    transfer_data = KafkaToPostgresOperator(
        task_id='transfer_kafka_to_postgres',
        topic='product_view',
        postgres_conn_id='postgres',
        target_table='raw_product_view',
        batch_size=100,
        timeout=30,
        max_messages=1000
    )