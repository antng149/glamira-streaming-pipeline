from datetime import datetime
from airflow.models import Variable
from airflow import DAG
from operators.kafka_operator import KafkaHealthCheckOperator, KafkaDataFlowCheckOperator
from telegram_alert import on_failure_callback, on_success_callback

KAFKA_BOOTSTRAP_SERVERS = Variable.get('kafka_bootstrap_servers')
KAFKA_TOPICS = ['product_view']
CONSUMER_GROUP = 'airflow-monitor'

with DAG(
    dag_id='kafka_monitoring',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['kafka', 'monitoring'],
    default_args={
        'retries': 1,
        'retry_delay': 60,
        'on_failure_callback': on_failure_callback,
        'on_success_callback': on_success_callback
    }
) as dag:

    check_broker = KafkaHealthCheckOperator(
        task_id='check_broker',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topics=KAFKA_TOPICS,
        consumer_group=CONSUMER_GROUP,
        timeout=30,
        retries=3
    )

    check_data_flow = KafkaDataFlowCheckOperator(
        task_id='check_data_flow',
        postgres_conn_id='postgres',
        fact_table='fact_product_view',
        bad_records_table='bad_product_view_records',
        recent_window_minutes=10,
        min_recent_rows=1,
        max_ingest_lag_minutes=15,
        max_bad_records=100,
    )

    check_broker >> check_data_flow