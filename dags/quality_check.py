from datetime import datetime

from airflow import DAG
from operators.quality_operator import DataQualityOperator
from telegram_alert import on_failure_callback, on_success_callback


with DAG(
    dag_id='quality_check',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['quality', 'data'],
    default_args={
        'retries': 3,
        'retry_delay': 30,
        'on_failure_callback': on_failure_callback,
        'on_success_callback': on_success_callback,
    },
) as dag:

    check_fact_product_view = DataQualityOperator(
        task_id='check_fact_product_view',
        postgres_conn_id='postgres',
        table='fact_product_view',
        checks=[
            {
                'name': 'fact_table_not_empty',
                'sql': 'SELECT COUNT(*) FROM fact_product_view',
                'expected': 0,
                'operator': 'gt',
            },
            {
                'name': 'no_null_view_id',
                'sql': "SELECT COUNT(*) FROM fact_product_view WHERE view_id IS NULL AND ingested_at > NOW() - INTERVAL '2 hours'",
                'expected': 0,
                'operator': 'eq',
            },
            {
                'name': 'no_null_event_ts',
                'sql': "SELECT COUNT(*) FROM fact_product_view WHERE event_ts IS NULL AND ingested_at > NOW() - INTERVAL '2 hours'",
                'expected': 0,
                'operator': 'eq',
            },
            {
                'name': 'no_invalid_event_timestamps',
                'sql': "SELECT COUNT(*) FROM fact_product_view WHERE event_ts > NOW() + INTERVAL '1 day' OR event_ts < NOW() - INTERVAL '90 days'",
                'expected': 0,
                'operator': 'eq',
            },
            {
                'name': 'no_duplicate_view_ids',
                'sql': "SELECT COUNT(*) - COUNT(DISTINCT view_id) FROM fact_product_view WHERE ingested_at > NOW() - INTERVAL '2 hours'",
                'expected': 0,
                'operator': 'eq',
            },
            {

                'name': 'product_key_coverage_above_40pct',

                'sql': "SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE product_key IS NOT NULL) / COUNT(*), 2) FROM fact_product_view",

                'expected': 40,

                'operator': 'gte',

            },
            {
                'name': 'recent_streaming_ingest_exists',
                'sql': "SELECT COUNT(*) FROM fact_product_view WHERE ingested_at > NOW() - INTERVAL '2 hours'",
                'expected': 0,
                'operator': 'gt',
            },
            {
                'name': 'no_null_store_key',
                'sql': "SELECT COUNT(*) FROM fact_product_view WHERE store_key IS NULL AND ingested_at > NOW() - INTERVAL '2 hours'",
                'expected': 0,
                'operator': 'eq',
            },
            {
                'name': 'no_null_device_key',
                'sql': "SELECT COUNT(*) FROM fact_product_view WHERE device_key IS NULL AND ingested_at > NOW() - INTERVAL '2 hours'",
                'expected': 0,
                'operator': 'eq',
            },
            {
                'name': 'no_null_date_key',
                'sql': "SELECT COUNT(*) FROM fact_product_view WHERE date_key IS NULL AND ingested_at > NOW() - INTERVAL '2 hours'",
                'expected': 0,
                'operator': 'eq',
            },
            {
                'name': 'no_null_ip_address',
                'sql': "SELECT COUNT(*) FROM fact_product_view WHERE ip_address IS NULL AND ingested_at > NOW() - INTERVAL '2 hours'",
                'expected': 0,
                'operator': 'eq',
            },
            {
                'name': 'bad_json_rate_below_threshold',
                'sql': "SELECT COUNT(*) FROM bad_product_view_records WHERE ingested_at > NOW() - INTERVAL '2 hours'",
                'expected': 100,
                'operator': 'lt',
            },
        ],
    )