from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators.spark_operator import SparkJobOperator
from telegram_alert import on_failure_callback, on_success_callback

with DAG(
    dag_id='spark_control',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'yarn'],
    default_args={
        'retries': 3,
        'retry_delay': 60,
        'on_failure_callback': on_failure_callback,
        'on_success_callback': on_success_callback
    }
) as dag:

    setup_schema = PostgresOperator(
        task_id='setup_schema',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS dim_device (
                device_key  SERIAL PRIMARY KEY,
                browser     VARCHAR(100),
                os          VARCHAR(100),
                device_type VARCHAR(50),
                UNIQUE (browser, os, device_type)
            );

            CREATE TABLE IF NOT EXISTS dim_date (
                date_key INTEGER PRIMARY KEY,
                year     INTEGER,
                month    INTEGER,
                day      INTEGER,
                hour     INTEGER
            );

            CREATE TABLE IF NOT EXISTS dim_product (
                product_key SERIAL PRIMARY KEY,
                product_id  VARCHAR(100) UNIQUE
            );

            CREATE TABLE IF NOT EXISTS dim_store (
                store_key SERIAL PRIMARY KEY,
                store_id  VARCHAR(50) UNIQUE
            );

            CREATE TABLE IF NOT EXISTS dim_referrer (
                referrer_key    SERIAL PRIMARY KEY,
                referrer_domain VARCHAR(255) UNIQUE
            );

            CREATE TABLE IF NOT EXISTS dim_location (
                location_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                ip_address   VARCHAR(50) UNIQUE,
                country_code VARCHAR(10),
                domain       VARCHAR(255)
            );

            CREATE TABLE IF NOT EXISTS fact_product_view (
                view_id      VARCHAR(100) PRIMARY KEY,
                date_key     INTEGER REFERENCES dim_date(date_key),
                product_key  INTEGER REFERENCES dim_product(product_key),
                store_key    INTEGER REFERENCES dim_store(store_key),
                location_key INTEGER REFERENCES dim_location(location_key),
                device_key   INTEGER REFERENCES dim_device(device_key),
                referrer_key INTEGER REFERENCES dim_referrer(referrer_key),
                event_ts     TIMESTAMP,
                event_hour   INTEGER,
                time_stamp   BIGINT,
                current_url  TEXT,
                referrer_url TEXT,
                ip_address   VARCHAR(50),
                collection   VARCHAR(100),
                api_version  VARCHAR(20),
                ingested_at  TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS bad_product_view_records (
                id           SERIAL PRIMARY KEY,
                batch_id     BIGINT,
                kafka_value  TEXT,
                error_reason VARCHAR(255),
                ingested_at  TIMESTAMP DEFAULT NOW()
            );
        """
    )

    submit_spark_job = SparkJobOperator(
        task_id='submit_spark_job',
        application='/opt/airflow/scripts/spark/kafka_streaming.py',
        name='glamira-streaming',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3',
        master='yarn',
        deploy_mode='cluster',
        executor_memory='512m',
        executor_cores=1,
        num_executors=2,
        conf={
            # Postgres credentials
            'spark.yarn.appMasterEnv.PG_HOST':     Variable.get('pg_host'),
            'spark.yarn.appMasterEnv.PG_DB':       Variable.get('pg_db'),
            'spark.yarn.appMasterEnv.PG_USER':     Variable.get('pg_user'),
            'spark.yarn.appMasterEnv.PG_PASSWORD': Variable.get('pg_password'),
            'spark.executorEnv.PG_HOST':           Variable.get('pg_host'),
            'spark.executorEnv.PG_DB':             Variable.get('pg_db'),
            'spark.executorEnv.PG_USER':           Variable.get('pg_user'),
            'spark.executorEnv.PG_PASSWORD':       Variable.get('pg_password'),
            # Kafka credentials
            'spark.yarn.appMasterEnv.KAFKA_BOOTSTRAP_SERVERS': Variable.get('kafka_bootstrap_servers'),
            'spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS':       Variable.get('kafka_bootstrap_servers'),
            'spark.yarn.appMasterEnv.SPARK_RUN_MODE': 'yarn',
            'spark.executorEnv.SPARK_RUN_MODE':       'yarn',
            'spark.yarn.appMasterEnv.KAFKA_SASL_USERNAME': Variable.get('kafka_username'),
            'spark.yarn.appMasterEnv.KAFKA_SASL_PASSWORD': Variable.get('kafka_password'),
            'spark.executorEnv.KAFKA_SASL_USERNAME': Variable.get('kafka_username'),
            'spark.executorEnv.KAFKA_SASL_PASSWORD': Variable.get('kafka_password'),
            
        },
        timeout=300,
        retries=3
    )

    # Schema must exist before streaming job starts
    setup_schema >> submit_spark_job