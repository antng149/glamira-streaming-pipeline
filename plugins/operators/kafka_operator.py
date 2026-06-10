from airflow.models import BaseOperator, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer
import logging
import time

logger = logging.getLogger(__name__)


def get_kafka_sasl_config():
    """Get Kafka SASL credentials from Airflow Variables.
    bootstrap.servers is excluded — it is passed explicitly by each operator
    via template_fields to avoid fetching it twice.
    """
    return {
        'security.protocol': Variable.get('kafka_security_protocol'),
        'sasl.mechanism':    Variable.get('kafka_sasl_mechanism'),
        'sasl.username':     Variable.get('kafka_sasl_username'),
        'sasl.password':     Variable.get('kafka_sasl_password'),
    }


class KafkaHealthCheckOperator(BaseOperator):
    template_fields = ('bootstrap_servers',)

    def __init__(
        self,
        bootstrap_servers: str,
        topics: list = None,
        consumer_group: str = None,
        timeout: int = 10,
        retries: int = 3,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics or []
        self.consumer_group = consumer_group
        self.timeout = timeout
        self.retries = retries

    def execute(self, context):
        for attempt in range(1, self.retries + 1):
            try:
                logger.info(f"Attempt {attempt}/{self.retries}: Checking Kafka health...")
                self._check_broker()
                if self.topics:
                    self._check_topics()
                if self.consumer_group:
                    self._check_consumer_group()
                logger.info("Kafka health check passed!")
                return True
            except Exception as e:
                logger.warning(f"Attempt {attempt} failed: {str(e)}")
                if attempt == self.retries:
                    raise
                sleep_seconds = min(30, attempt * 10)
                logger.info(f"Waiting {sleep_seconds}s before retrying Kafka health check...")
                time.sleep(sleep_seconds)

    def _get_admin_client(self):
        config = {
            **get_kafka_sasl_config(),
            'bootstrap.servers': self.bootstrap_servers,
            'socket.timeout.ms': self.timeout * 1000,
            'request.timeout.ms': self.timeout * 1000,
            'metadata.max.age.ms': 30000,
        }
        return AdminClient(config)

    def _check_broker(self):
        admin = self._get_admin_client()
        metadata = admin.list_topics(timeout=self.timeout)
        logger.info(f"Connected to brokers: {self.bootstrap_servers}")
        logger.info(f"Available topics: {list(metadata.topics.keys())}")

    def _check_topics(self):
        admin = self._get_admin_client()
        metadata = admin.list_topics(timeout=self.timeout)
        available_topics = list(metadata.topics.keys())
        for topic in self.topics:
            if topic not in available_topics:
                raise Exception(f"Topic '{topic}' not found!")
            logger.info(f"Topic '{topic}' is available ✅")

    def _check_consumer_group(self):
        config = {
            **get_kafka_sasl_config(),
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.consumer_group,
            'socket.timeout.ms': self.timeout * 1000,
        }
        consumer = Consumer(config)
        logger.info(f"Consumer group '{self.consumer_group}' check passed ✅")
        consumer.close()


class KafkaDataFlowCheckOperator(BaseOperator):
    def __init__(
        self,
        postgres_conn_id: str,
        fact_table: str,
        bad_records_table: str,
        recent_window_minutes: int = 10,
        min_recent_rows: int = 1,
        max_ingest_lag_minutes: int = 15,
        max_bad_records: int = 100,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.fact_table = fact_table
        self.bad_records_table = bad_records_table
        self.recent_window_minutes = recent_window_minutes
        self.min_recent_rows = min_recent_rows
        self.max_ingest_lag_minutes = max_ingest_lag_minutes
        self.max_bad_records = max_bad_records

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        recent_rows_sql = f"""
            SELECT COUNT(*)
            FROM {self.fact_table}
            WHERE ingested_at > NOW() - INTERVAL '{self.recent_window_minutes} minutes'
        """
        recent_rows = hook.get_first(recent_rows_sql)[0]
        logger.info(
            f"Kafka throughput: {recent_rows} rows inserted "
            f"in the last {self.recent_window_minutes} minutes"
        )

        if recent_rows < self.min_recent_rows:
            raise Exception(
                f"Kafka throughput check FAILED: recent_rows={recent_rows}, "
                f"expected >= {self.min_recent_rows}"
            )

        ingest_lag_sql = f"""
            SELECT COALESCE(
                EXTRACT(EPOCH FROM (NOW() - MAX(ingested_at))) / 60,
                999999
            )
            FROM {self.fact_table}
        """
        ingest_lag_minutes = float(hook.get_first(ingest_lag_sql)[0])
        logger.info(f"Kafka processing lag proxy: {ingest_lag_minutes:.2f} minutes")

        if ingest_lag_minutes > self.max_ingest_lag_minutes:
            raise Exception(
                f"Kafka processing lag check FAILED: lag={ingest_lag_minutes:.2f} minutes, "
                f"expected <= {self.max_ingest_lag_minutes} minutes"
            )

        processing_rate = recent_rows / self.recent_window_minutes
        logger.info(f"Kafka processing rate: {processing_rate:.2f} rows/minute")

        bad_records_sql = f"""
            SELECT COUNT(*)
            FROM {self.bad_records_table}
            WHERE ingested_at > NOW() - INTERVAL '{self.recent_window_minutes} minutes'
        """
        bad_records = hook.get_first(bad_records_sql)[0]
        logger.info(
            f"Kafka bad-record threshold: {bad_records} bad records "
            f"in the last {self.recent_window_minutes} minutes"
        )

        if bad_records > self.max_bad_records:
            raise Exception(
                f"Kafka bad-record threshold FAILED: bad_records={bad_records}, "
                f"expected <= {self.max_bad_records}"
            )

        logger.info("Kafka data flow checks passed ✅")
        return {
            "recent_rows": recent_rows,
            "ingest_lag_minutes": ingest_lag_minutes,
            "processing_rate_rows_per_minute": processing_rate,
            "bad_records": bad_records,
        }