from airflow.models import BaseOperator, Variable
from confluent_kafka import Consumer, KafkaError
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import logging

logger = logging.getLogger(__name__)


def get_kafka_config():
    return {
        'bootstrap.servers': Variable.get('kafka_bootstrap_servers'),
        'security.protocol': Variable.get('kafka_security_protocol'),
        'sasl.mechanism':    Variable.get('kafka_sasl_mechanism'),
        'sasl.username':     Variable.get('kafka_sasl_username'),
        'sasl.password':     Variable.get('kafka_sasl_password'),
        'group.id':          'airflow-transfer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }


class KafkaToPostgresOperator(BaseOperator):
    """
    Transfers data from Kafka topic to PostgreSQL
    - Configurable batch size
    - Checkpointing via offset commits
    - Progress tracking
    - Partial failure handling
    """

    def __init__(
        self,
        topic: str,
        postgres_conn_id: str,
        target_table: str,
        batch_size: int = 100,
        timeout: int = 30,
        max_messages: int = 1000,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.postgres_conn_id = postgres_conn_id
        self.target_table = target_table
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_messages = max_messages
    def execute(self, context):
        consumer = Consumer(get_kafka_config())
        consumer.subscribe([self.topic])

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        self._ensure_table(hook)

        messages = []
        total_transferred = 0

        try:
            logger.info(f"Starting transfer from topic '{self.topic}' to '{self.target_table}'")
            
            while True:

                if total_transferred >= self.max_messages:
                    logger.info(f"Reached max_messages limit: {self.max_messages} ✅")
                    if messages:
                        self._store_batch(hook, messages)
                        total_transferred += len(messages)
                        consumer.commit()
                    break
                
                msg = consumer.poll(timeout=self.timeout)
                
                if msg is None:
                    # No more messages
                    if messages:
                        self._store_batch(hook, messages)
                        total_transferred += len(messages)
                        consumer.commit()
                        logger.info(f"Final batch stored: {len(messages)} messages")
                    break
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("Reached end of partition")
                        break
                    raise Exception(f"Kafka error: {msg.error()}")

                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    messages.append(value)
                except Exception as e:
                    logger.warning(f"Failed to parse message: {e}")
                    continue

                # Checkpoint every batch_size messages
                if len(messages) >= self.batch_size:
                    self._store_batch(hook, messages)
                    total_transferred += len(messages)
                    consumer.commit()  # checkpoint
                    logger.info(f"Batch stored: {total_transferred} messages transferred so far")
                    messages = []

        finally:
            consumer.close()

        logger.info(f"Transfer complete! Total: {total_transferred} messages")
        return total_transferred

    def _ensure_table(self, hook):
        """Create target table if not exists"""
        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {self.target_table} (
                id          SERIAL PRIMARY KEY,
                raw_data    JSONB NOT NULL,
                transferred_at TIMESTAMP DEFAULT NOW()
            );
        """)
        logger.info(f"Table '{self.target_table}' ready ✅")

    def _store_batch(self, hook, messages):
        """Store batch of messages to PostgreSQL"""
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            for msg in messages:
                cursor.execute(
                    f"INSERT INTO {self.target_table} (raw_data) VALUES (%s)",
                    (json.dumps(msg),)
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to store batch: {e}")
            raise
        finally:
            cursor.close()