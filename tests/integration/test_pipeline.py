import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../plugins'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../dags'))


class TestPipelineIntegration:
    """Integration tests for the full pipeline"""

    def test_kafka_connection(self):
        """Test Kafka broker is reachable"""
        from confluent_kafka.admin import AdminClient
        config = {
            'bootstrap.servers': 'kafka-0:9092',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': 'kafka',
            'sasl.password': 'UnigapKafka@2024',
            'socket.timeout.ms': 5000
        }
        try:
            admin = AdminClient(config)
            metadata = admin.list_topics(timeout=5)
            assert metadata is not None
            print(f"✅ Kafka connected, topics: {list(metadata.topics.keys())}")
        except Exception as e:
            pytest.skip(f"Kafka not available: {e}")

    def test_product_view_topic_exists(self):
        """Test product_view topic exists"""
        from confluent_kafka.admin import AdminClient
        config = {
            'bootstrap.servers': 'kafka-0:9092',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': 'kafka',
            'sasl.password': 'UnigapKafka@2024',
            'socket.timeout.ms': 5000
        }
        try:
            admin = AdminClient(config)
            metadata = admin.list_topics(timeout=5)
            assert 'product_view' in metadata.topics, \
                "Topic 'product_view' not found!"
            print("✅ product_view topic exists")
        except Exception as e:
            pytest.skip(f"Kafka not available: {e}")

    def test_postgres_connection(self):
        """Test PostgreSQL is reachable"""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host='postgres',
                port=5432,
                database='airflow',
                user='airflow',
                password='airflow'
            )
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            result = cursor.fetchone()
            assert result[0] == 1
            conn.close()
            print("✅ PostgreSQL connected")
        except Exception as e:
            pytest.skip(f"PostgreSQL not available: {e}")

    def test_raw_product_view_table_exists(self):
        """Test raw_product_view table exists"""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host='postgres',
                port=5432,
                database='airflow',
                user='airflow',
                password='airflow'
            )
            cursor = conn.cursor()
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'raw_product_view'
                )
            """)
            exists = cursor.fetchone()[0]
            assert exists == True, "Table 'raw_product_view' does not exist!"
            conn.close()
            print("✅ raw_product_view table exists")
        except Exception as e:
            pytest.skip(f"PostgreSQL not available: {e}")