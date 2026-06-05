import pytest
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../plugins'))


class TestDataQualityOperator:
    """Unit tests for DataQualityOperator"""

    def test_evaluate_gt(self):
        from operators.quality_operator import DataQualityOperator
        op = DataQualityOperator(
            task_id='test',
            postgres_conn_id='postgres',
            table='test_table',
            checks=[]
        )
        assert op._evaluate(5, 0, 'gt') == True
        assert op._evaluate(0, 0, 'gt') == False
        assert op._evaluate(-1, 0, 'gt') == False

    def test_evaluate_eq(self):
        from operators.quality_operator import DataQualityOperator
        op = DataQualityOperator(
            task_id='test',
            postgres_conn_id='postgres',
            table='test_table',
            checks=[]
        )
        assert op._evaluate(0, 0, 'eq') == True
        assert op._evaluate(1, 0, 'eq') == False

    def test_evaluate_no_expected(self):
        from operators.quality_operator import DataQualityOperator
        op = DataQualityOperator(
            task_id='test',
            postgres_conn_id='postgres',
            table='test_table',
            checks=[]
        )
        assert op._evaluate(100, None, 'gt') == True


class TestKafkaHealthCheckOperator:
    """Unit tests for KafkaHealthCheckOperator"""

    def test_operator_init(self):
        from operators.kafka_operator import KafkaHealthCheckOperator
        op = KafkaHealthCheckOperator(
            task_id='test',
            bootstrap_servers='kafka-0:9092',
            topics=['test_topic'],
            consumer_group='test_group',
            timeout=10,
            retries=3
        )
        assert op.bootstrap_servers == 'kafka-0:9092'
        assert op.topics == ['test_topic']
        assert op.timeout == 10
        assert op.retries == 3

    def test_topic_not_found_raises(self):
        from operators.kafka_operator import KafkaHealthCheckOperator
        op = KafkaHealthCheckOperator(
            task_id='test',
            bootstrap_servers='kafka-0:9092',
            topics=['nonexistent_topic'],
            timeout=10,
            retries=1
        )
        with patch.object(op, '_check_broker'):
            with patch.object(op, '_check_topics',
                            side_effect=Exception("Topic 'nonexistent_topic' not found!")):
                with pytest.raises(Exception, match="not found"):
                    op.execute({})


class TestDataArchivalOperator:
    """Unit tests for DataArchivalOperator"""

    def test_operator_init(self):
        from operators.archival_operator import DataArchivalOperator
        op = DataArchivalOperator(
            task_id='test',
            postgres_conn_id='postgres',
            source_table='raw_data',
            archive_table='raw_data_archive',
            retention_days=30
        )
        assert op.source_table == 'raw_data'
        assert op.archive_table == 'raw_data_archive'
        assert op.retention_days == 30