import pytest
import sys
import os

# Add plugins to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../plugins'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../dags'))

from airflow.models import DagBag


class TestDagIntegrity:
    """Test DAG structure and integrity"""

    def setup_method(self):
        self.dagbag = DagBag(
            dag_folder='dags/',
            include_examples=False
        )

    def test_no_import_errors(self):
        """All DAGs should load without import errors"""
        assert len(self.dagbag.import_errors) == 0, \
            f"DAG import errors: {self.dagbag.import_errors}"

    def test_all_dags_present(self):
        """All expected DAGs should be present"""
        expected_dags = [
            'kafka_monitoring',
            'data_transfer',
            'spark_control',
            'quality_check',
            'archival'
        ]
        for dag_id in expected_dags:
            assert dag_id in self.dagbag.dags, \
                f"DAG '{dag_id}' not found!"

    def test_kafka_monitoring_schedule(self):
        """kafka_monitoring should run every 5 minutes"""
        dag = self.dagbag.dags['kafka_monitoring']
        assert dag.schedule_interval == '*/5 * * * *'

    def test_dag_catchup_disabled(self):
        """All DAGs should have catchup disabled"""
        for dag_id, dag in self.dagbag.dags.items():
            assert dag.catchup == False, \
                f"DAG '{dag_id}' has catchup enabled!"

    def test_dag_has_tags(self):
        """All DAGs should have tags"""
        for dag_id, dag in self.dagbag.dags.items():
            assert len(dag.tags) > 0, \
                f"DAG '{dag_id}' has no tags!"

    def test_dag_retries(self):
        """All DAGs should have retries configured"""
        for dag_id, dag in self.dagbag.dags.items():
            for task in dag.tasks:
                assert task.retries >= 1, \
                    f"Task '{task.task_id}' in DAG '{dag_id}' has no retries!"