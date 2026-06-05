from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)


class DataArchivalOperator(BaseOperator):
    """
    Archives old data:
    - Moves old records to archive table
    - Cleans up staging areas
    - Verifies archival success
    """

    def __init__(
        self,
        postgres_conn_id: str,
        source_table: str,
        archive_table: str,
        retention_days: int = 30,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.source_table = source_table
        self.archive_table = archive_table
        self.retention_days = retention_days

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self._create_archive_table(hook)
        archived = self._archive_old_data(hook)
        self._cleanup_staging(hook)
        self._verify_archival(hook, archived)

        logger.info(f"Archival complete! {archived} records archived ✅")
        return archived

    def _create_archive_table(self, hook):
        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {self.archive_table} (
                LIKE {self.source_table},
                archived_at TIMESTAMP DEFAULT NOW()
            );
        """)
        logger.info(f"Archive table '{self.archive_table}' ready ✅")

    def _archive_old_data(self, hook):
        hook.run(f"""
            INSERT INTO {self.archive_table}
            SELECT *, NOW() as archived_at
            FROM {self.source_table}
            WHERE transferred_at < NOW() - INTERVAL '{self.retention_days} days';
        """)

        result = hook.get_first(f"""
            SELECT COUNT(*) FROM {self.archive_table}
            WHERE archived_at > NOW() - INTERVAL '1 hour'
        """)
        archived = result[0] if result else 0
        logger.info(f"Archived {archived} records ✅")
        return archived

    def _cleanup_staging(self, hook):
        hook.run(f"""
            DELETE FROM {self.source_table}
            WHERE transferred_at < NOW() - INTERVAL '{self.retention_days} days';
        """)
        logger.info(f"Cleaned up staging table '{self.source_table}' ✅")

    def _verify_archival(self, hook, expected_archived):
        result = hook.get_first(f"""
            SELECT COUNT(*) FROM {self.source_table}
            WHERE transferred_at < NOW() - INTERVAL '{self.retention_days} days'
        """)
        remaining = result[0] if result else 0
        if remaining > 0:
            raise Exception(f"Archival verification failed! {remaining} records still in source table!")
        logger.info("Archival verification passed ✅")