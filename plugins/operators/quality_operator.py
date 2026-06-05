from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)


class DataQualityOperator(BaseOperator):
    """
    Validates data quality:
    - Data completeness
    - Data type validation
    - Business rules
    - Error rate monitoring
    """

    def __init__(
        self,
        postgres_conn_id: str,
        table: str,
        checks: list,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.checks = checks

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        results = []

        for check in self.checks:
            check_name = check['name']
            sql = check['sql']
            expected = check.get('expected')
            operator = check.get('operator', 'gt')  # gt, eq, lt, gte, lte

            logger.info(f"Running check: {check_name}")
            result = hook.get_first(sql)
            actual = result[0] if result else 0

            passed = self._evaluate(actual, expected, operator)

            status = '✅' if passed else '❌'
            logger.info(f"{status} {check_name}: actual={actual}, expected={expected}")

            results.append({
                'check': check_name,
                'actual': actual,
                'expected': expected,
                'passed': passed
            })

            if not passed:
                raise Exception(
                    f"Data quality check FAILED: {check_name} "
                    f"(actual={actual}, expected {operator} {expected})"
                )

        logger.info(f"All {len(results)} quality checks passed! ✅")
        return results

    def _evaluate(self, actual, expected, operator):
        if expected is None:
            return True
        ops = {
            'gt': actual > expected,
            'gte': actual >= expected,
            'lt': actual < expected,
            'lte': actual <= expected,
            'eq': actual == expected,
        }
        return ops.get(operator, False)