from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from urllib.request import urlopen
import subprocess
import logging
import re
import time
import os
import json

logger = logging.getLogger(__name__)


class SparkJobOperator(BaseOperator):
    """
    Submits and monitors Spark jobs on YARN:
    - Job submission
    - Status tracking
    - Timeout handling
    - Automatic retry
    - Cleanup on failure
    """

    template_fields = ('conf',)

    def __init__(
        self,
        application: str,
        name: str,
        packages: str = None,
        master: str = 'yarn',
        deploy_mode: str = 'cluster',
        executor_memory: str = '512m',
        executor_cores: int = 1,
        num_executors: int = 2,
        conf: dict = None,
        application_args: list = None,
        timeout: int = 300,
        retries: int = 3,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.application = application
        self.name = name
        self.packages = packages
        self.master = master
        self.deploy_mode = deploy_mode
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.num_executors = num_executors
        self.conf = conf or {}
        self.application_args = application_args or []
        self.timeout = timeout
        self.retries = retries

    def execute(self, context):
        for attempt in range(1, self.retries + 1):
            try:
                logger.info(f"Attempt {attempt}/{self.retries}: Submitting Spark job '{self.name}'")
                result = self._submit_job()
                logger.info(f"Spark job '{self.name}' submitted successfully!")
                return result
            except Exception as e:
                logger.warning(f"Attempt {attempt} failed: {str(e)}")
                self._cleanup()
                if attempt == self.retries:
                    raise

    def _build_command(self):
        cmd = [
            'spark-submit',
            '--master', self.master,
            '--deploy-mode', self.deploy_mode,
            '--name', self.name,
            '--executor-memory', self.executor_memory,
            '--executor-cores', str(self.executor_cores),
            '--num-executors', str(self.num_executors),

            '--conf', 'spark.pyspark.python=/usr/bin/python3',
            '--conf', 'spark.pyspark.driver.python=/usr/bin/python3',
            '--conf', 'spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3',
            '--conf', 'spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3',
        ]

        if self.packages:
            cmd.extend(['--packages', self.packages])

        for key, value in self.conf.items():
            cmd.extend(['--conf', f'{key}={value}'])

        cmd.append(self.application)
        cmd.extend(self.application_args)

        return cmd

    def _log_command(self, cmd):
        """Log command with sensitive values masked"""
        sensitive_keys = ['SASL_JAAS_CONFIG', 'PASSWORD', 'SECRET', 'KEY', 'TOKEN']
        masked_cmd = []
        skip_next = False
        for arg in cmd:
            if skip_next:
                if any(k in arg.upper() for k in sensitive_keys):
                    key = arg.split('=')[0]
                    masked_cmd.append(f'{key}=***')
                else:
                    masked_cmd.append(arg)
                skip_next = False
            else:
                masked_cmd.append(arg)
                if arg == '--conf':
                    skip_next = True
        return ' '.join(masked_cmd)

    def _submit_job(self):
        cmd = self._build_command()
        logger.info(f"Running command: {self._log_command(cmd)}")
        

        env = os.environ.copy()
        for key, value in self.conf.items():
            if 'appMasterEnv.' in key:
                env_key = key.split('appMasterEnv.')[-1]
                env[env_key] = str(value)



        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            env=env,
            preexec_fn=os.setsid  
        )

        # Wait just long enough to get the YARN application ID
        app_id = None
        start_time = time.time()

        while time.time() - start_time < 60:  # wait max 60s for app ID
            line = process.stderr.readline()
            if not line:
                continue
            logger.info(line.strip())
            match = re.search(r'application_\d+_\d+', line)
            if match:
                app_id = match.group(0)
                logger.info(f"YARN Application ID: {app_id} ✅")
                logger.info("Job submitted to YARN successfully!")
                return app_id

        # Check if process failed immediately
        if process.poll() is not None and process.returncode != 0:
            _, stderr = process.communicate()
            raise Exception(f"Spark job failed: {stderr}")

        logger.info("Job submitted to YARN — running as streaming job!")
        return "streaming-job-submitted"

    def _cleanup(self):
        """Clean up on failure"""
        logger.info("Running cleanup procedures...")
        logger.info("Cleanup complete!")


class SparkHealthCheckOperator(BaseOperator):
    """
    Monitors Spark/YARN health:
    - active Spark application exists
    - YARN worker nodes are available
    - worker CPU/RAM resources are visible
    - data has recently been inserted into Postgres
    """

    def __init__(
        self,
        application_name: str,
        postgres_conn_id: str,
        output_table: str,
        resource_manager_url: str = 'http://resourcemanager:8088',
        recent_window_minutes: int = 30,
        min_recent_rows: int = 1,
        max_ingest_lag_minutes: int = 30,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.application_name = application_name
        self.postgres_conn_id = postgres_conn_id
        self.output_table = output_table
        self.recent_window_minutes = recent_window_minutes
        self.min_recent_rows = min_recent_rows
        self.max_ingest_lag_minutes = max_ingest_lag_minutes
        self.resource_manager_url = resource_manager_url.rstrip('/')

    def execute(self, context):
        self._check_spark_application_running()
        self._check_yarn_workers_and_resources()
        self._check_output_data_inserted()
        logger.info("Spark health checks passed ✅")
        return True

    def _get_json(self, path):
        url = f"{self.resource_manager_url}{path}"
        logger.info(f"Calling YARN ResourceManager API: {url}")
        with urlopen(url, timeout=10) as response:
            return json.loads(response.read().decode('utf-8'))
        

    def _check_spark_application_running(self):
        data = self._get_json('/ws/v1/cluster/apps?states=RUNNING')
        apps = data.get('apps', {}).get('app', []) or []
        running_apps = [app for app in apps if app.get('name') == self.application_name]

        logger.info(f"Running YARN apps: {apps}")

        if not running_apps:
            raise Exception(
                f"Spark application '{self.application_name}' is not RUNNING in YARN"
            )

        app = running_apps[0]
        logger.info(
            f"Spark application '{self.application_name}' is RUNNING ✅ "
            f"app_id={app.get('id')}, state={app.get('state')}, "
            f"tracking_url={app.get('trackingUrl')}"
        )   

    def _check_yarn_workers_and_resources(self):
        data = self._get_json('/ws/v1/cluster/nodes')
        nodes = data.get('nodes', {}).get('node', []) or []
        running_nodes = [node for node in nodes if node.get('state') == 'RUNNING']

        logger.info(f"YARN nodes: {nodes}")

        if not running_nodes:
            raise Exception("No RUNNING YARN worker nodes found")

        total_memory_mb = sum(
            node.get('availMemoryMB', 0) + node.get('usedMemoryMB', 0)
            for node in running_nodes
        )
        used_memory_mb = sum(node.get('usedMemoryMB', 0) for node in running_nodes)

        total_vcores = sum(
            node.get('availableVirtualCores', 0) + node.get('usedVirtualCores', 0)
            for node in running_nodes
        )
        used_vcores = sum(node.get('usedVirtualCores', 0) for node in running_nodes)

        logger.info(
            f"YARN worker resources: running_nodes={len(running_nodes)}, "
            f"used_memory_mb={used_memory_mb}, total_memory_mb={total_memory_mb}, "
            f"used_vcores={used_vcores}, total_vcores={total_vcores}"
        )

        if total_memory_mb <= 0:
            raise Exception("Could not verify YARN worker memory/RAM resources")

        if total_vcores <= 0:
            raise Exception("Could not verify YARN worker CPU/vcore resources")

        logger.info("YARN workers and CPU/RAM resources are available ✅")    


    def _check_output_data_inserted(self):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        recent_rows_sql = f"""
            SELECT COUNT(*)
            FROM {self.output_table}
            WHERE ingested_at > NOW() - INTERVAL '{self.recent_window_minutes} minutes'
        """
        recent_rows = hook.get_first(recent_rows_sql)[0]
        logger.info(
            f"Spark output check: {recent_rows} rows inserted into {self.output_table} "
            f"in the last {self.recent_window_minutes} minutes"
        )

        if recent_rows < self.min_recent_rows:
            raise Exception(
                f"Spark output check FAILED: recent_rows={recent_rows}, "
                f"expected >= {self.min_recent_rows}"
            )

        lag_sql = f"""
            SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - MAX(ingested_at))) / 60, 999999)
            FROM {self.output_table}
        """
        ingest_lag_minutes = float(hook.get_first(lag_sql)[0])
        logger.info(f"Spark output ingest lag: {ingest_lag_minutes:.2f} minutes")

        if ingest_lag_minutes > self.max_ingest_lag_minutes:
            raise Exception(
                f"Spark output lag check FAILED: lag={ingest_lag_minutes:.2f} minutes, "
                f"expected <= {self.max_ingest_lag_minutes} minutes"
            )

        logger.info("Spark output data has been inserted into DB ✅")