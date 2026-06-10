# Airflow Streaming Data Platform

An end-to-end streaming data pipeline that ingests real-time product view events from Kafka, processes them with Spark Structured Streaming on YARN, loads curated analytics data into a PostgreSQL star schema, and monitors everything with Airflow DAGs and Telegram alerts.

---

## Architecture

```
Remote Kafka (SASL_PLAINTEXT)
        │
        ▼
Spark Structured Streaming (YARN)
        │
        ▼
PostgreSQL Star Schema (fact_product_view + 6 dims)
        │
        ▼
Airflow DAGs (monitoring, quality, archival)
        │
        ▼
Telegram Alerts (success + failure)
```

---

## Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Apache Airflow 2.10.4 (CeleryExecutor) |
| Stream Processing | Apache Spark 3.5.1 + Structured Streaming |
| Cluster Manager | Hadoop YARN 3.3.6 |
| Message Broker | Apache Kafka (SASL_PLAINTEXT, remote) |
| Warehouse | PostgreSQL 13 |
| Containerization | Docker Compose |
| Alerting | Telegram Bot API |

---

## Project Structure

```
airflow-pipeline/
├── dags/
│   ├── kafka_monitoring.py      # Kafka health + data flow checks every 5 min
│   ├── spark_monitoring.py      # YARN + Spark health checks every 5 min
│   ├── spark_control.py         # Schema setup + Spark job submission (daily)
│   ├── quality_check.py         # 12 data quality checks (hourly)
│   ├── data_transfer.py         # Kafka → raw_product_view (hourly)
│   ├── archival.py              # Archive raw data older than 30 days (daily)
│   └── telegram_alert.py        # Shared alert callbacks
│
├── plugins/operators/
│   ├── kafka_operator.py        # KafkaHealthCheckOperator, KafkaDataFlowCheckOperator
│   ├── spark_operator.py        # SparkJobOperator, SparkHealthCheckOperator
│   ├── quality_operator.py      # DataQualityOperator
│   ├── transfer_operator.py     # KafkaToPostgresOperator
│   └── archival_operator.py     # DataArchivalOperator
│
├── scripts/spark/
│   └── kafka_streaming.py       # Spark Structured Streaming job
│
├── tests/
│   ├── unit/
│   │   ├── test_operators.py    # 6 operator unit tests
│   │   └── test_dags.py         # 6 DAG integrity tests
│   └── integration/
│       └── test_pipeline.py     # 4 integration tests
│
├── config/hadoop/               # Hadoop XML config files
├── Dockerfile                   # Custom Airflow image
├── docker-compose.yaml          # Full stack definition
└── requirements.txt             # Python dependencies
```

---

## DAGs

### `kafka_monitoring` — every 5 minutes
Monitors the Kafka pipeline health:
- **check_broker**: broker connectivity, topic availability, consumer group validation
- **check_data_flow**: throughput (rows/min), ingest lag, processing rate, bad record rate

### `spark_monitoring` — every 5 minutes
Monitors the YARN Spark application:
- Verifies `glamira-streaming` is RUNNING in YARN
- Checks nodemanager availability and CPU/RAM utilization via ResourceManager REST API
- Validates recent data insertion into `fact_product_view`

### `spark_control` — daily
- Creates full PostgreSQL star schema (`CREATE TABLE IF NOT EXISTS`)
- Submits `kafka_streaming.py` to YARN in cluster mode

### `quality_check` — hourly
Runs 12 checks on `fact_product_view`:
- Table not empty
- No null `view_id`, `event_ts`, `date_key`, `device_key`, `store_key`, `ip_address`
- No duplicate `view_id`
- No invalid timestamps (outside 90-day window)
- Product key coverage above 40%
- Recent streaming ingest exists (last 2 hours)
- Bad JSON record rate below threshold

### `data_transfer` — hourly
Transfers up to 1,000 messages per run from Kafka `product_view` topic into `raw_product_view` table (raw landing zone).

### `archival` — daily
Archives `raw_product_view` records older than 30 days into `raw_product_view_archive`, then cleans up the source table.

---

## PostgreSQL Schema

```
dim_device       (device_key, browser, os, device_type)
dim_date         (date_key, year, month, day, hour)
dim_product      (product_key, product_id)
dim_store        (store_key, store_id)
dim_referrer     (referrer_key, referrer_domain)
dim_location     (location_key, ip_address, country_code, domain)

fact_product_view (view_id PK, date_key, product_key, store_key,
                   location_key, device_key, referrer_key,
                   event_ts, event_hour, time_stamp,
                   current_url, referrer_url, ip_address,
                   collection, api_version, ingested_at)

raw_product_view         (id, raw_data JSONB, transferred_at)
raw_product_view_archive (id, raw_data JSONB, transferred_at, archived_at)
bad_product_view_records (id, batch_id, kafka_value, error_reason, ingested_at)
```

---

## Warehouse Stats (as of deployment)

| Table | Rows |
|-------|------|
| fact_product_view | 5,839,576+ |
| dim_location | 558,197 |
| dim_product | 18,194 |
| dim_referrer | 4,983 |
| dim_device | 189 |
| dim_date | 170 |
| dim_store | 84 |
| bad_product_view_records | 0 |

---

## Setup

### Prerequisites
- Docker Desktop (16GB RAM recommended, set to 10GB minimum)
- Docker Compose

### Start the stack

```bash
# Start Hadoop cluster first
cd hadoop/00-setup/hadoop
docker compose up -d

# Reconnect Hadoop to streaming-network (required after each restart)
for container in hadoop-namenode-1 hadoop-datanode1-1 hadoop-datanode2-1 \
  hadoop-resourcemanager-1 hadoop-nodemanager1-1 hadoop-nodemanager2-1; do
  docker network connect streaming-network $container
done

# Create HDFS user directories
docker exec -ti hadoop-namenode-1 bash -c "
  hdfs dfs -mkdir -p /user/default &&
  hdfs dfs -chown default:supergroup /user/default
"

# Start Airflow
cd airflow-pipeline
docker compose up -d
```

### Configure Airflow Variables

Go to **Admin → Variables** in the Airflow UI (`http://localhost:18080`) and add:

| Key | Value |
|-----|-------|
| `kafka_bootstrap_servers` | `<your_kafka_bootstrap_servers>` |
| `kafka_security_protocol` | `SASL_PLAINTEXT` |
| `kafka_sasl_mechanism` | `PLAIN` |
| `kafka_sasl_username` | `kafka` |
| `kafka_sasl_password` | `<password>` |
| `pg_host` | `postgres` |
| `pg_db` | `airflow` |
| `pg_user` | `airflow` |
| `pg_password` | `airflow` |
| `telegram_bot_token` | `<your_bot_token>` |
| `telegram_chat_id` | `<your_chat_id>` |
| `spark_run_mode` | `yarn` |

### Configure Airflow Connections

Go to **Admin → Connections** and add:

| Conn Id | Type | Host | Schema | Login | Password | Port |
|---------|------|------|--------|-------|----------|------|
| `postgres` | Postgres | `postgres` | `airflow` | `airflow` | `airflow` | `5432` |

### Deploy Spark Streaming Job

1. Trigger `spark_control` DAG manually from the Airflow UI
2. This creates all tables and submits `glamira-streaming` to YARN
3. Pause `spark_control` after first run to avoid duplicate streaming applications
4. Enable `kafka_monitoring`, `spark_monitoring`, and `quality_check` DAGs

---

## Running Tests

```bash
docker exec -ti airflow-pipeline-airflow-worker-1 bash -c \
  "cd /opt/airflow && python -m pytest tests/ -v"
```

Expected: **14 passed, 2 skipped**

---

## Kafka Streaming Job

`scripts/spark/kafka_streaming.py` implements a Spark Structured Streaming pipeline:

1. Reads from Kafka `product_view` topic (SASL_PLAINTEXT)
2. Parses JSON with the Glamira schema
3. Separates malformed records into `bad_product_view_records`
4. Writes dimension tables first (device, date, product, store, referrer, location)
5. Joins back to get foreign keys, writes `fact_product_view`
6. Checkpoints to HDFS (`hdfs://namenode/checkpoint/glamira-yarn`)
7. Triggers every 30 seconds, 10,000 messages per batch

All credentials are passed via `spark.yarn.appMasterEnv` and `spark.executorEnv` — no hardcoded secrets.

---

## Security

- All credentials stored in Airflow Variables — never in source code
- Sensitive values masked in Airflow task logs via `_log_command()`
- No secrets committed to Git

---

## Deployment Decisions

### Why Spark Runs in YARN Cluster Mode

Initially Spark was executed using client mode, where the Spark driver was tied to the Airflow worker process.

Problems encountered:
- Driver lifecycle depended on Airflow worker availability
- Worker restarts could terminate the streaming application
- Long-running Spark workloads conflicted with task execution behavior
- Harder operational isolation and monitoring

Final decision:

```python
deploy_mode='cluster'
```

Benefits:
- Spark driver runs inside YARN rather than Airflow
- Better fault isolation
- More production-like architecture
- Airflow only submits and monitors jobs
- Streaming application survives independently of task execution

---

## Design Decisions

### Why Airflow Variables and Connections

Sensitive values are stored in Airflow Variables and Connections instead of source code.

Benefits:
- Prevent secrets from entering Git history
- Easier credential rotation
- Environment portability
- Better security practices

### Why YARN REST API Instead of CLI

Monitoring uses the ResourceManager REST API rather than shelling out to YARN commands.

Benefits:
- Works cleanly inside containers
- Avoids permission issues
- Easier monitoring and debugging
- Less dependency on local binaries

### Why Quarantine Bad Records

Malformed Kafka messages are written to `bad_product_view_records` instead of being discarded.

Benefits:
- Preserves observability
- Supports root-cause analysis
- Prevents streaming failures
- Allows future reprocessing

### Why Separate Monitoring DAGs

Kafka, Spark, Data Quality, and Data Transfer workflows are separated into dedicated DAGs.

Benefits:
- Independent failure domains
- Easier maintenance
- Better operational visibility
- Simpler troubleshooting

---

## Challenges and Resolutions

### Challenge 1: Duplicate Spark Streaming Applications

**Problem**

Multiple triggers of the Spark control DAG created multiple long-running Spark applications.

**Root Cause**

Spark Structured Streaming jobs are continuous applications. Each trigger submitted a new YARN application.

**Resolution**
- Identified duplicate applications in YARN
- Killed redundant RUNNING and ACCEPTED applications
- Established operational procedure to maintain a single active streaming application

**Result**

Only one `glamira-streaming` application runs at a time.

### Challenge 2: Spark Monitoring Permission Errors

**Problem**

Monitoring failed when attempting to execute YARN commands from Airflow containers.

**Root Cause**

Containerized Airflow workers lacked direct access to required YARN binaries and permissions.

**Resolution**

Replaced shell execution with YARN ResourceManager REST API calls.

**Result**

Monitoring became more reliable and container-friendly.

### Challenge 3: Product Key Coverage

**Problem**

Many events legitimately lacked product identifiers.

**Investigation**

Traffic included homepage visits, navigation events, category browsing, and other non-product interactions.

**Resolution**

Implemented a coverage threshold check — product key coverage must remain above 40% — instead of requiring 100% population.

**Result**

Quality validation better reflected business reality.

### Challenge 4: Malformed Kafka Messages

**Problem**

Invalid JSON messages could disrupt downstream processing.

**Resolution**

Implemented a quarantine table for malformed records and continued processing valid records.

**Result**

Pipeline reliability improved while preserving visibility into data issues.

---

## Final Validation Results

### Spark Streaming
- Spark application successfully running on YARN
- Two NodeManagers available
- Resource utilization monitored through Airflow

### Data Warehouse

| Table | Rows |
|-------|------|
| `fact_product_view` | 5,839,576+ |
| `dim_location` | 558,197+ |
| `dim_product` | 18,194+ |
| `raw_product_view` | ~96,000 |

### Data Quality

All 12 quality checks passed:
- No duplicate view IDs
- No NULL critical surrogate keys
- Valid event timestamps
- Product key coverage above threshold
- Recent streaming ingestion verified
- Bad JSON rate below threshold

### Monitoring

Spark monitoring successfully validated:
- Running Spark application
- Available worker resources
- Recent warehouse ingestion
- Acceptable ingestion lag

---

## Key Lessons Learned

1. **Streaming workloads require different orchestration patterns than batch jobs.**

One of the biggest lessons from this project was that Spark Structured Streaming cannot be managed the same way as a traditional batch job. Airflow is well-suited for job submission, monitoring, and recovery workflows, but the streaming application itself should run continuously on YARN. Treating a long-running stream as a scheduled task led to duplicate Spark applications, unnecessary restarts, and checkpoint-related complications.

2. **Spark deployment mode has major operational implications.**

Running Spark in client mode tied the driver process to the Airflow worker, making the streaming application vulnerable to worker restarts and Celery process management. During development, process-detachment techniques such as os.setsid() improved stability, but moving to YARN cluster mode provided a cleaner and more production-oriented architecture by allowing the driver to run independently inside YARN.

3. **Checkpoint management becomes critical as environments evolve.**

As the project expanded from local testing to YARN deployment, sharing the same checkpoint location created offset and state management issues. Maintaining separate checkpoint paths for local and cluster execution prevented conflicts and made testing significantly safer.

4. **API-based monitoring is more reliable than shell-based monitoring in containerized environments.**

Initial monitoring approaches relied on executing YARN commands from Airflow containers, which introduced permission and dependency issues. Migrating to the YARN ResourceManager REST APIs provided a more robust solution that was easier to maintain, easier to parse programmatically, and less dependent on container configuration.

5. **Effective data quality rules must reflect actual business behavior.**

A valuable lesson was that technically correct validation rules are not always operationally useful. Product identifiers were legitimately absent for many browsing and navigation events, making a strict “no NULL product key” rule unrealistic. Replacing that check with a product coverage threshold produced a more meaningful measure of data quality while reducing false positives.

6. **Bad data should remain visible even when it is excluded from processing.**

Instead of discarding malformed Kafka messages, the pipeline stores them in a dedicated quarantine table along with the associated error information. This approach keeps the streaming application running while preserving the ability to investigate data quality issues, identify root causes, and potentially reprocess records later.

7. **Idempotent loading patterns simplify recovery and operations.**

Warehouse loading logic was designed to be safely re-runnable using ON CONFLICT DO NOTHING semantics. This reduced operational risk during testing and recovery scenarios because failed batches could be retried without introducing duplicate dimension records.

8. **Observability is a core part of a data platform, not an optional enhancement.**

The project became significantly easier to operate after monitoring, alerting, and quality validation were treated as first-class components. Separate DAGs for Kafka health, Spark health, and data quality created clear ownership boundaries and made failures easier to diagnose than if all operational checks had been embedded inside a single pipeline workflow.

---

## Acknowledgements

Built as part of the K20 Data Engineering program at Unigap.
Dataset: Glamira jewelry e-commerce product view events.
