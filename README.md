# Hadoop Image — Spark Job Dependencies

This folder documents the Python dependencies that the Spark streaming job
(`scripts/spark/kafka_streaming.py`) requires on the Hadoop/YARN cluster, and how
they are baked into the Hadoop image so the cluster is fully reproducible.

## Why this exists

The Spark job runs in **YARN cluster mode** — the driver and executors execute
inside the Hadoop **nodemanager** containers, not inside Airflow. Those containers
use the `unigap/hadoop:3.3.6` image, which does not ship with the Python packages
the job imports:

- `psycopg2` — writing batches to PostgreSQL via `execute_values`
- `user_agents` — parsing the `user_agent` string into `dim_device`

Originally these were installed manually with `pip install` inside the running
containers. That state did **not** survive `docker compose down` / container
recreation, causing `ModuleNotFoundError` on a fresh cluster. The fix is to declare
the dependencies in code so every build of the image includes them.

## Files

| File | Purpose |
|------|---------|
| `requirements-spark.txt` | The dependency manifest (psycopg2-binary, user-agents) |
| `Dockerfile` | Full clean build of the Hadoop image, installs deps from the manifest |
| `Dockerfile.python` | Fast extension build — layers deps on top of an already-cached base image |

> Note: `install-hadoop.sh` and `start-namenode.sh` are part of the shared K20
> course Hadoop setup and are not duplicated here. Copy these files into that setup
> folder (`hadoop/00-setup/hadoop/`) to rebuild the image with the dependencies.

## Building the image

**Fast path (base image already cached locally):**
```bash
docker build -f Dockerfile.python -t unigap/hadoop:3.3.6 .
```

**Full clean build (from scratch — requires downloading Hadoop):**
```bash
docker build -t unigap/hadoop:3.3.6 .
```

After building, recreate the containers so they pick up the new image:
```bash
docker compose up -d --force-recreate
```

## Verifying

Confirm both packages are present in a nodemanager:
```bash
docker exec -ti hadoop-nodemanager1-1 bash -c \
  "python3 -c 'import psycopg2, user_agents; print(\"both OK\")'"
```

## Adding a new dependency

Add the package name to `requirements-spark.txt`, rebuild, and recreate the
containers. No Dockerfile edit needed — both Dockerfiles read from the same manifest.
