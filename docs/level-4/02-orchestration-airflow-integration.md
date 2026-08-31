# 02 · Orchestration Airflow Integration

!!! note "Not executed against a live cluster/scheduler in this environment"
    Code below is hand-traced against documented Airflow/PySpark behavior,
    not run against a live Airflow instance here.

A PySpark script that runs correctly once you launch it manually still
needs something to launch it on a schedule, retry it on failure, wait for
its upstream dependencies, and alert someone when it doesn't work. Apache
Airflow is the most common orchestrator for that job. This module covers
the integration points you need: submitting a Spark job from a DAG,
passing parameters, and handling failure/retry semantics correctly.

## The `SparkSubmitOperator`

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="daily_click_summary",
    default_args=default_args,
    schedule_interval="0 3 * * *",       # 3 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    run_summary_job = SparkSubmitOperator(
        task_id="run_click_summary",
        application="/opt/spark-jobs/click_summary.py",
        conn_id="spark_default",
        conf={
            "spark.executor.memory": "8g",
            "spark.sql.shuffle.partitions": "64",
        },
        application_args=["--run-date", "{{ ds }}"],
        verbose=True,
    )
```

`{{ ds }}` is an Airflow Jinja template rendering the DAG run's logical
date as `YYYY-MM-DD` — this is how you pass the correct partition/date
into a batch job without hardcoding "today," since Airflow backfills and
manual re-runs need the *logical* date, not wall-clock time when the task
happens to execute.

## Reading the date parameter inside the PySpark script

```python
# click_summary.py
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

parser = argparse.ArgumentParser()
parser.add_argument("--run-date", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName(f"click-summary-{args.run_date}").getOrCreate()

clicks = spark.read.parquet(f"/data/raw/clickstream/date={args.run_date}")
summary = clicks.groupBy("segment").count()
summary.write.mode("overwrite").parquet(f"/data/warehouse/click_summary/date={args.run_date}")

spark.stop()
```

Reading a specific `date=` partition directly (rather than reading the
whole table and filtering) means the job only scans exactly the data it
needs — the orchestration parameter and Spark's partition pruning work
together here.

## Sensors: waiting on upstream data

```python
from airflow.sensors.filesystem import FileSensor

wait_for_upstream = FileSensor(
    task_id="wait_for_clickstream_partition",
    filepath="/data/raw/clickstream/date={{ ds }}/_SUCCESS",
    poke_interval=300,     # check every 5 minutes
    timeout=60 * 60 * 4,   # give up after 4 hours
    mode="reschedule",      # free the worker slot between pokes instead of blocking it
)

wait_for_upstream >> run_summary_job
```

Checking for a `_SUCCESS` marker file (which Spark writes automatically
on a successful `.write()`) rather than just the partition directory's
existence avoids racing against an upstream job that's still mid-write —
the directory can exist with partial files well before `_SUCCESS` lands.

## Task dependencies and a realistic multi-stage DAG

```python
from airflow.operators.python import PythonOperator

def validate_output(**context):
    run_date = context["ds"]
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("validate").getOrCreate()
    df = spark.read.parquet(f"/data/warehouse/click_summary/date={run_date}")
    row_count = df.count()
    if row_count == 0:
        raise ValueError(f"click_summary for {run_date} is empty — failing DAG")
    spark.stop()

validate = PythonOperator(task_id="validate_output", python_callable=validate_output)

notify = SparkSubmitOperator(
    task_id="publish_downstream",
    application="/opt/spark-jobs/publish_to_bi.py",
    application_args=["--run-date", "{{ ds }}"],
)

wait_for_upstream >> run_summary_job >> validate >> notify
```

A `PythonOperator` doing a lightweight row-count sanity check between the
main compute job and the downstream publish step is a cheap, effective
data-quality gate (module 7 covers this pattern more formally) — cheaper
than discovering a silently-empty table three steps further downstream.

## Retry semantics and idempotency

```python
default_args_idempotent_note = {
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}
```

Airflow retrying a failed `SparkSubmitOperator` task means the **entire**
Spark job re-runs from scratch — there's no partial resume at the
Airflow level. This makes idempotency a hard requirement for every task
in the DAG: `click_summary.py`'s `.write().mode("overwrite")` on a
specific `date=` partition is safe to retry any number of times because
re-running it just overwrites the same output again with the same
result. A job that instead used `.mode("append")` without a
deduplication step would produce duplicate rows on every retry — always
prefer `overwrite` on a well-defined partition, or a Delta `MERGE`
(module 1), for anything Airflow might retry.

## Passing Spark configuration from Airflow connections

```python
# Airflow connection "spark_default" holds the master URL / cluster endpoint;
# job-specific tuning still goes through the `conf` dict on the operator,
# keeping cluster addressing separate from per-job performance tuning:
tuned_job = SparkSubmitOperator(
    task_id="tuned_job",
    application="/opt/spark-jobs/heavy_aggregation.py",
    conn_id="spark_default",
    conf={
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.shuffle.partitions": "128",
        "spark.executor.memory": "16g",
        "spark.executor.memoryOverhead": "4g",
    },
    execution_timeout=timedelta(hours=2),   # Airflow-side timeout, separate from Spark's own
)
```

## Worked example: a full daily pipeline DAG

```python
with DAG(
    dag_id="daily_lakehouse_pipeline",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as pipeline_dag:

    wait = FileSensor(
        task_id="wait_for_raw_data",
        filepath="/data/raw/clickstream/date={{ ds }}/_SUCCESS",
        poke_interval=300,
        timeout=14400,
        mode="reschedule",
    )
    ingest = SparkSubmitOperator(
        task_id="ingest_and_merge",
        application="/opt/spark-jobs/delta_merge.py",
        application_args=["--run-date", "{{ ds }}"],
    )
    validate_task = PythonOperator(task_id="validate", python_callable=validate_output)
    compact = SparkSubmitOperator(
        task_id="optimize_table",
        application="/opt/spark-jobs/optimize_delta.py",
        trigger_rule="all_success",
    )

    wait >> ingest >> validate_task >> compact
```

## Exercise

1. Rewrite the `FileSensor` to use `mode="poke"` instead of
   `"reschedule"` and explain the operational tradeoff (worker slot usage
   vs. detection latency) between the two modes.
2. Given a `SparkSubmitOperator` with `retries=3`, explain why a batch
   job that appends without deduplication is unsafe under Airflow's retry
   model, and rewrite the write step to make it safe.
3. Design (in words, plus the operator skeleton) a DAG branch that runs a
   Spark data-quality check and only proceeds to a downstream
   `publish_to_bi` task if the check passes, failing the DAG loudly
   otherwise.
