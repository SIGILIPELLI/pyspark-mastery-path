# 10 · Capstone Production Lakehouse

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented
    PySpark/Delta behavior, not run against a live cluster here.

This final capstone assembles every Level 4 module into one coherent
production pipeline: a Delta lakehouse table, upserted nightly, validated
by quality gates, orchestrated by Airflow, schema-aware, cost-conscious,
and covered by tests you'd actually run in CI. Treat this as the
reference shape for "a real production Spark pipeline," not just a
bigger demo.

## The pipeline shape

```
raw clickstream (JSON, landing daily) 
    -> validate schema + quality gate (module 7, 5)
    -> upsert into Delta "events" lakehouse table (module 1)
    -> compact weekly (module 1)
    -> orchestrated by Airflow with sensors + retries (module 2)
    -> cost-aware executor sizing (module 3)
    -> debuggable via Spark UI + clear failure modes (module 4, 8)
    -> tested in CI before deploy (module 8)
```

## Step 1 — the transformation module (testable, no entrypoint logic)

```python
# src/lakehouse_pipeline/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

EXPECTED_COLUMNS = {"event_id", "user_id", "event_type", "value", "event_date"}

def validate_schema(df: DataFrame, expected: set) -> None:
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

def apply_quality_gate(df: DataFrame, max_reject_pct: float = 0.2):
    checked = df.withColumn(
        "chk_ok",
        col("user_id").isNotNull() & col("event_id").isNotNull() & (col("value") >= 0),
    )
    good = checked.filter(col("chk_ok")).drop("chk_ok")
    bad = checked.filter(~col("chk_ok")).drop("chk_ok")

    total = checked.count()
    rejects = bad.count()
    reject_pct = rejects / total if total else 1.0
    if reject_pct > max_reject_pct:
        raise ValueError(f"Reject rate {reject_pct:.1%} exceeds {max_reject_pct:.1%} threshold")
    return good, bad
```

## Step 2 — unit tests against the transformation module

```python
# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from lakehouse_pipeline.transformations import validate_schema, apply_quality_gate, EXPECTED_COLUMNS

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("capstone-tests").getOrCreate()

def test_validate_schema_raises_on_missing_column(spark):
    df = spark.createDataFrame([(1, 2, "click", 1.0)], ["event_id", "user_id", "event_type", "value"])
    with pytest.raises(ValueError, match="event_date"):
        validate_schema(df, EXPECTED_COLUMNS)

def test_quality_gate_quarantines_bad_rows(spark):
    df = spark.createDataFrame(
        [(1, 1, "click", 1.0, "2024-01-01"), (2, None, "click", 1.0, "2024-01-01")],
        ["event_id", "user_id", "event_type", "value", "event_date"],
    )
    good, bad = apply_quality_gate(df, max_reject_pct=0.6)
    assert good.count() == 1
    assert bad.count() == 1

def test_quality_gate_raises_when_reject_rate_too_high(spark):
    df = spark.createDataFrame(
        [(1, None, "click", 1.0, "2024-01-01"), (2, None, "click", 1.0, "2024-01-01")],
        ["event_id", "user_id", "event_type", "value", "event_date"],
    )
    with pytest.raises(ValueError, match="Reject rate"):
        apply_quality_gate(df, max_reject_pct=0.5)
```

## Step 3 — the Delta upsert entrypoint

```python
# src/lakehouse_pipeline/job.py
import argparse
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from lakehouse_pipeline.transformations import validate_schema, apply_quality_gate, EXPECTED_COLUMNS

TABLE_PATH = "/data/lakehouse/events"
QUARANTINE_PATH = "/data/quarantine/events"

def run(spark, run_date):
    incoming = spark.read.json(f"/data/raw/clickstream/date={run_date}")
    validate_schema(incoming, EXPECTED_COLUMNS)
    good, bad = apply_quality_gate(incoming)

    bad.write.mode("append").parquet(QUARANTINE_PATH)

    if DeltaTable.isDeltaTable(spark, TABLE_PATH):
        target = DeltaTable.forPath(spark, TABLE_PATH)
        (
            target.alias("t")
            .merge(good.alias("s"), "t.event_id = s.event_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        good.write.format("delta").mode("overwrite").save(TABLE_PATH)

    from datetime import date
    if date.fromisoformat(run_date).weekday() == 0:
        spark.sql(f"OPTIMIZE delta.`{TABLE_PATH}` ZORDER BY (user_id)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"lakehouse-events-{args.run_date}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()

    )
    run(spark, args.run_date)
    spark.stop()
```

`run()` is separated from the `__main__` block specifically so it, too,
can be integration-tested with a local SparkSession and a temp directory
standing in for `TABLE_PATH`, without needing `spark-submit` at all.

## Step 4 — Airflow DAG wiring it together

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

default_args = {"retries": 2, "retry_delay": timedelta(minutes=10), "email_on_failure": True}

with DAG(
    dag_id="production_lakehouse_events",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    wait_for_raw = FileSensor(
        task_id="wait_for_raw_clickstream",
        filepath="/data/raw/clickstream/date={{ ds }}/_SUCCESS",
        poke_interval=300,
        timeout=14400,
        mode="reschedule",
    )

    upsert = SparkSubmitOperator(
        task_id="upsert_events",
        application="dist/lakehouse_pipeline-1.0.0-py3-none-any.whl#job.py",
        application_args=["--run-date", "{{ ds }}"],
        conf={
            "spark.executor.memory": "8g",
            "spark.executor.memoryOverhead": "2g",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.maxExecutors": "20",
        },
        execution_timeout=timedelta(hours=1),
    )

    wait_for_raw >> upsert
```

The retry/idempotency reasoning from module 2 holds here specifically
*because* the write is a Delta `MERGE` keyed on `event_id`, not a plain
append — Airflow re-running `upsert_events` after a transient failure
re-executes the same `MERGE` safely, producing the same end state rather
than duplicate rows.

## Step 5 — verifying the pipeline end to end

```python
# A post-deploy smoke check, run once after the first production execution:
spark = SparkSession.builder.appName("smoke-check") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

result = spark.read.format("delta").load("/data/lakehouse/events")
print(f"Row count: {result.count()}")
print(f"Distinct event_id count: {result.select('event_id').distinct().count()}")
# These two numbers matching confirms the MERGE key is actually unique --
# a mismatch here would mean event_id isn't the true dedup key and the
# MERGE condition needs revisiting.

history = DeltaTable.forPath(spark, "/data/lakehouse/events").history(5)
history.select("version", "operation", "operationMetrics").show(truncate=False)
```

## Step 6 — cost and monitoring checklist before calling it "done"

```python
production_checklist = [
    "Executor sizing justified against actual data volume, not copy-pasted (module 3)",
    "dynamicAllocation enabled with a sane maxExecutors ceiling (module 6)",
    "Quality gate threshold tuned against real historical reject rates, not a guess (module 7)",
    "Quarantine table has its own retention/review process, not write-and-forget (module 7)",
    "OPTIMIZE scheduled on a cadence (weekly here), not every run (module 1)",
    "CI runs the unit tests above on every PR before merge (module 8)",
    "Airflow alerting (email_on_failure) actually routes to someone who'll act on it (module 2)",
]
for item in production_checklist:
    print("[ ]", item)
```

## Exercise

1. Add a fourth Airflow task, downstream of `upsert_events`, that runs
   the smoke check from Step 5 as a `PythonOperator` and fails the DAG if
   `event_id` isn't unique in the target table.
2. Extend `apply_quality_gate` to log (not just quarantine) the specific
   check that failed per row, so a reviewer of the quarantine table can
   see *why* each row was rejected, not just that it was.
3. This pipeline chose a Delta `MERGE` keyed on `event_id`. Argue for or
   against using plain `overwrite` on a `date=` partition instead, given
   that clickstream events are naturally append-only and rarely updated
   after the fact.
