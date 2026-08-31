# 07 · Data Quality Gates

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented
    PySpark behavior, not run against a live cluster here.

A pipeline that runs successfully and writes wrong data is worse than one
that fails loudly — a failure gets noticed and fixed; silently wrong data
gets consumed by dashboards and decisions before anyone catches it. This
module covers building explicit quality gates into a pipeline: checks
that halt the pipeline (or quarantine bad rows) before bad data reaches a
production table.

## The core pattern: check, then branch

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as spark_count, when, lit

spark = SparkSession.builder.appName("data-quality-gates").getOrCreate()

orders = spark.createDataFrame(
    [
        (1, 101, 250.00, "2024-01-05"),
        (2, None, 75.50, "2024-01-05"),      # missing customer_id
        (3, 101, -40.00, "2024-01-06"),      # negative amount
        (4, 103, 500.00, None),              # missing order_date
        (5, 104, 12.25, "2024-01-07"),
    ],
    ["order_id", "customer_id", "amount", "order_date"],
)
```

## Row-level checks: null and range validation

```python
checked = (
    orders
    .withColumn("chk_customer_id_present", col("customer_id").isNotNull())
    .withColumn("chk_amount_nonnegative", col("amount") >= 0)
    .withColumn("chk_order_date_present", col("order_date").isNotNull())
)

checked = checked.withColumn(
    "chk_all_passed",
    col("chk_customer_id_present") & col("chk_amount_nonnegative") & col("chk_order_date_present"),
)

checked.select("order_id", "chk_all_passed").show()
# +--------+--------------+
# |order_id|chk_all_passed|
# +--------+--------------+
# |       1|          true|
# |       2|         false|
# |       3|         false|
# |       4|         false|
# |       5|          true|
# +--------+--------------+
```

## Quarantine pattern: split good and bad rows, never drop silently

```python
good_rows = checked.filter(col("chk_all_passed")).drop(
    "chk_customer_id_present", "chk_amount_nonnegative", "chk_order_date_present", "chk_all_passed"
)
bad_rows = checked.filter(~col("chk_all_passed"))

good_rows.write.mode("append").parquet("/data/warehouse/orders_clean")
bad_rows.write.mode("append").parquet("/data/quarantine/orders_rejected")

print(f"Good: {good_rows.count()}, Quarantined: {bad_rows.count()}")
# Good: 2, Quarantined: 3
```

Writing rejects to a **quarantine table** — rather than a `.filter()`
that just discards them — is the difference between "we can audit and
reprocess what got rejected" and "we lost data with no record it ever
existed." Always quarantine, never silently drop, unless you've
explicitly decided the rejected data has zero value (rare).

## Aggregate-level checks: threshold gates on the whole batch

Row-level checks catch individual bad rows; some problems only show up in
aggregate — an upstream outage that made 90% of a batch empty, for
example, would pass every row-level null check trivially (there's simply
almost no data) while still being a serious quality failure.

```python
def quality_gate(df, expected_min_rows, max_reject_pct, reject_count, total_count):
    if total_count < expected_min_rows:
        raise ValueError(f"Row count {total_count} below expected minimum {expected_min_rows}")
    reject_pct = reject_count / total_count if total_count else 1.0
    if reject_pct > max_reject_pct:
        raise ValueError(f"Reject rate {reject_pct:.1%} exceeds threshold {max_reject_pct:.1%}")
    return True

total = checked.count()
rejects = bad_rows.count()
quality_gate(checked, expected_min_rows=3, max_reject_pct=0.5, reject_count=rejects, total_count=total)
# passes: total=5 >= 3, reject_pct=0.6 > 0.5 -> actually raises here
```

In this example the gate correctly raises: a 60% reject rate on this
batch is high enough that continuing to write `good_rows` downstream
without a human looking at it first would be irresponsible — this is
exactly the "fail loudly" behavior a quality gate exists to provide.

## Schema and referential checks

```python
def check_no_duplicate_keys(df, key_cols):
    total = df.count()
    distinct = df.select(*key_cols).distinct().count()
    if total != distinct:
        raise ValueError(f"Found {total - distinct} duplicate rows on key {key_cols}")

check_no_duplicate_keys(good_rows, ["order_id"])   # passes here — order_id is unique

customers = spark.createDataFrame([(101, "Alice"), (103, "Carla")], ["customer_id", "name"])

def check_referential_integrity(fact_df, dim_df, fact_key, dim_key):
    orphans = fact_df.join(dim_df, fact_df[fact_key] == dim_df[dim_key], "left_anti")
    orphan_count = orphans.count()
    if orphan_count > 0:
        print(f"WARNING: {orphan_count} rows in fact table have no matching dimension row")
    return orphans

orphans = check_referential_integrity(good_rows, customers, "customer_id", "customer_id")
orphans.show()
# +--------+-----------+------+----------+
# |order_id|customer_id|amount|order_date|
# +--------+-----------+------+----------+
# |       5|        104|  12.25|2024-01-07|
# +--------+-----------+------+----------+
```

## Statistical drift checks: catching "silently different" data

```python
def check_distribution_shift(current_df, historical_avg, historical_stddev, column, z_threshold=3.0):
    from pyspark.sql.functions import avg, stddev
    stats = current_df.select(avg(col(column)).alias("mean")).collect()[0]
    current_mean = stats["mean"] or 0.0
    z_score = abs(current_mean - historical_avg) / historical_stddev if historical_stddev else 0
    if z_score > z_threshold:
        print(f"WARNING: {column} mean {current_mean:.2f} is {z_score:.1f} std devs from historical {historical_avg:.2f}")
    return z_score

check_distribution_shift(good_rows, historical_avg=180.0, historical_stddev=60.0, column="amount")
```

A distribution-shift check like this catches problems row-level checks
structurally cannot: every individual `amount` value could be perfectly
valid (positive, non-null, in range) while the whole batch's *average*
has shifted dramatically — a sign of an upstream currency bug, a unit
change, or a broken join multiplying values, well before it shows up as
an obviously "wrong" individual row.

## Building this into a pipeline as a reusable gate function

```python
def run_with_quality_gate(spark, source_path, output_path, quarantine_path):
    df = spark.read.parquet(source_path)

    checked = (
        df.withColumn("chk_ok", col("customer_id").isNotNull() & (col("amount") >= 0) & col("order_date").isNotNull())
    )
    good, bad = checked.filter(col("chk_ok")).drop("chk_ok"), checked.filter(~col("chk_ok"))

    total, rejects = checked.count(), bad.count()
    quality_gate(checked, expected_min_rows=1, max_reject_pct=0.3, reject_count=rejects, total_count=total)

    good.write.mode("append").parquet(output_path)
    bad.write.mode("append").parquet(quarantine_path)
    return {"total": total, "good": total - rejects, "rejected": rejects}
```

Raising an exception from `quality_gate` inside an Airflow-orchestrated
job (module 2) fails the task and triggers Airflow's own alerting/retry
logic — the quality gate and the orchestration layer compose naturally
this way, with the gate owning "is this data good enough" and Airflow
owning "what happens when it isn't."

## Exercise

1. Extend `checked` with a check for duplicate `order_id` values *within
   the incoming batch itself* (not just against the existing table), and
   decide whether a duplicate should be quarantined or deduplicated
   silently — justify your choice.
2. `check_distribution_shift` uses a hardcoded historical mean/stddev.
   Sketch how you'd compute and store those historical baselines so they
   update over time without being skewed by the very anomalies you're
   trying to catch.
3. Given `max_reject_pct=0.3` and a batch where exactly 30.0% of rows
   fail, does `quality_gate` raise or pass? Trace through the comparison
   operator and explain whether that boundary behavior is what you'd
   actually want in production.
