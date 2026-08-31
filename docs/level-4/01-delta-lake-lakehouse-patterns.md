# 01 · Delta Lake Lakehouse Patterns

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented
    Delta Lake / PySpark behavior, not run against a live cluster here.

Plain Parquet on object storage gives you columnar storage and predicate
pushdown, but no transactions, no reliable schema enforcement, and no way
to update or delete rows without rewriting whole files. Delta Lake adds a
transaction log on top of Parquet that gives you ACID writes, time travel,
and efficient upserts — the foundation of a "lakehouse." This module
covers the operations you'll use daily.

## Setup

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("delta-lakehouse")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
```

## Writing and reading a Delta table

```python
from pyspark.sql.functions import col

customers = spark.createDataFrame(
    [(1, "Alice", "US", 100.0), (2, "Bob", "IN", 250.0), (3, "Carla", "DE", 75.0)],
    ["customer_id", "name", "country", "lifetime_value"],
)

customers.write.format("delta").mode("overwrite").save("/data/lakehouse/customers")

df = spark.read.format("delta").load("/data/lakehouse/customers")
df.show()
# +-----------+-----+-------+--------------+
# |customer_id| name|country|lifetime_value|
# +-----------+-----+-------+--------------+
# |          1|Alice|     US|         100.0|
# |          2|  Bob|     IN|         250.0|
# |          3|Carla|     DE|          75.0|
# +-----------+-----+-------+--------------+
```

On disk, this looks like a normal Parquet directory plus a `_delta_log/`
subdirectory holding JSON commit files (`00000000000000000000.json`, ...)
— each write appends a new commit describing exactly which Parquet files
were added/removed, which is what makes every operation atomic.

## `MERGE INTO`: upserts without rewriting the whole table

This is the operation plain Parquet fundamentally cannot do atomically —
Delta's `MERGE` matches incoming rows against existing ones by key and
updates, inserts, or deletes as instructed, rewriting only the affected
files.

```python
from delta.tables import DeltaTable

delta_customers = DeltaTable.forPath(spark, "/data/lakehouse/customers")

updates = spark.createDataFrame(
    [(2, "Bob", "IN", 300.0),      # existing customer, LTV changed
     (4, "Dane", "US", 50.0)],     # new customer
    ["customer_id", "name", "country", "lifetime_value"],
)

(
    delta_customers.alias("target")
    .merge(updates.alias("source"), "target.customer_id = source.customer_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

delta_customers.toDF().orderBy("customer_id").show()
# +-----------+-----+-------+--------------+
# |customer_id| name|country|lifetime_value|
# +-----------+-----+-------+--------------+
# |          1|Alice|     US|         100.0|
# |          2|  Bob|     IN|         300.0|   <- updated
# |          3|Carla|     DE|          75.0|
# |          4| Dane|     US|          50.0|   <- inserted
# +-----------+-----+-------+--------------+
```

`whenMatchedUpdateAll()`/`whenNotMatchedInsertAll()` are the common case;
you can also pass explicit column maps or conditions
(`.whenMatchedUpdate(condition=..., set={...})`) for partial or
conditional updates.

## `UPDATE` and `DELETE` directly

```python
delta_customers.update(
    condition="country = 'DE'",
    set={"lifetime_value": "lifetime_value * 1.1"},
)

delta_customers.delete(condition="lifetime_value < 60")

delta_customers.toDF().orderBy("customer_id").show()
# customer_id=4 (lifetime_value=50.0) removed; customer_id=3's value bumped ~10%
```

Both operations are transactional — a concurrent reader never sees a
partially-updated table, and a failed `UPDATE`/`DELETE` leaves the table
exactly as it was before the operation started.

## Time travel: querying a previous version

Every commit is a numbered version. You can query any historical version
either by version number or timestamp:

```python
history = delta_customers.history()
history.select("version", "timestamp", "operation").show(truncate=False)
# +-------+-----------------------+---------------+
# |version|timestamp              |operation      |
# +-------+-----------------------+---------------+
# |3      |2024-01-15 10:04:02    |DELETE         |
# |2      |2024-01-15 10:03:11    |UPDATE         |
# |1      |2024-01-15 10:02:45    |MERGE          |
# |0      |2024-01-15 10:01:00    |WRITE          |
# +-------+-----------------------+---------------+

as_of_v1 = spark.read.format("delta").option("versionAsOf", 1).load("/data/lakehouse/customers")
as_of_v1.show()   # the table exactly as it looked right after the MERGE, before UPDATE/DELETE

# Or by timestamp:
as_of_ts = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-15 10:02:00") \
    .load("/data/lakehouse/customers")
```

Time travel is invaluable for debugging ("what did this table look like
before yesterday's bad load?") and for reproducible ML training sets, but
it's not unlimited — old files eventually get physically deleted by
`VACUUM` (below), after which old versions referencing them can no longer
be read.

## Schema enforcement and evolution

```python
bad_schema = spark.createDataFrame([(5, "Eve")], ["customer_id", "name"])

# bad_schema.write.format("delta").mode("append").save("/data/lakehouse/customers")
# AnalysisException: A schema mismatch detected when writing to the Delta table.
# ... missing fields: country, lifetime_value

# Opt in explicitly when the schema change is intentional:
bad_schema.write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .save("/data/lakehouse/customers")
```

Schema enforcement by default is a deliberate safety feature — it's what
prevents a silent, buggy upstream change from corrupting a table's
structure without anyone noticing (module 5 covers this in far more
depth for production evolution scenarios).

## `OPTIMIZE` and `VACUUM`: file compaction and cleanup

```python
# Frequent small writes (e.g., from streaming or many small MERGEs) leave
# behind many small files. OPTIMIZE compacts them into fewer, larger ones.
spark.sql("OPTIMIZE delta.`/data/lakehouse/customers`")

# Z-ORDER co-locates related data within files for columns you filter on often —
# improves data-skipping effectiveness beyond simple compaction.
spark.sql("OPTIMIZE delta.`/data/lakehouse/customers` ZORDER BY (country)")

# VACUUM physically deletes files no longer referenced by the current table
# state and older than the retention threshold (default 7 days) --
# this is what actually reclaims storage, and what eventually cuts off time travel.
spark.sql("VACUUM delta.`/data/lakehouse/customers` RETAIN 168 HOURS")
```

Never run `VACUUM` with a retention window shorter than any
concurrently-running long read or time-travel query might need — doing so
can delete files a running query still expects to find.

## Worked example: a nightly upsert pipeline

```python
def nightly_upsert(spark, incoming_df, table_path):
    delta_table = DeltaTable.forPath(spark, table_path)
    (
        delta_table.alias("t")
        .merge(incoming_df.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    # Compact roughly weekly, not every run, to avoid over-optimizing small deltas.
    from datetime import date
    if date.today().weekday() == 0:
        spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY (country)")

nightly_upsert(spark, updates, "/data/lakehouse/customers")
```

## Exercise

1. Run a `MERGE` that also deletes target rows not present in the source
   (`whenNotMatchedBySourceDelete`) — write the PySpark call and explain
   what real-world scenario this "full sync" pattern fits.
2. Query `customers` `versionAsOf` version 0, then run `VACUUM ... RETAIN
   0 HOURS` (force immediate cleanup) — explain what happens if you then
   try to re-read version 0.
3. Explain why `mergeSchema=true` should be a deliberate per-write opt-in
   rather than a default-on setting, in terms of what upstream bug it
   would otherwise mask.
