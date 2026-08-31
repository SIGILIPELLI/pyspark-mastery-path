# 05 · Schema Evolution Production

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented
    Spark/Delta behavior, not run against a live cluster here.

Production tables outlive the schema you designed them with. New columns
get added upstream, types get widened, fields get renamed or dropped —
and your pipeline has to keep working (or fail loudly and safely) through
all of it. This module covers the practical patterns for handling schema
change without breaking downstream consumers.

## The baseline: strict schema enforcement

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("schema-evolution").getOrCreate()

v1_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("value", DoubleType()),
])

events_v1 = spark.createDataFrame(
    [(1, "click", 1.0), (2, "purchase", 49.99)], schema=v1_schema
)
events_v1.write.format("delta").mode("overwrite").save("/data/lakehouse/events")
```

## Additive changes: new columns

The safest and most common schema change — a new optional column arrives
upstream.

```python
v2_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("value", DoubleType()),
    StructField("device", StringType()),   # NEW
])

events_v2_batch = spark.createDataFrame(
    [(3, "click", 1.0, "mobile")], schema=v2_schema
)

# events_v2_batch.write.format("delta").mode("append").save("/data/lakehouse/events")
# AnalysisException: schema mismatch — "device" not in target schema

events_v2_batch.write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .save("/data/lakehouse/events")

spark.read.format("delta").load("/data/lakehouse/events").orderBy("user_id").show()
# +-------+----------+-----+------+
# |user_id|event_type|value|device|
# +-------+----------+-----+------+
# |      1|     click|  1.0|  null|   <- old rows backfilled with null for the new column
# |      2|  purchase|49.99|  null|
# |      3|     click|  1.0|mobile|
# +-------+----------+-----+------+
```

`mergeSchema` is opt-in per write deliberately (module 1) — old rows get
`null` for the new column automatically, which is correct as long as
downstream consumers treat that column as genuinely optional.

## Reading old and new schema versions together, from plain files

Without Delta's schema merge, a plain Parquet directory containing files
written under both `v1_schema` and `v2_schema` needs an explicit merge
option at read time:

```python
mixed = (
    spark.read
    .option("mergeSchema", "true")   # also a valid read-time option for Parquet
    .parquet("/data/raw/events_parquet/")
)
mixed.printSchema()
# root
#  |-- user_id: integer
#  |-- event_type: string
#  |-- value: double
#  |-- device: string   <- picked up from whichever files had it
```

This is more expensive than Delta's approach (Spark has to inspect every
file's footer schema rather than trust a single transaction log entry) —
one more reason Delta/lakehouse formats (module 1) are preferred for
tables that evolve frequently.

## Type widening: safe vs. unsafe changes

```python
from pyspark.sql.functions import col

# Safe (widening): int -> long, float -> double, non-nullable -> nullable.
# Spark/Delta can generally read old int-typed files as if they were long.
widened_schema_note = "user_id: IntegerType -> LongType is safe"

# Unsafe (narrowing or incompatible): long -> int (possible overflow),
# string -> int (parse failures on existing data), any -> non-nullable.
narrowing_example = spark.createDataFrame([(1, "9999999999")], ["id", "big_str"])
# .withColumn("big_str", col("big_str").cast("int")) on a value like this
# silently produces null (overflow) rather than raising — ALWAYS validate
# row counts / null counts before and after a narrowing cast in production.
before = narrowing_example.count()
after = narrowing_example.withColumn("big_int", col("big_str").cast("int")).filter(col("big_int").isNotNull()).count()
print(before, after)   # 1, 0 -- silent data loss if unnoticed
```

Never cast a widening-only path without checking: `int -> long` is always
safe; `long -> int`, `double -> float`, or `string -> numeric` can all
silently drop or corrupt data on values outside the narrower type's range
or format, and Spark's default cast behavior is to produce `null` rather
than raise an error.

## Column renames: the pattern that breaks the most things

Spark/Delta schema merge handles *added* columns gracefully but has no
concept of "this old column was renamed to that new name" — from its
perspective a rename is "drop one column, add a different one," which
silently loses the old column's historical data association unless you
handle it explicitly.

```python
# Renaming user_id -> customer_id upstream. WRONG naive approach:
# just start writing customer_id and mergeSchema=true -> you now have BOTH
# columns in the table, with old rows null in customer_id and new rows
# null in user_id.

# Correct approach: explicit backfill/reconciliation as part of the same
# write that introduces the rename.
incoming = spark.createDataFrame([(5, "click", 2.0)], ["customer_id", "event_type", "value"])

from delta.tables import DeltaTable
delta_events = DeltaTable.forPath(spark, "/data/lakehouse/events")

# Coalesce the two id columns going forward, and communicate the rename
# to consumers explicitly rather than relying on schema merge alone.
reconciled = (
    spark.read.format("delta").load("/data/lakehouse/events")
    .withColumn("customer_id", col("user_id"))   # backfill from the old name
    .drop("user_id")
    .unionByName(incoming, allowMissingColumns=True)
)
reconciled.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/data/lakehouse/events")
```

`overwriteSchema=true` is a deliberately heavier-handed option than
`mergeSchema` — it replaces the table's schema outright rather than
merging, appropriate for exactly this kind of explicit, planned migration,
never as a routine append option.

## Defensive reading: contract-checking upstream data

```python
expected_columns = {"user_id", "event_type", "value"}

def validate_schema(df, expected):
    actual = set(df.columns)
    missing = expected - actual
    extra = actual - expected
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")
    if extra:
        print(f"Warning: unexpected new columns present: {extra} — investigate before assuming safe to ignore")

incoming_batch = spark.read.parquet("/data/raw/latest_events")
validate_schema(incoming_batch, expected_columns)
```

A cheap schema contract check like this at the top of a pipeline turns a
silent downstream `AnalysisException` three stages later (or worse, a
silent `null`-filled column nobody notices) into an immediate, clear
failure at the point where the actual upstream change happened.

## Worked example: a schema-aware ingestion function

```python
def safe_ingest(spark, incoming_df, table_path, known_columns):
    incoming_cols = set(incoming_df.columns)
    new_cols = incoming_cols - known_columns
    if new_cols:
        print(f"New columns detected: {new_cols} — allowing via mergeSchema")
    (
        incoming_df.write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .save(table_path)
    )
    return known_columns | incoming_cols

known_columns = {"user_id", "event_type", "value"}
known_columns = safe_ingest(spark, events_v2_batch, "/data/lakehouse/events", known_columns)
print(sorted(known_columns))   # ['device', 'event_type', 'user_id', 'value']
```

Logging *when* a new column first appears (rather than silently accepting
it every time) gives you an audit trail for exactly when an upstream
schema change landed — useful when a downstream consumer later asks "when
did this column start existing?"

## Exercise

1. Given a table with a `price: FloatType` column, write the code to
   widen it to `DoubleType` via a full-table rewrite with
   `overwriteSchema=true`, and explain why this direction is safe while
   the reverse would not be.
2. Design (in words) a reconciliation strategy for a column that gets
   split upstream — `full_name: string` becomes `first_name` +
   `last_name` — for both old and new rows in the same table.
3. Extend `validate_schema` to also check column *types*, not just names,
   and explain what production incident this would have caught that a
   name-only check would miss.
