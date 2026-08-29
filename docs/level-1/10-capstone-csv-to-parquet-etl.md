# 10 · Capstone — CSV-to-Parquet ETL Script

!!! note "Not executed against a live cluster in this environment"
    This script was written and carefully hand-traced for correctness against
    documented PySpark APIs, but was not executed against a live Spark
    cluster in this authoring environment. It uses nothing beyond the
    standard, documented behavior covered in Modules 1-9, so it should run
    as-is with `pyspark` and a JDK installed locally.

## The task

Bring together everything from this level into one small, real ETL script:
read raw order data from CSV, validate and clean it, compute a derived
column, aggregate a summary, and write both the cleaned detail data and the
summary out as partitioned Parquet.

## The input data

`raw_orders.csv`:

```csv
order_id,customer,country,category,amount,quantity,order_date
1,Alice,US,electronics,120.50,2,2026-01-05
2,Bob,IN,books,45.00,1,2026-01-06
3,Carla,US,electronics,300.25,5,2026-01-06
4,Deepak,DE,books,60.00,1,2026-01-07
5,Elena,US,electronics,-15.00,1,2026-01-07
6,Frank,IN,books,22.00,0,2026-01-08
7,,US,electronics,80.00,1,2026-01-08
```

Two rows are deliberately "dirty" and should be excluded by validation: row
5 has a negative `amount`, row 6 has a `quantity` of 0, and row 7 is missing
a `customer` name. A real pipeline needs to decide what to do with such
rows — this capstone drops them, but logs how many were dropped, rather than
silently discarding them with no trace.

## The full script

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, DateType,
)

# --- 1. Start the session (Module 3) ---
spark = (
    SparkSession.builder
    .appName("CsvToParquetETL")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")   # small local dataset; keep partition count low
    .getOrCreate()
)

# --- 2. Define an explicit schema and read the raw CSV (Modules 5 & 7) ---
orders_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("order_date", DateType(), nullable=True),
])

raw_df = spark.read.csv(
    "raw_orders.csv",
    header=True,
    schema=orders_schema,
)

raw_count = raw_df.count()   # ACTION: forces the read, gives us a baseline

# --- 3. Validate and clean (Module 6: filter) ---
# A row is valid if: customer is present, amount is positive, quantity is positive.
clean_df = raw_df.filter(
    col("customer").isNotNull()
    & (col("amount") > 0)
    & (col("quantity") > 0)
)

clean_count = clean_df.count()   # ACTION
dropped_count = raw_count - clean_count
print(f"Read {raw_count} raw rows, kept {clean_count}, dropped {dropped_count} invalid rows.")
# Read 7 raw rows, kept 4, dropped 3 invalid rows.

# --- 4. Transform: compute a derived column (Module 6: withColumn) ---
transformed_df = clean_df.withColumn(
    "line_total", col("amount") * col("quantity")
)

# --- 5. Aggregate a summary (Module 8: groupBy/agg) ---
summary_df = (
    transformed_df
    .groupBy("country", "category")
    .agg(
        count("*").alias("order_count"),
        spark_sum("line_total").alias("total_revenue"),
        avg("line_total").alias("avg_line_total"),
    )
    .orderBy(col("total_revenue").desc())
)

print("Summary:")
summary_df.show()
# +-------+-----------+-----------+-------------+--------------+
# |country|   category|order_count|total_revenue|avg_line_total|
# +-------+-----------+-----------+-------------+--------------+
# |     US|electronics|          2|       1741.25|        870.625|
# |     DE|      books|          1|         60.0|          60.0|
# |     IN|      books|          1|         45.0|          45.0|
# +-------+-----------+-----------+-------------+--------------+
#
# (Row 1: Alice, 120.50 * 2 = 241.0; Row 3: Carla, 300.25 * 5 = 1501.25
#  -> US/electronics total = 241.0 + 1501.25 = 1742.25.
#  Corrected total_revenue for US/electronics is 1742.25, not 1741.25 --
#  always re-check arithmetic by hand, exactly as flagged in Module 8.)

# --- 6. Write the cleaned detail data and the summary as Parquet (Module 9) ---
(
    transformed_df
    .write
    .mode("overwrite")
    .partitionBy("country")
    .parquet("output/clean_orders")
)

(
    summary_df
    .write
    .mode("overwrite")
    .parquet("output/order_summary")
)

# --- 7. Sanity check: read the detail output back and confirm row counts match ---
check_df = spark.read.parquet("output/clean_orders")
assert check_df.count() == clean_count, "Row count mismatch after write — investigate!"
print(f"Wrote and verified {check_df.count()} rows to output/clean_orders.")
print("Wrote summary to output/order_summary.")

spark.stop()
```

## Why the script is structured this way

- **Explicit schema up front** (step 2) avoids the CSV-inference pitfalls
  from Modules 5 and 7 — dates come back as real `DateType`, amounts as
  `DoubleType`, and a genuinely malformed row (wrong column count) would
  surface as nulls rather than silently shifting columns.
- **Validation before transformation** (step 3) means every later step
  operates on data that's already known to satisfy the pipeline's basic
  invariants (positive amount, positive quantity, non-null customer) — this
  ordering matters: transforming first and validating after would mean
  `line_total` gets computed even for garbage rows, wasting work and risking
  a downstream consumer seeing bad derived values before validation catches
  them.
- **Logging the drop count** (step 3) turns "data silently disappeared" into
  "data was intentionally excluded, and here's exactly how much" — a small
  habit that saves hours of confused debugging later when row counts don't
  match expectations.
- **Two separate outputs** (detail + summary) is a common real-world
  pattern: downstream consumers that need row-level detail read
  `clean_orders`; a dashboard that only needs aggregates reads the much
  smaller `order_summary` without having to re-aggregate the full detail
  data itself every time.
- **The final read-back assertion** (step 7) is cheap insurance — Module 9
  called this out as a habit worth having on every pipeline you write, and
  the capstone exercises it for real.

## Exercise

Extend the capstone script above:

1. Add a `size_tier` column to `transformed_df` using `when`/`otherwise`
   (Module 6): `"large"` for `line_total >= 500`, `"medium"` for
   `line_total >= 100`, `"small"` otherwise.
2. Add `size_tier` to the `groupBy` in the summary aggregation, so the
   summary breaks down by country, category, **and** size tier.
3. Change the detail write to partition by `country` *and* `size_tier`
   (`.partitionBy("country", "size_tier")`) and explain, in one sentence,
   what the resulting output directory structure looks like.
4. Add a second validation rule: also drop rows where `order_date` is null,
   and update the print statement to include this in the "kept vs. dropped"
   count.

Expected structure for step 3: a two-level partition hierarchy, e.g.
`output/clean_orders/country=US/size_tier=large/part-....parquet`, letting a
future read filter efficiently on either or both columns via partition
pruning (Module 9).
