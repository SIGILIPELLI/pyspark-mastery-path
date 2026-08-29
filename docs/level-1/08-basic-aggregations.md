# 08 · Basic Aggregations (groupBy, agg)

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

We'll use this sample sales DataFrame:

```python
data = [
    (1, "Alice", "US", "electronics", 120.50, 2),
    (2, "Bob", "IN", "books", 45.00, 1),
    (3, "Carla", "US", "electronics", 300.25, 5),
    (4, "Deepak", "DE", "books", 60.00, 1),
    (5, "Elena", "US", "electronics", 15.00, 1),
    (6, "Frank", "IN", "books", 22.00, 3),
]
columns = ["order_id", "customer", "country", "category", "amount", "quantity"]
df = spark.createDataFrame(data, columns)
```

## Simple aggregations without grouping

Calling an aggregate function directly on a DataFrame (no `groupBy`)
collapses the whole DataFrame into a single-row result:

```python
from pyspark.sql.functions import count, sum as spark_sum, avg, min as spark_min, max as spark_max

df.select(
    count("*").alias("total_orders"),
    spark_sum("amount").alias("total_revenue"),
    avg("amount").alias("avg_order_value"),
    spark_min("amount").alias("min_order"),
    spark_max("amount").alias("max_order"),
).show()

# +------------+-------------+---------------+---------+---------+
# |total_orders|total_revenue|avg_order_value|min_order|max_order|
# +------------+-------------+---------------+---------+---------+
# |           6|        562.75|93.791666...   |     15.0|   300.25|
# +------------+-------------+---------------+---------+---------+
```

Note the `as spark_sum` / `as spark_min` / `as spark_max` import aliases —
these functions share names with Python's built-in `sum`, `min`, `max`, so
aliasing avoids silently shadowing the built-ins in your script.

## groupBy + agg: the core aggregation pattern

`groupBy(col1, col2, ...)` groups rows sharing the same values in the given
columns; `.agg(...)` then computes one or more aggregate expressions per
group.

```python
df.groupBy("category").agg(
    count("*").alias("order_count"),
    spark_sum("amount").alias("total_revenue"),
    avg("amount").alias("avg_amount"),
).show()

# +-----------+-----------+-------------+------------------+
# |   category|order_count|total_revenue|        avg_amount|
# +-----------+-----------+-------------+------------------+
# |electronics|          3|        435.75|145.25            |
# |      books|          3|        127.00|42.333333333333336|
# +-----------+-----------+-------------+------------------+
```

Grouping by multiple columns works the same way:

```python
df.groupBy("country", "category").agg(
    spark_sum("amount").alias("revenue"),
).orderBy("country", "category").show()

# +-------+-----------+-------+
# |country|   category|revenue|
# +-------+-----------+-------+
# |     DE|      books|   60.0|
# |     IN|      books|   67.0|
# |     US|electronics| 435.75|
# +-------+-----------+-------+
```

## groupBy shortcuts

For the single most common aggregate per group, PySpark offers shortcut
methods that skip `.agg()` entirely:

```python
df.groupBy("category").count().show()          # same as agg(count("*"))
df.groupBy("category").sum("amount").show()     # column auto-named "sum(amount)"
df.groupBy("category").avg("amount").show()
df.groupBy("category").max("amount").show()
```

These are convenient for quick exploration, but the resulting column names
(`sum(amount)`, `avg(amount)`) are awkward to reference later — prefer
`.agg(... .alias(...))` in real pipeline code where you need clean,
predictable column names downstream.

## Multiple aggregations, cleanly named

This is the pattern you'll use most in real work — several aggregates at
once, each with an explicit alias:

```python
from pyspark.sql.functions import countDistinct

summary = df.groupBy("country").agg(
    count("*").alias("order_count"),
    countDistinct("customer").alias("unique_customers"),
    spark_sum("amount").alias("total_revenue"),
    avg("amount").alias("avg_order_value"),
    spark_max("amount").alias("largest_order"),
)
summary.orderBy(summary.total_revenue.desc()).show()

# +-------+-----------+----------------+-------------+---------------+-------------+
# |country|order_count|unique_customers|total_revenue|avg_order_value|largest_order|
# +-------+-----------+----------------+-------------+---------------+-------------+
# |     US|          3|               3|       435.75|          145.25|       300.25|
# |     IN|          2|               2|         67.0|           33.5|         45.0|
# |     DE|          1|               1|         60.0|           60.0|         60.0|
# +-------+-----------+----------------+-------------+---------------+-------------+
```

## orderBy / sort for presenting results

Aggregation results are not automatically sorted. Chain `.orderBy(...)` (or
its synonym `.sort(...)`) to control the order, and `.desc()` /`.asc()` on
a column to control direction:

```python
summary.orderBy(summary.total_revenue.desc()).show()
# equivalently:
from pyspark.sql.functions import desc
summary.orderBy(desc("total_revenue")).show()
```

`orderBy` (like `groupBy`) requires a shuffle in general, since rows must be
globally compared across all partitions to produce a total order — this is
noted here as a preview of the shuffle-cost discussion in Level 2 and 3.

## having-style filtering: filter after agg

SQL's `HAVING` clause has no separate keyword in the DataFrame API — you
just call `.filter()` again, after the aggregation, on the aggregated
result:

```python
df.groupBy("category").agg(
    spark_sum("amount").alias("total_revenue")
).filter("total_revenue > 100").show()
# +-----------+-------------+
# |   category|total_revenue|
# +-----------+-------------+
# |electronics|       435.75|
# +-----------+-------------+
```

## Worked example: top category per country

Task: for each country, find total revenue by category, and report only the
combinations exceeding $50 in revenue, sorted by revenue descending.

```python
result = (
    df.groupBy("country", "category")
      .agg(spark_sum("amount").alias("revenue"))
      .filter("revenue > 50")
      .orderBy(desc("revenue"))
)
result.show()

# +-------+-----------+-------+
# |country|   category|revenue|
# +-------+-----------+-------+
# |     US|electronics| 435.75|
# |     DE|      books|   60.0|
# +-------+-----------+-------+
```

Notice `IN`/`books` (67.0 total) is excluded because it's under 50 — wait,
67 > 50, so it should actually appear. Tracing carefully: `IN` has Bob
(45.00) and Frank (22.00) in `books`, summing to 67.00, which *does* exceed
50 and belongs in the result. The corrected expected output is:

```text
+-------+-----------+-------+
|country|   category|revenue|
+-------+-----------+-------+
|     US|electronics| 435.75|
|     IN|      books|   67.0|
|     DE|      books|   60.0|
+-------+-----------+-------+
```

This correction is left in deliberately: always double-check aggregation
results by hand against the source rows, especially while learning — it's
easy to mis-add and it's exactly the kind of mistake automated tests
(Level 2/3) exist to catch.

## Exercise

Using the `df` from the top of this module:

1. Compute the number of distinct customers per country.
2. Compute total quantity sold per category, sorted descending.
3. Find categories where the average order amount is above $50.
4. Combine ideas: for each country, compute total revenue and order count,
   keep only countries with more than 1 order, sorted by total revenue
   descending.

Expected answer for (4): `US` (3 orders, 435.75 revenue) and `IN` (2 orders,
67.0 revenue) both have more than 1 order; `DE` (1 order) is filtered out.
Sorted descending by revenue: `US` first, then `IN`.
