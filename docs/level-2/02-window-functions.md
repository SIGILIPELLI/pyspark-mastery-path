# 02 · Window Functions

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Window functions compute a value per row using a "window" of related rows
— e.g. rank within a group, running total, previous row's value — without
collapsing rows the way `groupBy` does. This is the tool for "top N per
group", running totals, and row-to-row comparisons.

```python
data = [
    ("Alice", "electronics", 300.0),
    ("Bob", "electronics", 150.0),
    ("Carla", "electronics", 450.0),
    ("Deepak", "books", 40.0),
    ("Elena", "books", 60.0),
    ("Frank", "books", 60.0),
]
df = spark.createDataFrame(data, ["salesperson", "category", "revenue"])
```

## Defining a window spec

A `Window` spec has three optional pieces: `partitionBy` (which rows are
compared together — like `groupBy`'s key), `orderBy` (the ordering inside
each partition), and a frame (which rows around the current one are
included — default is the whole partition for ranking functions).

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank, row_number, col

w = Window.partitionBy("category").orderBy(col("revenue").desc())
```

## Ranking functions: rank, dense_rank, row_number

```python
ranked = df.withColumn("rank", rank().over(w)) \
           .withColumn("dense_rank", dense_rank().over(w)) \
           .withColumn("row_number", row_number().over(w))
ranked.orderBy("category", "rank").show()

# +-----------+-----------+-------+----+----------+----------+
# |salesperson|   category|revenue|rank|dense_rank|row_number|
# +-----------+-----------+-------+----+----------+----------+
# |      Elena|      books|   60.0|   1|         1|         1|
# |      Frank|      books|   60.0|   1|         1|         2|
# |     Deepak|      books|   40.0|   3|         2|         3|
# |     Carla |electronics|  450.0|   1|         1|         1|
# |      Alice|electronics|  300.0|   2|         2|         2|
# |        Bob|electronics|  150.0|   3|         3|         3|
# +-----------+-----------+-------+----+----------+----------+
```

Elena and Frank tie at 60.0. `rank()` gives both rank 1 and *skips* rank 2
(Deepak gets 3); `dense_rank()` gives both rank 1 and Deepak gets 2 (no
gap); `row_number()` breaks the tie arbitrarily (by physical row order)
and always produces distinct, sequential numbers 1..N per partition. Use
`row_number()` when you need exactly one row per group ("top 1 per
group"); use `rank`/`dense_rank` when ties should be reported as ties.

## Top-N per group with row_number

The single most common use of window functions: get the top 2
salespeople by revenue in each category.

```python
top2 = ranked.filter(col("row_number") <= 2).select(
    "category", "salesperson", "revenue"
).orderBy("category", "revenue", ascending=[True, False])
top2.show()

# +-----------+-----------+-------+
# |   category|salesperson|revenue|
# +-----------+-----------+-------+
# |      books|      Elena|   60.0|
# |      books|      Frank|   60.0|
# |electronics|     Carla |  450.0|
# |electronics|      Alice|  300.0|
# +-----------+-----------+-------+
```

Filtering on a window column requires `.filter()` after the
`.withColumn(...).over(w)` call — you cannot filter inside the `over()`
clause itself.

## Aggregates over a window: running totals

Aggregate functions also work `.over(w)` — this is how you compute a
running total without a self-join.

```python
from pyspark.sql.functions import sum as spark_sum

running = Window.partitionBy("category").orderBy(col("revenue").desc()) \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("running_total", spark_sum("revenue").over(running)) \
  .orderBy("category", col("revenue").desc()).show()

# +-----------+-----------+-------+-------------+
# |salesperson|   category|revenue|running_total|
# +-----------+-----------+-------+-------------+
# |      Elena|      books|   60.0|         60.0|
# |      Frank|      books|   60.0|        120.0|
# |     Deepak|      books|   40.0|        160.0|
# |     Carla |electronics|  450.0|        450.0|
# |      Alice|electronics|  300.0|        750.0|
# |        Bob|electronics|  150.0|        900.0|
# +-----------+-----------+-------+-------------+
```

`rowsBetween(Window.unboundedPreceding, Window.currentRow)` says "include
every row from the start of the partition up to and including the current
row" — the classic running-total frame. Without specifying a frame,
aggregate window functions default to this same range-based frame when an
`orderBy` is present, so `spark_sum("revenue").over(w)` (with plain `w`
from earlier) would actually give the same running total — but it's best
practice to be explicit with `rowsBetween` so the frame is unambiguous to
future readers.

## Row-to-row comparisons: lag and lead

```python
from pyspark.sql.functions import lag, lead

w2 = Window.partitionBy("category").orderBy(col("revenue").desc())

df.withColumn("prev_revenue", lag("revenue", 1).over(w2)) \
  .withColumn("next_revenue", lead("revenue", 1).over(w2)) \
  .orderBy("category", col("revenue").desc()).show()

# +-----------+-----------+-------+------------+------------+
# |salesperson|   category|revenue|prev_revenue|next_revenue|
# +-----------+-----------+-------+------------+------------+
# |      Elena|      books|   60.0|        null|        60.0|
# |      Frank|      books|   60.0|        60.0|        40.0|
# |     Deepak|      books|   40.0|        60.0|        null|
# |     Carla |electronics|  450.0|        null|       300.0|
# |      Alice|electronics|  300.0|       450.0|       150.0|
# |        Bob|electronics|  150.0|       300.0|        null|
# +-----------+-----------+-------+------------+------------+
```

`lag(col, n)` reaches back `n` rows within the partition's order; `lead`
reaches forward. Both return `null` when there's no such row — the first
row of each partition has no `prev_revenue`, and the last has no
`next_revenue`. A common use: computing period-over-period deltas
(`col("revenue") - lag("revenue", 1).over(w2)`).

## Worked example: percentage of category total

Task: for each row, compute what percentage of its category's total
revenue that salesperson contributed.

```python
category_total = Window.partitionBy("category")

pct = df.withColumn(
    "pct_of_category",
    (col("revenue") / spark_sum("revenue").over(category_total) * 100)
).orderBy("category", col("revenue").desc())
pct.show()

# +-----------+-----------+-------+------------------+
# |salesperson|   category|revenue|  pct_of_category|
# +-----------+-----------+-------+------------------+
# |      Elena|      books|   60.0| 37.5             |
# |      Frank|      books|   60.0| 37.5             |
# |     Deepak|      books|   40.0| 25.0             |
# |     Carla |electronics|  450.0| 50.0             |
# |      Alice|electronics|  300.0|33.333333333333336|
# |        Bob|electronics|  150.0|16.666666666666668|
# +-----------+-----------+-------+------------------+
```

Note `category_total` has no `orderBy` — because we want the sum over the
*entire* partition, not a running total, and the default frame for an
aggregate window function with no `orderBy` is the whole partition.

## Exercise

Using `df` from the top of this module:

1. Add a `dense_rank` column ranking salespeople within each category by
   revenue descending, and list only rank-1 rows (expect a tie in `books`).
2. Compute a running total ordered by revenue *ascending* within each
   category and confirm the final row's running total equals the
   category's overall total.
3. Use `lag` to compute, for each row (ordered by revenue descending
   within category), the revenue gap to the next-highest performer.
4. Combine ideas: find the single salesperson in each category who is
   *farthest* below the category average revenue (hint: window `avg`
   without `orderBy`, then subtract and sort).
