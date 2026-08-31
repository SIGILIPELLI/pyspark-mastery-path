# 03 · Handling Skewed Data

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Data skew is when one or a few keys hold a disproportionate share of the
rows. A `groupBy` or `join` on a skewed key sends most of the data to a
single task while the other 199 tasks finish in seconds and one straggler
runs for an hour. This module covers how to detect skew and the three
main techniques to fix it.

## Building a deliberately skewed dataset

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, floor, rand
import random

spark = SparkSession.builder.appName("skew-handling").getOrCreate()

# 90% of rows belong to store_id=1 ("hot key"); the rest spread over 49 stores.
n = 1_000_000
events = spark.range(0, n).withColumn(
    "store_id",
    when(col("id") % 10 < 9, lit(1)).otherwise((col("id") % 49) + 2),
).withColumn("amount", (col("id") % 100).cast("double"))

events.groupBy("store_id").count().orderBy(col("count").desc()).show(5)
# +--------+------+
# |store_id| count|
# +--------+------+
# |       1|900000|
# |       2| 18367|
# |       3| 18367|
# |       4| 18367|
# |       5| 18367|
# +--------+------+
```

`store_id=1` holds 90% of the data — a task processing that key's
partition will take roughly 49x longer than a task handling an average
key.

## Detecting skew: the Spark UI symptom

Before fixing skew you have to confirm it's actually skew. In the Spark
UI's Stages tab, a skewed stage shows a huge gap between the **median**
task duration and the **max** task duration — e.g. median 2s, max 40min,
with one task's "Shuffle Read Size" dwarfing the rest. (Module 8 covers
the Spark UI in depth.) In code, you can approximate this check yourself:

```python
counts = events.groupBy("store_id").count()
stats = counts.selectExpr(
    "percentile_approx(count, 0.5) as median_count",
    "max(count) as max_count",
)
stats.show()
# +------------+---------+
# |median_count|max_count|
# +------------+---------+
# |       18367|   900000|
# +------------+---------+
```

A max/median ratio this large (≈49x) is a strong skew signal before you
even run the job.

## Technique 1: salting the skewed key

Split the hot key into several synthetic sub-keys, aggregate on the
salted key, then combine the partial results.

```python
NUM_SALTS = 20

salted = events.withColumn(
    "salt", (rand(seed=42) * NUM_SALTS).cast("int")
).withColumn(
    "salted_store_id", concat(col("store_id").cast("string"), lit("_"), col("salt"))
)

# Stage 1: aggregate on the salted key — spreads store_id=1's rows across
# 20 sub-keys instead of one, so no single task gets 900k rows.
partial = salted.groupBy("salted_store_id", "store_id").count()

# Stage 2: re-aggregate by the real key to combine the salted partials.
final = partial.groupBy("store_id").agg({"count": "sum"}).withColumnRenamed("sum(count)", "count")

final.orderBy(col("count").desc()).show(5)
# +--------+------+
# |store_id| count|
# +--------+------+
# |       1|900000|
# |       2| 18367|
# ...
```

Salting adds a second shuffle stage, but each individual shuffle is now
balanced — a net win once the skew ratio is large enough that the
straggler task would otherwise dominate wall-clock time.

## Technique 2: salting a skewed join

Salting matters most for joins, where a hot key on the large side means
one reducer does a disproportionate amount of the join work.

```python
stores = spark.createDataFrame(
    [(i, f"Store {i}") for i in range(1, 51)], ["store_id", "store_name"]
)

# Explode the small side to match every possible salt value of the large side,
# so each salted partition of the large side has a matching small-side row.
stores_salted = stores.crossJoin(
    spark.range(0, NUM_SALTS).withColumnRenamed("id", "salt")
).withColumn(
    "salted_store_id", concat(col("store_id").cast("string"), lit("_"), col("salt"))
)

events_salted = events.withColumn(
    "salt", (rand(seed=7) * NUM_SALTS).cast("int")
).withColumn(
    "salted_store_id", concat(col("store_id").cast("string"), lit("_"), col("salt"))
)

joined = events_salted.join(
    stores_salted.select("salted_store_id", "store_name"), "salted_store_id"
)
joined.select("store_id", "store_name", "amount").show(3)
```

If `stores` is small enough to broadcast outright (it is here — 50 rows),
plain `broadcast()` is simpler and preferable; salting a join is worth the
complexity specifically when **both** sides are too large to broadcast
and one side is still skewed.

## Technique 3: isolate and handle the hot key separately

Sometimes the simplest fix is to split the DataFrame, process the hot key
with different parallelism, and union the results back.

```python
hot_key_value = 1

hot = events.filter(col("store_id") == hot_key_value).repartition(200)
cold = events.filter(col("store_id") != hot_key_value)

hot_result = hot.groupBy("store_id").count()
cold_result = cold.groupBy("store_id").count()

combined = hot_result.unionByName(cold_result)
combined.orderBy(col("count").desc()).show(5)
```

Repartitioning just the hot-key slice to 200 partitions spreads its
900,000 rows across many tasks, while the cold slice (already well
distributed across 49 keys) doesn't need the extra shuffle overhead.

## Adaptive Query Execution's built-in skew handling

Spark 3.x's AQE can detect and split skewed partitions automatically at
runtime — covered in depth in module 6 — via:

```python
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
```

With AQE skew join handling on, Spark inspects post-shuffle partition
sizes and automatically splits oversized ones into smaller sub-tasks,
often making manual salting unnecessary for join skew specifically (it
does not help with `groupBy` skew — salting is still the tool there).

## Exercise

1. Using the `events` DataFrame above, measure the max/median partition
   size ratio before and after applying the salting technique from
   Technique 1 — confirm the ratio drops close to 1.
2. Explain why salting a `groupBy` requires a second aggregation stage
   but a broadcast join does not require any equivalent "combine" step.
3. For the skewed join in Technique 2, what happens to correctness if you
   forget to give the small side the *same* `NUM_SALTS` value used to
   salt the large side? Trace through with a concrete example.
