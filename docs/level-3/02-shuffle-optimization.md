# 02 · Shuffle Optimization

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Shuffles — repartitioning data across the network so rows with the same
key land together — are usually the most expensive stages in a Spark job.
This module covers the knobs that control shuffle behavior and the
patterns that reduce how much data actually has to move.

## What triggers a shuffle

Any wide transformation — one where an output partition can depend on
data from many input partitions — triggers a shuffle: `groupBy`, `join`
(unless broadcast), `distinct`, `orderBy`, `repartition`. Narrow
transformations (`filter`, `select`, `withColumn`, `map`) never shuffle.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder.appName("shuffle-tuning").getOrCreate()

sales = spark.range(0, 1_000_000).withColumn("store_id", (col("id") % 50)) \
    .withColumn("amount", (col("id") % 97).cast("double"))

sales.groupBy("store_id").agg(spark_sum("amount")).explain()
# == Physical Plan ==
# *(2) HashAggregate(keys=[store_id#..], functions=[sum(amount#..)])
# +- Exchange hashpartitioning(store_id#.., 200), ...
#    +- *(1) HashAggregate(keys=[store_id#..], functions=[partial_sum(amount#..)])
#       +- *(1) Project [...]
#          +- *(1) Range (0, 1000000, step=1, splits=8)
```

The `Exchange hashpartitioning(store_id#.., 200)` line is the shuffle:
200 is the default number of post-shuffle partitions, set by
`spark.sql.shuffle.partitions`.

## `spark.sql.shuffle.partitions`: the single biggest shuffle knob

```python
spark.conf.get("spark.sql.shuffle.partitions")   # '200' by default
```

200 is a fixed default written for a much larger cluster than most jobs
run on. With only 50 distinct `store_id` values above, 200 output
partitions means 150 of them are empty — wasted task scheduling overhead.

```python
spark.conf.set("spark.sql.shuffle.partitions", 50)

sales.groupBy("store_id").agg(spark_sum("amount")).explain()
# Exchange hashpartitioning(store_id#.., 50), ...
```

Rule of thumb for batch jobs: aim for shuffle partitions where each
partition holds roughly 100–200 MB of shuffled data post-aggregation.
Too few → each task handles too much data and risks spill/OOM. Too many →
scheduling overhead and small-file proliferation on write.

```python
# Rough sizing formula:
# shuffle_partitions ≈ total_shuffle_bytes / target_partition_size (128 MB)
total_shuffle_mb = 8_000
target_mb = 128
recommended_partitions = max(1, total_shuffle_mb // target_mb)
print(recommended_partitions)   # 62
```

## `repartition()` vs `coalesce()`

```python
df = spark.range(0, 100)

# repartition(): always a full shuffle, can increase or decrease partitions,
# results in evenly balanced partitions.
even = df.repartition(4)

# coalesce(): merges existing partitions WITHOUT a full shuffle when
# reducing partition count — much cheaper, but can leave partitions
# unevenly sized because it only combines adjacent partitions.
merged = df.coalesce(2)

print(even.rdd.getNumPartitions())    # 4
print(merged.rdd.getNumPartitions())  # 2
```

Use `coalesce()` when writing final output and you only need *fewer*
files (e.g., after a filter dramatically shrank the dataset). Use
`repartition()` when you need an even distribution before a shuffle-heavy
operation, or when increasing partition count (`coalesce` cannot increase
partitions).

```python
# Repartitioning by a column, not just a count — colocates rows sharing
# a key BEFORE a downstream groupBy/join, useful when you'll do several
# operations keyed on the same column in sequence.
by_store = sales.repartition(50, "store_id")
```

## Reducing shuffle volume: aggregate before you shuffle

Spark already does partial aggregation automatically for `groupBy().agg()`
(seen in the `partial_sum` node above). But when you control the pipeline
shape, filtering and projecting down to only needed columns *before* a
join or groupBy shrinks what has to cross the network:

```python
# Worse — shuffles all columns of orders, including unused ones
bad = orders.join(customers, "customer_id").groupBy("country").count()

# Better — project down to only what's needed before the wide op
good = (
    orders.select("customer_id")
    .join(customers.select("customer_id", "country"), "customer_id")
    .groupBy("country")
    .count()
)
```

## Map-side combine with `reduceByKey`-style aggregation (RDD API)

If you ever drop to the RDD API, prefer `reduceByKey` over
`groupByKey` — `reduceByKey` combines values per key on the map side
before shuffling, while `groupByKey` shuffles every raw value first and
combines after:

```python
pairs = spark.sparkContext.parallelize(
    [("a", 1), ("b", 1), ("a", 1), ("a", 1), ("b", 1)]
)

# groupByKey: ships every individual value across the network
grouped = pairs.groupByKey().mapValues(sum).collect()

# reduceByKey: sums locally per partition first, ships only partial sums
reduced = pairs.reduceByKey(lambda a, b: a + b).collect()

print(sorted(reduced))  # [('a', 3), ('b', 2)]
```

On the DataFrame API this optimization is automatic (the `partial_sum`
pattern above) — this matters only if you're working with raw RDDs.

## Worked example: tuning a groupBy for a known cardinality

Task: aggregate `sales` by `store_id` (50 distinct values, ~1M rows), and
choose `shuffle.partitions` sized for the actual data rather than the
200 default.

```python
spark.conf.set("spark.sql.shuffle.partitions", 50)

result = (
    sales
    .repartition(50, "store_id")          # pre-shuffle once, evenly, by key
    .groupBy("store_id")
    .agg(spark_sum("amount").alias("total_amount"))
)
result.explain()
# Exchange hashpartitioning(store_id#.., 50) appears exactly once —
# the groupBy reuses the existing store_id partitioning, no second shuffle.

print(result.rdd.getNumPartitions())  # 50, matched to actual cardinality
```

## Exercise

1. Given a dataset with 5 distinct grouping keys and 500 GB of data,
   compute a reasonable `spark.sql.shuffle.partitions` value using the
   ~128 MB-per-partition rule of thumb, and justify the number.
2. Take a pipeline that does `.filter()` after a `.join()` and rewrite it
   to filter both sides *before* the join — explain why this reduces
   shuffle volume even though the final row count is identical.
3. Explain, in your own words, why `coalesce(1)` right before a
   `.write()` on a large DataFrame is usually a performance trap, and
   what symptom you'd see in the Spark UI if you did it anyway.
