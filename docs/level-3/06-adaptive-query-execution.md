# 06 · Adaptive Query Execution

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Adaptive Query Execution (AQE), enabled by default since Spark 3.2,
re-optimizes a query plan *during* execution using actual runtime
statistics — not just the estimates the optimizer had before running
anything. This module covers the three AQE features you're most likely
to rely on: partition coalescing, skew join splitting, and join strategy
switching.

## Turning it on (and confirming it's on)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder.appName("aqe-demo").getOrCreate()

spark.conf.set("spark.sql.adaptive.enabled", True)                          # default True in 3.2+
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)       # default True
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)                 # default True

print(spark.conf.get("spark.sql.adaptive.enabled"))   # 'true'
```

## Why "adaptive" — the problem with static plans

Without AQE, Spark picks join strategies and shuffle partition counts
*before* running anything, based on estimated statistics (file sizes,
`ANALYZE TABLE` stats if present). Those estimates can be badly wrong
after several transformations — a `.filter()` that removes 99% of rows
isn't reflected in the pre-execution size estimate, so the optimizer might
pick a shuffle join when a broadcast join would now be cheap.

```python
customers = spark.createDataFrame(
    [(i, f"name_{i}", "US" if i % 2 == 0 else "IN") for i in range(1, 200_000)],
    ["customer_id", "name", "country"],
)
orders = spark.range(0, 5_000_000).withColumnRenamed("id", "order_id") \
    .withColumn("customer_id", (col("order_id") % 200_000) + 1)

# Static plan (pre-AQE) sees customers as 200k rows — too big to
# auto-broadcast under the default 10MB threshold. But after this filter,
# far fewer rows remain, and the runtime size could now fit comfortably.
filtered_customers = customers.filter(col("country") == "US")

result = orders.join(filtered_customers, "customer_id")
```

## Coalescing shuffle partitions at runtime

Instead of guessing `spark.sql.shuffle.partitions` up front, AQE can
start with a generous partition count and *merge* small adjacent
post-shuffle partitions automatically once it sees their actual sizes:

```python
spark.conf.set("spark.sql.shuffle.partitions", 200)   # deliberately generous starting point
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")

agg = orders.groupBy("customer_id").agg(spark_sum("order_id"))
agg.explain()

# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=true
# +- == Final Plan ==
#    *(2) HashAggregate(keys=[customer_id#..], functions=[sum(order_id#..)])
#    +- CustomShuffleReader coalesced
#       +- ShuffleQueryStage ...
#       +- *(1) HashAggregate(keys=[customer_id#..], functions=[partial_sum(order_id#..)])
```

`CustomShuffleReader coalesced` is the tell: AQE merged what would have
been up to 200 small post-shuffle partitions down to however many are
actually needed to hit the ~64 MB advisory size, without you having to
guess the right `shuffle.partitions` value ahead of time.

## Dynamically switching join strategy

When a filtered or aggregated intermediate result turns out to be small
enough at runtime, AQE can convert a planned sort-merge join into a
broadcast join *after* seeing the actual size — something a static plan
can never do:

```python
spark.conf.set("spark.sql.adaptive.enabled", True)
result.explain()

# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=true
# +- == Final Plan ==
#    *(2) BroadcastHashJoin [customer_id#..], [customer_id#..], Inner, BuildRight
#    +- ...
#    +- BroadcastQueryStage
#       +- BroadcastExchange HashedRelationBroadcastMode(...)
```

Note this only shows up in the **Final Plan** section — if you call
`.explain()` before execution starts, you'd see the *initial* plan
(likely `SortMergeJoin`), because AQE re-plans stage by stage as actual
data materializes, not before the query starts running at all.

## Skew join splitting

Covered briefly in module 3 — AQE detects a post-shuffle partition
significantly larger than the median (governed by
`spark.sql.adaptive.skewJoin.skewedPartitionFactor`, default `5`, and a
minimum size floor `skewedPartitionThresholdInBytes`) and splits it into
several smaller sub-partitions handled by separate tasks:

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
```

With this on, a `SortMergeJoin` physical plan may show
`CustomShuffleReader skewedPartitionsHandled` — AQE split the hot
partition on one or both sides into pieces small enough to process in
parallel across multiple tasks instead of one straggler task.

## Reading `AdaptiveSparkPlan` in `.explain()`

```python
result.explain(mode="formatted")
# ...
# (1) AdaptiveSparkPlan
# Output: [customer_id, order_id, name, country]
# Arguments: isFinalPlan=true
```

`isFinalPlan=false` means you're looking at the plan *before* AQE has
finished re-optimizing — call `.explain()` only after triggering an
action (or on the completed query in the Spark UI's SQL tab) to see the
plan AQE actually executed.

## When AQE does *not* help

AQE only re-optimizes at shuffle (stage) boundaries — it cannot fix a
skewed `groupBy` key without a shuffle boundary present, and it cannot
help if statistics collection itself is disabled
(`spark.sql.cbo.enabled` / cost-based optimizer stats being stale doesn't
block AQE, but very small datasets where every plan is already cheap
won't show a measurable difference). It's also not a substitute for
fixing genuinely bad partitioning strategy or an under-provisioned
cluster — it optimizes within the plan space, not around real resource
constraints.

## Exercise

1. Given `spark.sql.adaptive.advisoryPartitionSizeInBytes=128m` and a
   shuffle stage producing 40 GB of data across 200 initial partitions,
   estimate roughly how many partitions AQE's coalescing would target.
2. Explain, step by step, why calling `.explain()` *before* running
   `.collect()` on an AQE-eligible join can show a `SortMergeJoin` even
   though the job actually executes a `BroadcastHashJoin`.
3. `spark.sql.adaptive.skewJoin.skewedPartitionFactor` is set to `5`. If
   the median partition size is 200 MB, at what partition size does AQE
   consider a partition skewed (assuming it also clears the minimum size
   threshold)?
