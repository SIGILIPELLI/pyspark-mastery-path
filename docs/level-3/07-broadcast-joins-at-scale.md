# 07 · Broadcast Joins At Scale

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Level 2 introduced broadcast joins with small, toy tables. This module
looks at the operational realities of broadcasting at production scale:
how big is "too big," what failure modes look like, multi-way broadcast
chains, and broadcast variables outside of joins.

## Recap: why broadcast at all

A broadcast join sends a full copy of the small side to every executor
once, so each executor can do a local hash join with no shuffle of the
large side. The cost is proportional to `(small side size) × (number of
executors)` for the broadcast itself, versus a shuffle join's cost being
proportional to moving *both* full datasets across the network.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

spark = SparkSession.builder.appName("broadcast-at-scale").getOrCreate()

fact = spark.range(0, 2_000_000_000).withColumnRenamed("id", "event_id") \
    .withColumn("dim_key", (col("event_id") % 10_000_000))
dim = spark.range(0, 10_000_000).withColumnRenamed("id", "dim_key") \
    .withColumn("dim_value", col("dim_key") * 2)
```

At 10 million rows, `dim` is roughly 100–200 MB depending on schema width
— broadcastable on a reasonably provisioned cluster but past the default
10 MB auto-threshold, so it needs either an explicit `broadcast()` call or
a raised `autoBroadcastJoinThreshold`.

## How big is "too big" to broadcast?

There's no fixed number — it depends on **executor memory** and
**executor count**, since every executor holds a full copy simultaneously.

```python
# Rough capacity check before broadcasting:
executor_memory_gb = 8
memory_overhead_gb = 2
safety_margin = 0.5   # leave half of overhead free for other broadcast/shuffle buffers

max_safe_broadcast_mb = (memory_overhead_gb * safety_margin) * 1024
print(max_safe_broadcast_mb)   # 1024 MB — a starting ceiling, not a guarantee

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", int(max_safe_broadcast_mb * 1024 * 1024))
```

A broadcast that's too large manifests as executors OOM-ing during the
`BroadcastExchange` stage, or the driver itself running out of memory
collecting the broadcast data before distributing it — the driver builds
the broadcast variable first, so driver memory matters here too.

## `spark.sql.broadcastTimeout`

Broadcasting a genuinely large table can simply take longer than the
default timeout allows, especially over a slow network or with many
executors pulling the same broadcast simultaneously:

```python
spark.conf.get("spark.sql.broadcastTimeout")   # '300' (seconds), default

# If a legitimately large-but-still-worth-broadcasting table times out
# building or distributing, raise it rather than assume broadcast is impossible:
spark.conf.set("spark.sql.broadcastTimeout", 600)
```

A `BroadcastTimeoutException` doesn't necessarily mean "too big to
broadcast" — check whether it's a genuine size problem (lower the
threshold, use a shuffle join instead) or just a slow-network timing
issue (raise the timeout).

## Explicit broadcast vs. broadcast hints in SQL

```python
dim.createOrReplaceTempView("dim")
fact.createOrReplaceTempView("fact")

spark.sql("""
    SELECT /*+ BROADCAST(dim) */ fact.event_id, dim.dim_value
    FROM fact JOIN dim ON fact.dim_key = dim.dim_key
""").explain()
# == Physical Plan ==
# *(2) BroadcastHashJoin [dim_key#..], [dim_key#..], Inner, BuildRight
```

The `/*+ BROADCAST(table_alias) */` hint is the SQL-API equivalent of
wrapping a DataFrame in `broadcast(...)` — useful when the join is
expressed in raw SQL rather than the DataFrame API, and it takes
precedence over the size-based auto-broadcast decision either way.

## Multi-way broadcast chains

Joining a large fact table against several small dimension tables in
sequence is one of the most common production patterns — broadcast each
dimension independently rather than joining dimensions to each other
first:

```python
region = spark.createDataFrame([(1, "east"), (2, "west")], ["region_id", "region_name"])
category = spark.createDataFrame([(1, "electronics"), (2, "home")], ["category_id", "category_name"])

fact_wide = fact.withColumn("region_id", (col("dim_key") % 2) + 1) \
    .withColumn("category_id", (col("dim_key") % 2) + 1)

enriched = (
    fact_wide
    .join(broadcast(region), "region_id")
    .join(broadcast(category), "category_id")
    .join(broadcast(dim), "dim_key")
)
enriched.explain()
# Three separate BroadcastHashJoin nodes chained together, each with its
# own BroadcastExchange — no shuffle at all in this plan.
```

Chaining broadcasts like this keeps the large `fact_wide` table's
partitioning completely untouched through all three joins — a pure
map-side operation with zero shuffles, which is the ideal shape for a
star-schema fact/dimension enrichment pipeline.

## Broadcast variables outside of joins

`sc.broadcast()` is the lower-level primitive behind `broadcast()` on
DataFrames — useful directly when you need to ship a lookup structure
(e.g., a Python dict) into a UDF without re-serializing it per task:

```python
lookup_dict = {1: "east", 2: "west"}
bc = spark.sparkContext.broadcast(lookup_dict)

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def lookup_region(region_id):
    return bc.value.get(region_id, "unknown")

fact_wide.withColumn("region_name", lookup_region(col("region_id"))).select(
    "region_id", "region_name"
).distinct().show()
# +---------+-----------+
# |region_id|region_name|
# +---------+-----------+
# |        1|       east|
# |        2|       west|
# +---------+-----------+
```

`bc.value` is fetched once per executor (not per task, not per row) —
far cheaper than closing over `lookup_dict` directly in the UDF, which
Spark would otherwise have to re-serialize into every task's closure.

```python
bc.unpersist()   # release the broadcast from executor memory when no longer needed
bc.destroy()     # release fully, including from the driver — cannot be reused after this
```

## Exercise

1. Given `executor.memory=16g`, `executor.memoryOverhead=4g`, and 20
   executors, estimate a conservative `autoBroadcastJoinThreshold` value
   and justify the number.
2. Rewrite the three-way broadcast chain above using the SQL API with
   `/*+ BROADCAST(...) */` hints on all three joins, and confirm in
   words that the resulting physical plan should be identical to the
   DataFrame-API version.
3. A broadcast join intermittently fails with
   `BroadcastTimeoutException` under network load but succeeds when
   re-run. Name two different remediations and explain the tradeoff
   between them.
