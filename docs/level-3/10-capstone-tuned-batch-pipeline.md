# 10 · Capstone Tuned Batch Pipeline

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

This capstone builds a batch pipeline end-to-end, deliberately introduces
the performance problems from Level 3 (bad plan, shuffle bloat, skew,
small files), diagnoses each with `.explain()` and Spark-UI reasoning, and
fixes them one at a time. Treat this as a template for tuning a real job.

## The scenario

A daily job joins a large `clickstream` fact table (skewed toward one
"power user") against a small `users` dimension table, aggregates
click counts and average session duration per user segment, and writes
the result partitioned for a BI tool to query.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, avg, count as spark_count, broadcast

spark = SparkSession.builder.appName("tuned-batch-capstone").getOrCreate()

n = 2_000_000
clickstream = (
    spark.range(0, n)
    .withColumnRenamed("id", "click_id")
    # 80% of clicks belong to user_id=1 -- deliberate skew
    .withColumn("user_id", when(col("click_id") % 5 < 4, lit(1)).otherwise((col("click_id") % 999) + 2))
    .withColumn("session_seconds", (col("click_id") % 600).cast("double"))
)

users = spark.createDataFrame(
    [(i, f"user_{i}", "premium" if i % 10 == 0 else "standard") for i in range(1, 1001)],
    ["user_id", "username", "segment"],
)
```

## Step 1 — naive first pass, and why it's slow

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)   # simulate the naive/default-off case
spark.conf.set("spark.sql.shuffle.partitions", 200)
spark.conf.set("spark.sql.adaptive.enabled", False)          # start with AQE off to see the raw problem

naive = (
    clickstream.join(users, "user_id")
    .groupBy("segment")
    .agg(spark_count("*").alias("clicks"), avg("session_seconds").alias("avg_session"))
)
naive.explain()
# == Physical Plan ==
# *(5) HashAggregate(keys=[segment#..], functions=[count(1), avg(session_seconds#..)])
# +- Exchange hashpartitioning(segment#.., 200), ...
#    +- *(4) HashAggregate(keys=[segment#..], functions=[partial_count(1), partial_avg(session_seconds#..)])
#       +- *(4) SortMergeJoin [user_id#..], [user_id#..], Inner
#          :- *(2) Sort [user_id#.. ASC NULLS FIRST], false, 0
#          :  +- Exchange hashpartitioning(user_id#.., 200), ...
#          +- *(3) Sort [user_id#.. ASC NULLS FIRST], false, 0
#             +- Exchange hashpartitioning(user_id#.., 200), ...
```

Two full shuffles (`SortMergeJoin` on `user_id`) for a join against a
1,000-row dimension table, and the `user_id=1` key (80% of 2M rows) will
land entirely on one of those 200 partitions — a skew problem baked
straight into the join's shuffle stage.

## Step 2 — fix the join strategy: broadcast the small side

```python
fixed_join = (
    clickstream.join(broadcast(users), "user_id")
    .groupBy("segment")
    .agg(spark_count("*").alias("clicks"), avg("session_seconds").alias("avg_session"))
)
fixed_join.explain()
# *(2) BroadcastHashJoin [user_id#..], [user_id#..], Inner, BuildRight
# ...no shuffle at all for the join itself — only the final groupBy's
# Exchange hashpartitioning(segment#.., 200) remains.
```

This removes both join-side shuffles entirely. But the skew in
`clickstream.user_id` is now irrelevant to the join (no shuffle happens on
it) — good, since broadcasting sidesteps the skewed-join problem
altogether rather than needing salting from module 3.

## Step 3 — right-size the remaining shuffle partitions

Only one `Exchange` remains, for the final `groupBy("segment")` — and
`segment` has only 2 distinct values, so 200 output partitions is wildly
oversized:

```python
spark.conf.set("spark.sql.shuffle.partitions", 4)   # small, matches the 2-value cardinality with headroom

fixed_join.explain()
# Exchange hashpartitioning(segment#.., 4), ... -- far fewer near-empty partitions
```

## Step 4 — turn AQE back on as a safety net

Even with a broadcast join and right-sized partitions, turn AQE on so any
future data-volume growth or a mis-sized manual partition count is caught
automatically:

```python
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
```

## Step 5 — verify with `.explain(mode="formatted")` and row counts

```python
fixed_join.explain(mode="formatted")

sanity = fixed_join.collect()
for row in sanity:
    print(row)
# Row(segment='standard', clicks=1_800_400ish, avg_session=~299.5)
# Row(segment='premium',  clicks=  199_600ish, avg_session=~299.5)
```

(Exact counts depend on how the modulo skew and segment assignment
interact — the point of this step is confirming the row *count* and
aggregate values are sane and stable across repeated runs, not the exact
numbers.)

## Step 6 — write with sane file layout

```python
final = fixed_join.withColumn("run_date", lit("2024-01-15"))

(
    final
    .coalesce(2)                      # match the ~2-segment cardinality; avoid tiny files
    .write
    .mode("overwrite")
    .partitionBy("run_date")
    .parquet("/data/warehouse/segment_click_summary")
)
```

## Step 7 — a before/after tuning summary

```python
tuning_log = [
    {"change": "broadcast(users) instead of default join",
     "effect": "removed 2 full shuffles (SortMergeJoin -> BroadcastHashJoin)"},
    {"change": "shuffle.partitions 200 -> 4",
     "effect": "matched final groupBy's actual 2-value cardinality, removed ~196 empty tasks"},
    {"change": "AQE + skew join handling re-enabled",
     "effect": "safety net for future data growth; no-op today since the join no longer shuffles"},
    {"change": "coalesce(2) before write",
     "effect": "avoided 200 tiny output files from the old shuffle-partition count"},
]
for entry in tuning_log:
    print(f"- {entry['change']}: {entry['effect']}")
```

This is the shape a real tuning pass takes: diagnose with `.explain()` and
UI reasoning (modules 1, 8), fix join strategy first since it's usually
the biggest lever (module 7), right-size remaining shuffles (module 2),
lean on AQE as a runtime backstop (module 6), and finish with file-layout
hygiene on the write (module 5).

## Exercise

1. Re-run Step 1's naive plan but with AQE enabled from the start —
   would AQE's dynamic join-strategy switching alone have fixed the
   `SortMergeJoin` problem without the explicit `broadcast()` call? Trace
   through why or why not given `users`' actual size.
2. Add a `checkpointDir` and insert a `.checkpoint()` call after Step 2's
   join, and justify (or argue against) whether this particular pipeline
   actually benefits from it, given its lineage length.
3. Extend the pipeline with a second dimension table (`device_type`) and
   confirm your final `.explain()` plan shows two independent
   `BroadcastHashJoin` nodes with no shuffle introduced by the second
   join.
