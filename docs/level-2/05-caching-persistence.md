# 05 · Caching Persistence

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Spark DataFrames are lazily evaluated and, by default, recomputed from
scratch every time an action touches them. If you reuse a DataFrame
multiple times, caching avoids repeating expensive upstream work — but
misused, it can also silently waste memory or produce stale results. This
module covers when and how to cache correctly.

```python
data = [(i, i % 5, float(i) * 2.5) for i in range(1, 1001)]
df = spark.createDataFrame(data, ["id", "group_id", "value"])
```

## Why recomputation happens by default

```python
from pyspark.sql.functions import sqrt, log

expensive = df.withColumn("derived", sqrt(df.value) + log(df.value + 1))

expensive.filter("group_id = 0").count()   # recomputes `derived` for all rows
expensive.filter("group_id = 1").count()   # recomputes AGAIN from scratch
```

Each `.count()` is a separate action, and Spark's lazy execution model
re-runs the entire lineage — including the `sqrt`/`log` computation —
every single time, because nothing told Spark to keep the intermediate
result around.

## cache(): the default persistence

```python
expensive_cached = expensive.cache()

expensive_cached.count()   # triggers computation AND materializes the cache
expensive_cached.filter("group_id = 0").count()  # reuses cached data, no recompute
expensive_cached.filter("group_id = 1").count()  # reuses cached data again
```

`.cache()` is a lazy marker — it does nothing by itself until the first
action runs. That first action computes the DataFrame *and* stores the
result (by default, deserialized in executor memory, spilling to disk if
it doesn't fit). Every subsequent action against `expensive_cached` reads
from the cached data instead of recomputing.

```python
expensive_cached.is_cached
# True
```

## persist(): explicit storage levels

`cache()` is shorthand for `persist(StorageLevel.MEMORY_AND_DISK)` (in
recent PySpark versions). `persist()` lets you choose the tradeoff
explicitly:

```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)          # fastest, lost if it doesn't fit
df.persist(StorageLevel.MEMORY_AND_DISK)      # default cache() behavior; spills to disk
df.persist(StorageLevel.MEMORY_ONLY_SER)      # serialized, less memory, more CPU to deserialize
df.persist(StorageLevel.DISK_ONLY)            # no memory pressure, slower reads
df.persist(StorageLevel.MEMORY_AND_DISK_2)    # replicated to 2 nodes, resilient to executor loss
```

| Level | Memory | Disk | Serialized | Replicated |
|---|---|---|---|---|
| `MEMORY_ONLY` | yes | no | no | no |
| `MEMORY_AND_DISK` | yes | spills | no | no |
| `MEMORY_ONLY_SER` | yes | no | yes | no |
| `DISK_ONLY` | no | yes | yes | no |
| `MEMORY_AND_DISK_2` | yes | spills | no | 2x |

`MEMORY_ONLY` risks silently dropping partitions (and recomputing them on
demand) under memory pressure; `MEMORY_AND_DISK` is the safer general
default, trading some speed for guaranteed availability of the cached
data.

## unpersist(): freeing the cache

```python
expensive_cached.unpersist()
```

Cached data occupies executor memory/disk indefinitely until you call
`unpersist()` (or the Spark application ends). In a long-running job that
caches many intermediate DataFrames in a loop, forgetting to unpersist is
a common cause of executor memory pressure and eventual `OutOfMemoryError`
or excessive disk spill — always unpersist a cached DataFrame once you're
done reusing it.

```python
# unpersist(blocking=True) waits for the cache to actually be cleared
# before returning, useful right before something else needs that memory:
expensive_cached.unpersist(blocking=True)
```

## When caching helps — and when it doesn't

Caching pays off when:

- The same DataFrame is used as input to **multiple actions** (multiple
  `.show()`/`.count()`/`.write()` calls, or multiple downstream
  branches).
- The upstream computation (reads, joins, UDFs, aggregations) is genuinely
  expensive to repeat.

Caching does **not** help when:

- The DataFrame is used exactly once — you pay the cache-materialization
  cost for zero reuse benefit.
- The dataset is far larger than available cluster memory — most
  partitions spill to disk immediately, and disk-spilled cache is barely
  faster than just recomputing.

```python
# Anti-pattern: caching something used only once
df.filter("group_id = 2").cache().count()   # wasted — no reuse follows
```

## Caching and the DAG: verifying with explain

```python
expensive.cache()
expensive.filter("group_id = 0").explain()
# == Physical Plan ==
# *(1) Filter (group_id#3 = 0)
# +- InMemoryTableScan [id#1, group_id#3, value#5, derived#9], [(group_id#3 = 0)]
#       +- InMemoryRelation [...], StorageLevel(disk, memory, deserialized, 1 replicas)
#          +- *(1) Project [...]
#             +- *(1) Scan ExistingRDD[...]
```

`InMemoryTableScan` / `InMemoryRelation` in the plan confirms the cache is
actually being used for this query, rather than recomputing from the
underlying scan — a good habit before trusting that a `.cache()` call is
paying off.

## Caching and correctness: stale cache pitfall

```python
df.cache()
df.count()          # materializes cache

df2 = df.withColumn("value", df.value * 2)  # new DataFrame, doesn't mutate df's cache
df2.count()          # recomputes from df's CACHED data, then applies *2 — correct

# But if the UNDERLYING SOURCE changes after caching (e.g. new files land
# in the source path) and you re-read + cache again under the same
# variable name, always drop the old cache first:
df.unpersist()
df = spark.read.parquet("/tmp/updated_source").cache()
```

Cached data is a snapshot as of when it was materialized — mutating the
DataFrame lineage afterward (via `withColumn`, `filter`, etc.) is safe
because those return *new* DataFrame objects, but re-reading the same
source path and expecting the cache to reflect new data is not: always
`unpersist()` before caching a fresh read under a reused reference.

## Worked example: multi-branch report from one cached base

Task: from `df`, compute three different summaries (per-group count,
per-group average, and overall total) that all depend on the same derived
`value_bucket` column — cache once, since it's used three times.

```python
from pyspark.sql.functions import when, count, avg, sum as spark_sum

base = df.withColumn(
    "value_bucket",
    when(df.value < 1000, "low").otherwise("high")
).cache()

base.count()  # materialize the cache with one action first

by_group = base.groupBy("group_id").agg(count("*").alias("n"))
by_bucket = base.groupBy("value_bucket").agg(avg("value").alias("avg_value"))
overall = base.agg(spark_sum("value").alias("total_value"))

by_group.show()
by_bucket.show()
overall.show()

base.unpersist()   # done reusing it — free the memory
```

Each of the three downstream `.show()` calls reuses `base`'s cached,
already-`withColumn`-derived data rather than recomputing the `when(...)`
expression three separate times.

## Exercise

Using `df` from the top of this module:

1. Build a derived DataFrame with an expensive-looking column (e.g.
   `sqrt(value) * log(value + 1)`), cache it, materialize with `.count()`,
   then run two different `.filter().count()` calls and confirm (via
   `.explain()`) that `InMemoryTableScan` appears in both plans.
2. Explain in a comment why caching a DataFrame used exactly once provides
   no benefit and costs materialization time.
3. Persist the same base DataFrame with `StorageLevel.DISK_ONLY` instead
   of the default, and describe (in a comment) the tradeoff versus
   `MEMORY_AND_DISK`.
4. After finishing all queries against a cached DataFrame, call
   `unpersist(blocking=True)` and explain why `blocking=True` matters if
   another expensive job is about to start immediately afterward.
