# 05 · Performance Tuning

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

This module pulls together the tuning knobs that don't fit neatly under
"shuffle" or "skew" alone: memory configuration, executor sizing, file
layout, and the small-file problem. Treat it as a checklist you run
through before declaring a job "as fast as it can be."

## Executor sizing: cores, memory, and count

```python
# Typical spark-submit / cluster config, shown here as conf for reference:
spark_conf = {
    "spark.executor.cores": "4",       # parallel tasks per executor
    "spark.executor.memory": "8g",     # JVM heap per executor
    "spark.executor.memoryOverhead": "1g",  # off-heap: shuffle buffers, Python worker memory
    "spark.executor.instances": "10",  # number of executors
}
```

Rules of thumb:

- **5 cores per executor** is a commonly cited sweet spot — beyond that,
  HDFS/network I/O throughput per executor tends to degrade from
  contention, and JVM garbage collection pauses get worse with larger
  heaps holding more concurrent tasks' data.
- **Total parallelism** = `executor.instances × executor.cores`. This
  should comfortably exceed your `shuffle.partitions` count, or many
  shuffle tasks queue up behind a small number of executor slots.
- Leave **1 core and ~1 GB** per node for the OS/YARN NodeManager/cluster
  manager daemon — don't size executors to consume 100% of a node.

```python
# If a node has 16 cores / 64 GB and you want 5 cores/executor:
cores_per_executor = 5
executors_per_node = 16 // cores_per_executor    # 3, leaving 1 core for the OS
mem_per_executor_gb = 64 // executors_per_node    # ~21 GB, split into heap + overhead
print(executors_per_node, mem_per_executor_gb)
```

## `spark.executor.memoryOverhead` and PySpark specifically

PySpark UDFs and pandas UDFs run Python worker processes *outside* the
JVM heap — their memory comes out of `memoryOverhead`, not
`executor.memory`. A job that's fine in Scala but OOMs in PySpark with
heavy UDF usage is almost always an under-sized overhead setting:

```python
spark_conf_pyspark_heavy = {
    "spark.executor.memory": "6g",
    "spark.executor.memoryOverhead": "3g",   # bumped up for Python worker headroom
    "spark.executor.pyspark.memory": "2g",   # explicit cap on Python worker memory (optional)
}
```

## Caching: when it helps, when it hurts

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("perf-tuning").getOrCreate()

base = spark.range(0, 5_000_000).withColumn("bucket", col("id") % 100)

# Worth caching: base is read from disk/computed once, then reused
# across 3 independent downstream actions.
base.cache()
base.count()   # materialize

a = base.filter(col("bucket") < 10).count()
b = base.filter(col("bucket") >= 90).count()
c = base.groupBy("bucket").count().count()

base.unpersist()
```

Caching is wasted (and costs memory pressure / eviction risk) if the
cached DataFrame is only ever used once — you've paid the cost of
materializing and storing it for no reuse benefit. Always check `.count()`
of distinct action call-sites against the cached DataFrame before adding
`.cache()`.

## Choosing a storage level

```python
from pyspark import StorageLevel

# Default for .cache(): deserialized objects in memory, spill to disk if it doesn't fit.
base.persist(StorageLevel.MEMORY_AND_DISK)

# Serialized: more CPU to (de)serialize, but roughly 2-4x less memory footprint —
# useful when a DataFrame barely fits and GC pressure is hurting more than CPU cost.
base.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Memory only, no disk spill: fastest but rows are simply dropped and
# recomputed from lineage if they don't fit — risky for very large data.
base.persist(StorageLevel.MEMORY_ONLY)
```

## The small-file problem

Too many tiny output files (from over-partitioned writes) slow down every
downstream job that has to open each file's metadata separately —
especially painful on cloud object stores where each file open is a
network round trip.

```python
# Before: 500 shuffle partitions -> up to 500 small files per write
spark.conf.set("spark.sql.shuffle.partitions", 500)
skewed_write = base.groupBy("bucket").count()
skewed_write.write.mode("overwrite").parquet("/tmp/too_many_files")

# After: coalesce right before the write to consolidate into fewer,
# larger files, without triggering a second full shuffle.
skewed_write.coalesce(10).write.mode("overwrite").parquet("/tmp/fewer_files")
```

Target file sizes of roughly 128 MB–1 GB for Parquet on most
object stores; use `.repartition(n)` instead of `.coalesce(n)` if you also
need to fix a skewed key distribution across the output files (coalesce
alone can't rebalance, only merge adjacent partitions).

## File format and compression choices

```python
# Parquet + Snappy (default) is the standard choice: columnar, splittable,
# supports predicate/column pruning, low CPU cost to decompress.
base.write.option("compression", "snappy").parquet("/tmp/snappy_out")

# gzip: smaller files, but NOT splittable within a single file and
# noticeably more CPU to decompress — usually a poor choice for Spark inputs.
base.write.option("compression", "gzip").parquet("/tmp/gzip_out")

# zstd: often a good middle ground — better compression ratio than snappy
# at comparable CPU cost, splittable within Parquet's own row-group structure.
base.write.option("compression", "zstd").parquet("/tmp/zstd_out")
```

## Broadcast threshold and join strategy hints

```python
# Covered in Level 2 module 1 and Level 3 module 7 in depth — repeated here
# as a tuning checklist item:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50 MB
```

## Worked example: a tuning pass on a slow aggregation job

Starting point — default config, job takes 40 minutes on a dataset with
known skew and 800 shuffle partitions worth of tiny files on output:

```python
# 1. Right-size shuffle partitions to actual data volume (module 2)
spark.conf.set("spark.sql.shuffle.partitions", 96)

# 2. Turn on AQE so Spark can further split any skewed partitions at runtime (module 6)
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)

# 3. Bump overhead memory since the pipeline uses a pandas_udf
spark_conf_note = {"spark.executor.memoryOverhead": "2g"}

# 4. Coalesce the final write to avoid 800 small files
result = base.groupBy("bucket").count()
result.coalesce(20).write.mode("overwrite").parquet("/data/warehouse/bucket_counts")
```

Each change targets a distinct bottleneck category (parallelism sizing,
runtime skew, Python memory, file layout) — apply them incrementally and
re-check the Spark UI (module 8) between changes rather than all at once,
so you know which change actually moved the needle.

## Exercise

1. Given a 16-core, 64 GB node and a target of 5 cores per executor,
   compute executor count per node and a reasonable
   `executor.memory` + `executor.memoryOverhead` split.
2. Explain why `MEMORY_AND_DISK_SER` might outperform `MEMORY_ONLY` on a
   cluster experiencing frequent GC pauses, even though it does more CPU
   work per access.
3. A job outputs 4,000 files averaging 3 MB each into a partitioned
   Parquet table. Propose a specific `.coalesce()` or `.repartition()`
   change (with a target file-size justification) to fix it.
