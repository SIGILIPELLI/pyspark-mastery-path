# 04 · Partitioning Strategy

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Partitioning determines how a DataFrame's rows are split across executors
in memory (in-memory partitions) and how output files are split on disk
(partition columns / files). Getting this wrong is the #1 cause of slow or
failing Spark jobs at scale — this module covers both meanings and how to
control each.

```python
data = [(i, i % 4, f"user_{i}", float(i) * 1.5) for i in range(1, 21)]
df = spark.createDataFrame(data, ["id", "region_id", "user", "amount"])
```

## In-memory partitions: how many, and why it matters

```python
df.rdd.getNumPartitions()
# depends on spark.default.parallelism / how the DataFrame was created;
# commonly matches the number of cores available locally, e.g. 8
```

Each in-memory partition is processed by one task on one executor core.
Too few partitions means you're not using all your cores (bad
parallelism); too many means excessive scheduling overhead and small,
inefficient tasks. As a rule of thumb, aim for 2-4x as many partitions as
you have executor cores for CPU-bound work, and larger partitions (closer
to 128MB-1GB each) for I/O-bound file writes.

## repartition(): full shuffle, even distribution

```python
df2 = df.repartition(4)
df2.rdd.getNumPartitions()
# 4
```

`repartition(n)` triggers a full shuffle to redistribute rows as evenly as
possible across `n` partitions (roughly round-robin, unless you also pass
column(s)). Use it when you need to *increase* partition count, or need
partitions genuinely balanced regardless of key distribution.

```python
# Repartition BY a column: rows with the same key land in the same partition
df3 = df.repartition(4, "region_id")
```

This is the version to use before a `groupBy`/join on `region_id` when you
know you'll do several operations keyed on it — paying the shuffle cost
once up front, rather than implicitly on every downstream operation.

## coalesce(): cheap partition reduction, no shuffle

```python
df4 = df.repartition(8).coalesce(2)
df4.rdd.getNumPartitions()
# 2
```

`coalesce(n)` (for `n` less than the current partition count) merges
existing partitions together *without* a full shuffle — it just combines
partitions locally, which is much cheaper than `repartition`. Use it when
you only need *fewer* partitions (e.g. before writing output, to avoid
producing hundreds of tiny files) and don't need perfectly even
distribution. `coalesce` cannot increase partition count — calling
`coalesce(20)` on a 4-partition DataFrame silently does nothing (stays
at 4), since it can only merge, not split.

## repartition vs. coalesce: when to use which

| | shuffle? | can increase count? | balance |
|---|---|---|---|
| `repartition(n)` | yes, full shuffle | yes | even (or by key) |
| `coalesce(n)` | no (local merge) | no, only decreases | may be uneven |

The classic pattern: process data with a partition count suited to
parallel compute (`repartition` up), then `coalesce` down right before
writing output, to avoid the "small files problem" (thousands of tiny
output files that are expensive for downstream readers to open).

```python
(df.repartition(50, "region_id")
   .groupBy("region_id").count()
   .coalesce(1)
   .write.mode("overwrite").csv("/tmp/region_counts"))
```

## Partition pruning: filtering on disk-partition columns

When data is written partitioned by a column (see Level 1's write module),
Spark can skip entire directories at read time if your filter matches the
partition column — this is **partition pruning**, and it's one of the
biggest wins available for large datasets.

```python
df.write.mode("overwrite").partitionBy("region_id").parquet("/tmp/sales_by_region")

# Reading back with a filter on the partition column:
filtered = spark.read.parquet("/tmp/sales_by_region").filter("region_id = 2")
filtered.explain()

# == Physical Plan ==
# *(1) Scan parquet [id,user,amount,region_id] ...
# PartitionFilters: [isnotnull(region_id#5), (region_id#5 = 2)]
# PushedFilters: []
```

`PartitionFilters` shown in the plan confirms Spark skipped reading the
`region_id=0`, `region_id=1`, `region_id=3` directories entirely — no I/O
was spent on rows that couldn't match. Filtering on a *non*-partition
column instead shows up under `PushedFilters` (a Parquet-level predicate
pushdown, still helpful, but requiring at least a file-level read).

## Choosing partition columns wisely

Bad choices for `partitionBy`:

- **High-cardinality columns** (e.g. `user_id` with millions of values) —
  produces millions of tiny directories, crushing the filesystem/metastore
  and the small-files problem.
- **Columns rarely used in filters** — you pay the write-time cost of
  splitting into directories but gain no read-time pruning benefit.

Good choices: low-to-medium cardinality columns that queries commonly
filter on — `region_id`, `event_date`, `country` — where each partition
value has enough rows to produce reasonably-sized files.

## Skew preview: uneven partitions

```python
skewed = spark.createDataFrame(
    [(1, "A")] * 1000 + [(2, "B")] * 10, ["id", "region"]
)
skewed.groupBy("region").count().show()
# +------+-----+
# |region|count|
# +------+-----+
# |     A| 1000|
# |     B|   10|
# +------+-----+
```

If `region_id` were used as a shuffle/repartition key here, one task would
process 1000 rows while another processes 10 — the job's wall-clock time
is bounded by the slowest (largest) task. This is **data skew**, covered
in depth (with salting and AQE-based fixes) in Level 3.

## Worked example: right-sizing a pipeline

Task: `df` (20 rows, illustrative — imagine millions in production) needs
to be grouped by `region_id`, aggregated, and written out without
producing tiny files, while also being pruned efficiently for future
reads filtered on `region_id`.

```python
from pyspark.sql.functions import sum as spark_sum, count

result = (
    df.repartition(4, "region_id")           # even shuffle, keyed for the groupBy
      .groupBy("region_id")
      .agg(count("*").alias("n"), spark_sum("amount").alias("total"))
)

(result.coalesce(1)                           # small result set -> 1 output file
       .write.mode("overwrite")
       .partitionBy("region_id")              # enables pruning on future reads
       .parquet("/tmp/region_summary"))
```

Note `coalesce(1)` here only makes sense because `result` is a small,
already-aggregated DataFrame (one row per region) — never `coalesce(1)` a
large, unaggregated DataFrame before writing, or you'll force everything
through a single task and lose all parallelism on the write.

## Exercise

Using `df` from the top of this module:

1. Check `df.rdd.getNumPartitions()`, then `repartition(6)` and confirm
   the new count.
2. Repartition `df` by `region_id` into 4 partitions, then use
   `spark_partition_id()` (from `pyspark.sql.functions`) grouped by
   `region_id` to confirm all rows sharing a `region_id` land in the same
   partition.
3. Write `df` partitioned by `region_id` to a local path, then read it
   back with a filter on `region_id` and confirm `PartitionFilters`
   appears (not just `PushedFilters`) in `.explain()`.
4. Explain in a comment why `coalesce(1)` before a large unaggregated
   write is usually a mistake, referencing task parallelism.
