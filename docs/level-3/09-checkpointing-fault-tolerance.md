# 09 · Checkpointing Fault Tolerance

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Spark's fault tolerance is built on **lineage**: every RDD/DataFrame knows
exactly how to recompute itself from its inputs, so losing an executor
just means re-running the lost partitions' transformations. Checkpointing
exists for the cases where lineage-based recovery is too expensive, too
long, or (for streaming) impossible without persisted state. This module
covers both batch and streaming checkpointing, and how they differ.

## Lineage recovery: the default mechanism

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("checkpointing").getOrCreate()

df = spark.range(0, 10_000_000)
step1 = df.withColumn("x", col("id") * 2)
step2 = step1.filter(col("x") % 3 == 0)
step3 = step2.withColumn("y", col("x") + 1)
```

If an executor dies while computing `step3`, Spark doesn't fail the job —
it re-schedules only the lost partitions, re-deriving them by replaying
`step1 → step2 → step3` from the original `df.range()` source, which is
itself cheaply re-creatable. This works well when the lineage chain is
short and the source is cheap to re-read. It works poorly when the chain
is very long (say, 50 chained transformations, several involving
shuffles) — recomputing from scratch after a late-stage failure can cost
more than the checkpoint would have.

## Batch checkpointing: truncating lineage

```python
spark.sparkContext.setCheckpointDir("/data/checkpoints/batch")

long_chain = df
for i in range(50):
    long_chain = long_chain.withColumn(f"step_{i}", col("id") + i)

# Without checkpointing, a failure late in a 50-step chain re-executes
# all 50 steps for the lost partitions. checkpoint() writes the current
# state to reliable storage and truncates lineage back to this point.
long_chain = long_chain.checkpoint()

# Any further failure only needs to re-read from the checkpoint files,
# not replay the 50 preceding transformations.
result = long_chain.filter(col("id") % 1000 == 0).count()
```

`.checkpoint()` is an **eager** action-like call by default — it triggers
computation immediately and writes to disk right there, unlike the lazy
transformations you're used to. There's also a `.localCheckpoint()`
variant that writes to executor local disk instead of a reliable
distributed filesystem — faster, but not fault-tolerant to losing the
node that holds it, so it truncates lineage without truly guaranteeing
recovery across node failure.

```python
# localCheckpoint(): faster, uses executor storage, does NOT survive a
# lost executor — use only to shorten lineage for lineage/query-planner
# performance, never as a durability guarantee.
long_chain.localCheckpoint()
```

## Why checkpointing differs from caching

```python
long_chain.cache()          # keeps data + FULL lineage; recompute-from-source on eviction
long_chain.checkpoint()     # writes data + TRUNCATES lineage entirely
```

A cached DataFrame that gets evicted from memory falls back to
recomputing from its original lineage — the lineage graph is preserved
regardless of caching. A checkpointed DataFrame discards the old lineage
graph once the checkpoint write succeeds: recovery after that point reads
the checkpoint files directly, never replays the pre-checkpoint
transformations again, even if you never explicitly cache it.

## Streaming checkpointing: offsets + state

Streaming checkpointing (introduced operationally in module 4) tracks two
distinct things: **source offsets** (how far into Kafka/files you've
consumed) and, for stateful operations, the **aggregation state** itself
(e.g., partial sums for open windows).

```python
from pyspark.sql.functions import window, col, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("event_time", TimestampType()),
    StructField("device_id", StringType()),
    StructField("temperature", DoubleType()),
])

stream = spark.readStream.format("json").schema(schema).load("/data/incoming/")

windowed = (
    stream.withWatermark("event_time", "10 minutes")
    .groupBy(window(col("event_time"), "5 minutes"), col("device_id"))
    .agg(avg("temperature").alias("avg_temp"))
)

query = (
    windowed.writeStream
    .format("parquet")
    .option("path", "/data/warehouse/device_avg")
    .option("checkpointLocation", "/data/checkpoints/device_avg")
    .outputMode("append")
    .start()
)
```

If the driver crashes and you restart this exact query pointed at the
same `checkpointLocation`, Spark resumes from the last committed offset —
no reprocessing of already-committed micro-batches, and no data loss for
anything acknowledged by the source since the last checkpoint commit.
This is what gives Structured Streaming its **exactly-once** processing
guarantee for the aggregation logic itself, provided the sink is also
idempotent or transactional (a Parquet directory sink is "at least once"
in the presence of certain failure timing unless paired with something
like Delta Lake, covered in Level 4).

## Recovering after a schema or code change

```python
# query.stop()
# Restarting with the SAME checkpointLocation but a CHANGED aggregation
# key or window definition can raise:
#   "Provided stateful operator was created differently: ..."
# because Spark can't reconcile old state format with the new logic.
```

Never change the shape of a stateful streaming query (grouping keys,
window definitions, added/removed stateful operators) while resuming from
an existing checkpoint — either start a fresh checkpoint directory
(accepting reprocessing / potential duplicate output) or make the change
in a way that's backward-compatible with the existing state schema.

## Checkpoint cleanup and lifecycle

```python
# Checkpoint directories are NOT automatically cleaned up when you stop a
# query — old offset/state files accumulate. Delete only when you're
# certain the query will never resume from that checkpoint again:
# dbutils.fs.rm("/data/checkpoints/device_avg", recurse=True)  # example, platform-specific

# Batch RDD checkpoint files similarly persist after the SparkContext
# stops -- clean them up explicitly as part of pipeline housekeeping.
spark.sparkContext.setCheckpointDir("/data/checkpoints/batch")
```

## Worked example: choosing checkpointing strategy for a long iterative job

Task: an iterative algorithm (e.g., PageRank-style) chains 30 rounds of
transformations on the same DataFrame, each round depending on the
previous.

```python
ranks = spark.range(0, 1_000_000).withColumn("rank", col("id").cast("double") * 0 + 1.0)
spark.sparkContext.setCheckpointDir("/data/checkpoints/iterative")

for i in range(30):
    ranks = ranks.withColumn("rank", col("rank") * 0.85 + 0.15)
    if i % 5 == 0:                       # checkpoint every 5 rounds
        ranks = ranks.checkpoint()       # truncate lineage periodically, not every round
        ranks.count()                    # force materialization of the checkpoint

ranks.show(5)
```

Checkpointing every round would add too much I/O overhead for the
recovery benefit; checkpointing too rarely leaves lineage chains long
enough that a late failure is expensive to recover from. Every 5th round
is a reasonable middle ground for a 30-round job — tune the interval
against observed recovery cost vs. checkpoint I/O cost on your own
cluster.

## Exercise

1. Explain, in terms of lineage, why `.checkpoint()` requires an eager
   materialization (it can't stay lazy like `.filter()` or `.select()`).
2. A streaming query's checkpoint directory is accidentally deleted while
   the query is stopped. What are your two options for resuming
   processing, and what does each cost you in terms of data
   duplication/loss?
3. For the 30-round iterative example, explain what would go wrong
   (concretely, in terms of the DAG) if you never checkpointed at all and
   an executor failed on round 29.
