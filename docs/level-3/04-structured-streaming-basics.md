# 04 · Structured Streaming Basics

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Structured Streaming lets you write the same DataFrame code you already
know and run it incrementally against an unbounded input — files landing
in a directory, a Kafka topic, a socket. Spark treats the stream as a
table that keeps growing, and re-runs your query logic against just the
new rows on each micro-batch (or, with newer versions, continuously).

## The core mental model: an unbounded table

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import window, col, count as spark_count, avg

spark = SparkSession.builder.appName("streaming-basics").getOrCreate()

schema = StructType([
    StructField("event_time", TimestampType()),
    StructField("device_id", StringType()),
    StructField("temperature", DoubleType()),
])
```

Every streaming source declaration looks like a batch `read`, but
`readStream` instead of `read`:

```python
raw_stream = (
    spark.readStream
    .format("json")
    .schema(schema)          # streaming sources require an explicit schema —
    .option("maxFilesPerTrigger", 1)   # no schema inference against a moving target
    .load("/data/incoming_sensor_events/")
)

print(raw_stream.isStreaming)   # True
```

`raw_stream` is a DataFrame like any other — you can `.select()`,
`.filter()`, `.groupBy()` it with the exact same API from Levels 1–2. The
difference only shows up when you try to *run* it.

## Batch actions don't work on streams

```python
# raw_stream.show()      # AnalysisException: Queries with streaming sources
#                         # must be executed with writeStream.start()
```

Instead of `.show()`/`.collect()`, a streaming DataFrame is launched with
`.writeStream`, which returns a `StreamingQuery` handle you manage
independently of the rest of your script:

```python
query = (
    raw_stream
    .filter(col("temperature") > 30.0)
    .writeStream
    .format("console")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

# query.awaitTermination()   # blocks; in a notebook you'd instead call query.stop() later
```

## Output modes

Three output modes govern what gets (re-)written on each trigger:

- **append** — only new rows since the last trigger are output. Works for
  any query without aggregation; for aggregations, only after watermark
  expiry closes out a window (below).
- **complete** — the entire updated result table is rewritten every
  trigger. Only feasible for aggregations, and only when the result set
  is small enough to hold entirely (e.g., a running total by category).
- **update** — only rows that changed since the last trigger are output;
  works for aggregations without requiring watermarking.

```python
counts = raw_stream.groupBy("device_id").agg(spark_count("*").alias("event_count"))

query = (
    counts.writeStream
    .format("console")
    .outputMode("complete")   # required here — no watermark, unbounded aggregation
    .start()
)
```

## Windowed aggregation with a watermark

Event-time windowing groups rows into time buckets based on
`event_time`, not wall-clock arrival time. A **watermark** tells Spark how
long to wait for late-arriving data before finalizing (and evicting from
state) a given window.

```python
windowed_avg = (
    raw_stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("device_id"),
    )
    .agg(avg("temperature").alias("avg_temp"))
)

query = (
    windowed_avg.writeStream
    .format("console")
    .outputMode("append")     # append is valid here because the watermark
    .start()                  # guarantees a window won't get late data forever
)
```

With `withWatermark("event_time", "10 minutes")`, a 5-minute window
covering `12:00–12:05` is finalized and emitted once Spark has seen an
event with `event_time` past `12:15` (`window end` + watermark delay) —
any row arriving even later than that for that window is silently
dropped, trading completeness for bounded state size.

## Checkpointing: required for fault tolerance

Every production streaming query needs a checkpoint location so Spark can
resume from exactly where it left off after a restart, without
duplicating or dropping data (fault-tolerance semantics covered in depth
in module 9):

```python
query = (
    windowed_avg.writeStream
    .format("parquet")
    .option("path", "/data/warehouse/device_temp_windows")
    .option("checkpointLocation", "/data/checkpoints/device_temp_windows")
    .outputMode("append")
    .trigger(processingTime="1 minute")
    .start()
)
```

The checkpoint directory stores offsets (how far into the source you've
read) and, for stateful aggregations, the aggregation state itself. Never
point two different queries at the same checkpoint directory, and never
delete it unless you intend to reprocess the source from scratch.

## Managing the running query

```python
print(query.id)                 # a UUID stable across restarts from the same checkpoint
print(query.status)             # {"message": "Processing new data", "isTriggerActive": true, ...}
print(query.lastProgress)       # dict with batch duration, input/processing rate, etc.

query.stop()                    # graceful stop; safe to call any time
```

`query.lastProgress["inputRowsPerSecond"]` vs
`query.lastProgress["processedRowsPerSecond"]` is the first thing to check
if a stream is falling behind: if input rate consistently exceeds
processed rate, the query can't keep up and backlog will grow unbounded.

## Worked example: a running alert stream

Task: read sensor JSON files as they land, compute a 5-minute tumbling
average temperature per device, and flag any window averaging above 35°C
as `is_alert`.

```python
alerts = (
    raw_stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(window(col("event_time"), "5 minutes"), col("device_id"))
    .agg(avg("temperature").alias("avg_temp"))
    .withColumn("is_alert", col("avg_temp") > 35.0)
)

query = (
    alerts.writeStream
    .format("parquet")
    .option("path", "/data/warehouse/temp_alerts")
    .option("checkpointLocation", "/data/checkpoints/temp_alerts")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)
```

Downstream, any batch job or BI tool can simply read
`/data/warehouse/temp_alerts` as a normal Parquet table and filter
`WHERE is_alert` — the streaming complexity is fully contained upstream.

## Exercise

1. Change the `outputMode` on `windowed_avg` from `append` to `update`
   and explain what difference in emitted rows you'd expect to observe
   for a window that receives a late-but-within-watermark update.
2. Given a watermark of `"10 minutes"` and 5-minute tumbling windows,
   compute the exact wall-clock cutoff at which the window
   `[12:00, 12:05)` is finalized and its state evicted.
3. Explain why streaming sources require an explicit `schema(...)` call
   while batch `spark.read.json(...)` can infer one, in terms of what
   "infer" would require Spark to do against an unbounded input.
