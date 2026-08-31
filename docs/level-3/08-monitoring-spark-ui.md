# 08 · Monitoring Spark UI

!!! note "Not executed against a live cluster in this environment"
    Descriptions below walk through the Spark UI's standard tabs and
    fields as documented, since no live cluster UI is reachable in this
    environment — treat screenshots-in-words as a guide for what to look
    for on your own cluster.

Every SparkSession serves a web UI, by default at `http://<driver>:4040`
(incrementing to `4041`, `4042`, ... if the port is taken). This module is
a tour of the tabs you'll actually use when diagnosing a slow or failing
job, tied back to the concepts from modules 1–7.

## Finding and confirming the UI

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ui-tour").getOrCreate()

print(spark.sparkContext.uiWebUrl)
# 'http://driver-host:4040'
```

For a finished application (on YARN/standalone with history server
enabled), the same information is available after the fact through the
**Spark History Server**, since the live UI disappears when the driver
process exits.

## Jobs tab

Each *action* (`.collect()`, `.show()`, `.write()`, ...) creates one Job.
The Jobs tab lists them with duration, number of stages, and number of
tasks. This is the first place to look when a script "runs slow" but you
don't know which action is the culprit — sort by duration.

```python
df = spark.range(0, 10_000_000)
df.groupBy((df.id % 100).alias("bucket")).count().collect()   # this line -> one Job entry
```

Clicking into a Job shows its **DAG visualization** — the sequence of
stages, with a box drawn around operations that got fused via whole-stage
codegen (matching the `*(N)` markers from `.explain()` in module 1).

## Stages tab: where skew and shuffle problems show up

Each Job breaks into Stages at shuffle boundaries. The Stages tab's
per-stage summary is the primary skew-detection tool referenced in
module 3:

- **Duration** column, sorted descending, shows which stage dominates
  wall-clock time.
- Clicking into a stage shows a **task duration histogram** — a long tail
  (max task duration far exceeding the 75th percentile) is the classic
  skew signature.
- **Shuffle Read Size / Records** per task — if one task's shuffle read is
  10-100x the median, that task's partition holds a hot key.
- **Summary Metrics** table gives Min/25th/Median/75th/Max for task
  duration, GC time, and shuffle read/write — always check GC time here
  too: a stage where tasks spend 30%+ of their time in GC usually means
  under-provisioned executor memory (module 5), not a shuffle problem.

## SQL / DataFrame tab

For DataFrame and SQL API queries, the SQL tab shows the same physical
plan `.explain()` prints, but annotated with **live runtime metrics** at
each node — actual rows produced, actual data size, actual time spent —
overlaid directly on the plan graph. This is the single best place to
confirm AQE's `isFinalPlan=true` behavior from module 6: the SQL tab shows
the plan Spark actually executed, including any runtime join-strategy
switch or skew-partition split, not just the initial estimate.

```python
spark.sql("""
    SELECT bucket, COUNT(*) FROM (
        SELECT id % 100 AS bucket FROM range(0, 10000000)
    ) GROUP BY bucket
""").collect()
```

Clicking this query's entry in the SQL tab and expanding each node shows
"number of output rows" actually produced at that node — compare it
against what you expected from the logical query to catch a join
producing unexpected row multiplication (a common symptom of an
accidental many-to-many join key).

## Storage tab

Lists every RDD/DataFrame you've `.cache()`d or `.persist()`d, with size
in memory, size on disk (if spilled), and fraction cached. If a cached
DataFrame shows less than 100% cached, some partitions didn't fit and
either spilled to disk (`MEMORY_AND_DISK`) or were dropped and will
recompute from lineage on next access (`MEMORY_ONLY`) — this tab is how
you catch a caching strategy that isn't actually helping (module 5).

```python
big = spark.range(0, 50_000_000)
big.cache()
big.count()
# Storage tab now shows one entry: "*Range" with a cached fraction — check it's 100%
```

## Executors tab

Per-executor breakdown: active tasks, completed tasks, task time, input
size, shuffle read/write, and **storage memory used** vs. available. Two
things worth checking routinely:

- **Task distribution** across executors — if one executor is doing
  visibly more work than others over the whole job's lifetime (not just
  one stage), that's a data-locality or partitioning problem, not a
  single-stage skew problem.
- A **"Dead" executor** entry with a reason (`OOMKilled`, `Executor lost`)
  is your first clue when a job fails partway through — click into it for
  the stderr/stdout logs from that specific executor.

## Environment tab

A dump of every effective Spark configuration property for this
application — the fastest way to confirm a `spark.conf.set(...)` call
earlier in your script actually took effect, since some properties are
only settable at session-creation time and silently ignore a later
`.set()`.

```python
spark.conf.set("spark.sql.shuffle.partitions", 50)
# Confirm on the Environment tab that spark.sql.shuffle.partitions actually reads 50,
# not still 200 — this property IS settable at runtime, but others (like
# spark.executor.memory) are not and require a fresh SparkSession.
```

## Worked example: diagnosing a stuck job with the UI alone

Given: a job has been "running" for 45 minutes with no error, and one
stage's progress bar shows 199/200 tasks complete.

1. **Stages tab** → click the stuck stage → sort tasks by duration
   descending. One task shows `Status: RUNNING` for the full 45 minutes
   while the other 199 finished in under a minute each.
2. **Shuffle Read Size** for that one task is 40 GB versus ~200 MB median
   — confirms skew (module 3), not a hang or deadlock.
3. **Executors tab** for the executor running that task shows GC time
   climbing steadily — the executor is likely also memory-thrashing on
   top of the raw data volume.
4. Fix: apply salting (module 3) or verify AQE skew-join handling is
   enabled (module 6) and re-run; watch the same Stages tab for a task
   duration histogram that's now tight around the median.

## Exercise

1. In the Stages tab's Summary Metrics for a stage, you see: Min 1s,
   25th 2s, Median 2s, 75th 3s, Max 340s. Name the specific concept this
   pattern indicates and where else in the app you'd go to confirm it.
2. Explain what it means, specifically, if a cached DataFrame's Storage
   tab entry shows "60% cached" with the remainder listed under
   "disk-only" fraction, and what config change would raise that
   percentage.
3. A job's SQL tab shows a join node's "number of output rows" as 50x
   larger than either input side's row count. What kind of bug does this
   almost always indicate?
