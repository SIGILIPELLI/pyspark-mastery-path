# 04 · Debugging Failed Spark Jobs

!!! note "Not executed against a live cluster in this environment"
    Error messages and traces below are reconstructed from documented
    Spark failure modes, not captured from a live run here.

A failed Spark job's error message usually shows up wrapped in several
layers of Python/Py4J/JVM stack trace, with the actually-useful line
buried in the middle. This module is a field guide to the most common
failure signatures and how to get from "job died" to "here's the fix"
quickly.

## Reading a PySpark stack trace

```python
# A typical failure looks like this (heavily abbreviated):
"""
Traceback (most recent call last):
  File "job.py", line 42, in <module>
    result.write.parquet("/data/out")
py4j.protocol.Py4JJavaError: An error occurred while calling o123.parquet.
: org.apache.spark.SparkException: Job aborted due to stage failure:
Task 37 in stage 12.0 failed 4 times, most recent failure: Lost task 37.3
in stage 12.0 (TID 891, executor 5): org.apache.spark.memory.SparkOutOfMemoryError:
Unable to acquire 4194304 bytes of memory ...
Caused by: org.apache.spark.memory.SparkOutOfMemoryError: ...
"""
```

Read it bottom-up, not top-down: the outermost `Traceback` is just where
your Python code called into the JVM; the real cause is in the deepest
`Caused by:` line. "Task 37 ... failed 4 times" tells you Spark already
retried it 4 times (the default `spark.task.maxFailures`) before giving
up — a transient blip wouldn't reach this message, so treat it as a
reproducible failure, not a fluke.

## `SparkOutOfMemoryError` / executor OOM

```python
# Common causes, roughly in order of frequency:
# 1. A skewed partition (module 3) — one task holds far more data than
#    executor memory can handle, even though the average partition is fine.
# 2. A broadcast join gone wrong (module 7) — broadcasting something
#    larger than intended.
# 3. A UDF/pandas_udf leaking Python-side memory outside the JVM heap,
#    exhausting memoryOverhead rather than executor.memory.
# 4. Caching too much data with an inappropriate storage level (module 5).

# Diagnostic first step: check the Stages tab (module 8) for that specific
# stage's task duration/shuffle-read histogram BEFORE changing any config —
# confirms which of the above it actually is.
```

```python
# If it's skew: apply salting or confirm AQE skew handling (module 3, 6)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)

# If it's a bad broadcast: lower the auto-threshold or remove an explicit broadcast() call
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 20 * 1024 * 1024)

# If it's UDF/Python memory: raise overhead, not executor.memory
# spark.executor.memoryOverhead: 2g -> 4g
```

## `AnalysisException`: caught before the job even runs

```python
# df.select("cutsomer_id")   # typo
# pyspark.sql.utils.AnalysisException: Column 'cutsomer_id' does not exist.
# Did you mean one of the following? [customer_id, order_id, ...]
```

`AnalysisException` fires during the analysis phase (module 1) — before
any executor does any work — so it's always a logic/schema bug in your
code, never a cluster resource issue. Spark's "did you mean" suggestion is
usually exactly the fix; when it isn't, print `df.printSchema()`
immediately before the failing line to confirm the DataFrame's actual
shape at that point in the pipeline, since an upstream `.select()` or
`.join()` may have silently dropped or renamed the column you expect.

## `PythonException` inside a UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(IntegerType())
def parse_age(age_str):
    return int(age_str)   # will raise ValueError on non-numeric input

df = spark.createDataFrame([("25",), ("unknown",)], ["age_str"])
# df.withColumn("age", parse_age("age_str")).collect()
# org.apache.spark.api.python.PythonException: 'ValueError: invalid literal
# for int() with base 10: 'unknown''
```

A raw Python exception inside a UDF surfaces wrapped as
`PythonException`, and it kills the whole task (and, after retries, the
whole job) — UDFs don't fail "softly" per row by default. Fix by handling
bad input explicitly inside the UDF:

```python
@udf(IntegerType())
def parse_age_safe(age_str):
    try:
        return int(age_str)
    except (ValueError, TypeError):
        return None

df.withColumn("age", parse_age_safe("age_str")).show()
# +-------+----+
# |age_str| age|
# +-------+----+
# |     25|  25|
# |unknown|null|
# +-------+----+
```

## Executor lost / `Container killed by YARN`

```python
# "ExecutorLostFailure (executor 3 exited caused by one of the running
# tasks) Reason: Container killed by YARN for exceeding memory limits.
# 8.2 GB of 8 GB physical memory used. Consider boosting
# spark.executor.memoryOverhead."
```

This is YARN's own memory cap (executor.memory + memoryOverhead,
roughly), not the JVM's internal OOM — the fix is almost always exactly
what the message says: raise `memoryOverhead`, especially if the job uses
Python UDFs/pandas UDFs, which live in that overhead space (module 5).

## Driver OOM vs. executor OOM

```python
# Driver OOM usually comes from:
# 1. .collect() on a DataFrame too large to fit on the driver
# 2. Building a broadcast variable/table too large for driver memory (module 7)
# 3. Accumulating results in a Python list across many .collect() calls in a loop

# df.collect()   # NEVER do this on a large DataFrame — pulls every row to the driver
# Prefer: df.write(...), or df.take(n) / df.limit(n).collect() for a bounded sample
```

A `java.lang.OutOfMemoryError: Java heap space` reported by the **driver**
(check which process in the stack trace/log source) needs a completely
different fix than an executor OOM — raising `spark.driver.memory`, or
more often, simply removing the `.collect()` that shouldn't have existed.

## Stuck/hanging jobs (no error, no progress)

```python
# Checklist, in order of likelihood:
# 1. Genuine skew (module 3/8) — one task running far longer than the rest,
#    not actually hung, just very slow. Check Stages tab task duration histogram.
# 2. Deadlock/resource starvation — not enough executors to satisfy every
#    stage's minimum parallelism (rare, but check spark.dynamicAllocation
#    settings and cluster resource availability).
# 3. A broadcast join blocked collecting the broadcast side because a
#    DIFFERENT executor died mid-broadcast and it's silently retrying
#    (check Executors tab for a recently "Dead" entry).
# 4. Cartesian product from a missing join condition — row count exploding
#    silently, job LOOKS hung but is actually processing an enormous
#    unintended row count.
df1.crossJoin(df2)   # if you meant .join(df2, "key") — check the SQL tab's
                      # "number of output rows" for a suspiciously huge value
```

## Worked example: from stack trace to fix, step by step

```python
# Symptom: nightly job fails ~1x/week with:
# "Lost task 142 in stage 8.0 ... SparkOutOfMemoryError"

# Step 1: Stages tab, stage 8 -> task duration histogram shows task 142's
#         shuffle read is 45x the median -> confirms skew, not a random OOM.
# Step 2: identify the join/groupBy key for stage 8 via the SQL tab.
# Step 3: check key cardinality/distribution for that column.
skewed_key_counts = spark.sql("SELECT customer_id, COUNT(*) c FROM orders GROUP BY customer_id ORDER BY c DESC LIMIT 5")
skewed_key_counts.show()
# Step 4: apply the fix — salting (module 3) or confirm AQE skew handling
#         is actually enabled (it may have been silently overridden by a
#         config file, check Environment tab).
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
```

## Exercise

1. Given `"Container killed by YARN for exceeding memory limits ... 8.2 GB
   of 8 GB"`, write out the two specific config changes you'd try first
   and in what order, with reasoning for the order.
2. A UDF's `PythonException` traceback shows a `KeyError` on a dict
   lookup. Rewrite the UDF defensively so bad input produces `null`
   instead of failing the task.
3. A job "hangs" with no errors for 2 hours on a stage that normally
   takes 5 minutes. List three specific things you'd check, in the order
   you'd check them, and what each would tell you.
