# 01 · What Is Spark & Why Distributed Processing

## The problem Spark solves

Imagine you have a 500 GB CSV file of e-commerce orders and you need to
compute total revenue per country. A single machine with 16 GB of RAM simply
cannot load that file into memory at once. Even if it could, scanning,
filtering, and aggregating 500 GB on one CPU core (or even 16 cores) would
take hours.

The core idea of **distributed processing** is: instead of moving a huge
dataset to one powerful machine, split the dataset into pieces and move small
pieces of *compute* to many ordinary machines that each hold a piece of the
data. Each machine processes its slice in parallel, and the partial results
are combined at the end. Ten machines each holding 50 GB and running the same
filter/aggregate logic can finish in a fraction of the time a single machine
would take — and it scales further as you add more machines.

This is not a new idea — Google's MapReduce paper (2004) and Hadoop
popularized it. **Apache Spark** is the dominant modern engine for this kind
of work, because it improved on early MapReduce in three important ways:

1. **In-memory computation.** Hadoop MapReduce wrote intermediate results to
   disk between every step. Spark keeps intermediate data in memory (RAM)
   whenever possible, which makes multi-step pipelines dramatically faster —
   often 10-100x for iterative workloads.
2. **A richer, higher-level API.** Instead of hand-writing `map` and `reduce`
   functions for every operation, Spark gives you DataFrames with SQL-like
   operations (`select`, `filter`, `groupBy`, `join`) that an internal
   optimizer can rewrite for better performance.
3. **One engine, many workloads.** Spark unifies batch processing, SQL
   queries, streaming, and machine learning under one API and one execution
   engine, instead of requiring separate systems for each.

## Why "distributed" instead of "just buy a bigger machine"

Scaling a single machine ("scaling up" / vertical scaling) has a ceiling —
there's a limit to how much RAM and how many cores you can put in one box,
and the cost grows non-linearly as you approach that ceiling. Scaling by
adding more machines ("scaling out" / horizontal scaling) is, in principle,
unbounded — you can keep adding commodity machines to a cluster. Distributed
engines like Spark are built to make horizontal scaling practical: they
handle splitting the work, moving data between machines when needed, and
recovering automatically when a machine fails mid-job (which is expected to
happen occasionally at scale).

**PySpark** is the Python API for Spark. Spark itself is written in Scala and
runs on the Java Virtual Machine (JVM), but PySpark lets you write Python code
that gets translated into the same distributed execution plan a Scala program
would produce — for DataFrame and SQL operations, there is effectively no
performance penalty for using Python over Scala, because the actual data
processing happens inside the JVM, not in the Python interpreter.

## When you do (and don't) need Spark

Spark shines when:

- Your dataset doesn't fit comfortably in one machine's memory (roughly:
  tens of GB and up, though the exact threshold depends on your hardware).
- You need the same transformation logic to run against a growing dataset
  without a rewrite.
- You need a single framework for batch ETL, SQL analytics, and streaming.

Spark is overkill when:

- Your dataset fits comfortably in memory on one machine — `pandas` or
  `polars` will be simpler and often faster for that case, because Spark has
  real overhead (starting a JVM, planning, network coordination between
  processes) that dominates for small data.
- You need sub-millisecond, single-record lookups — that's a database's job,
  not a batch/streaming engine's.

A useful mental model for this whole course: **Spark's job is to take a
description of what you want done (a DataFrame of transformations) and figure
out the most efficient way to actually do it across a cluster of machines.**
Everything from here builds on that idea.

## Worked example: picturing the split

You won't run code yet in this module — but here's the mental picture to
carry into Module 2, expressed as a small diagram of what "distributed"
means for a single `filter` + `count` operation on a 3-partition dataset:

```text
Full dataset (conceptually one table):
+----+---------+--------+
| id | country | amount |
+----+---------+--------+
| 1  | US      | 120    |
| 2  | IN      | 45     |
| 3  | US      | 300    |
| 4  | DE      | 60     |
| 5  | IN      | 90     |
| 6  | US      | 15     |
+----+---------+--------+

Split into 3 partitions, each handled by a different executor:

Partition A (rows 1-2)   Partition B (rows 3-4)   Partition C (rows 5-6)
filter(country == "US")  filter(country == "US")  filter(country == "US")
  -> 1 row (id=1)          -> 1 row (id=3)          -> 0 rows

Each executor counts its own filtered partition, then the driver
sums the partial counts: 1 + 1 + 0 = 2 matching rows total.
```

Every operation you'll write in PySpark compiles down to something like
this: work split across partitions, computed independently and in parallel,
then combined.

## Exercise

Without writing any code, answer these for yourself (or in a notes file) —
they set up ideas used in the next two modules:

1. If a 100 GB dataset is split into 200 partitions, roughly how large is
   each partition, and why might having *many small* partitions instead of a
   *few large* ones help parallelism, up to a point?
2. Suppose one machine in a 10-machine Spark cluster crashes halfway through
   a job. Based on the "combine partial results" model above, why does it
   make sense that Spark can recompute just that machine's lost partitions
   rather than restarting the entire job from scratch?
3. Name one workload you've encountered (personally or hypothetically) that
   would benefit from Spark, and one that wouldn't (fits easily in memory) —
   justify each in one sentence.

There is no single "correct" written answer to check against; the goal is to
walk into Module 2 already thinking in terms of partitions and parallel work,
since that's exactly what Spark's architecture is built around.
