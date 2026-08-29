---
title: "Learn PySpark Free: Beginner to Master Course"
description: "Free course on PySpark and Apache Spark for big-data processing -- DataFrames, joins, window functions, Structured Streaming, and performance tuning, with real hands-on code."
---

# PySpark Mastery Path

A structured, module-wise training program on **PySpark**, Apache Spark's Python
API for distributed, big-data processing — from your first `SparkSession` to
production-grade Spark jobs, streaming pipelines, and lakehouse patterns — with
real code in every module and a hands-on project at the end of each level.

Spark is the engine behind most large-scale data pipelines you'll meet in the
wild: it takes a dataset too big (or too slow) for a single machine and spreads
the work across a cluster, while giving you an API that still reads like
ordinary DataFrame code. This site teaches Spark from first principles — what a
driver and an executor actually do, why a shuffle is expensive, why joins pick
different physical strategies — not just which method to call.

## How the program is organized

| Level | Focus | Modules |
|-------|-------|---------|
| [Level 1 · Entry](level-1/index.md) | Spark architecture, SparkSession, RDDs vs. DataFrames, reading/writing data, DataFrame basics, schemas, aggregations | 9 topics + 1 capstone |
| [Level 2 · Intermediate](level-2/index.md) | Joins (broadcast vs. shuffle), window functions, UDFs, partitioning strategy, caching & persistence, Spark SQL | 9 topics + 1 capstone |
| [Level 3 · Advanced](level-3/index.md) | Execution plans (`explain()`), shuffle optimization, data skew, Structured Streaming, performance tuning | 9 topics + 1 capstone |
| [Level 4 · Master](level-4/index.md) | Delta Lake / lakehouse patterns, orchestration with Airflow, cost/performance tradeoffs at scale, debugging failed jobs | 9 topics + 1 capstone |

## What you need

- **Python 3.10+**, `pip`, and a Java runtime (JDK 11 or 17) — Spark runs on the
  JVM even when you drive it from Python.
- `pip install pyspark` gives you a local, single-machine Spark you can run on
  a laptop — no cluster, no cloud account needed for Level 1.
- Later levels introduce Delta Lake, Structured Streaming, and orchestration
  concepts; each lesson states exactly what to install before you start.

## How to use this site

- Work through each level in order — later modules assume earlier ones.
- Every topic page has real, syntactically-checked PySpark code. Code that was
  reasoned through carefully but not executed against a live cluster in this
  environment is labeled as such, so you always know what you're looking at.
- Each level ends with a project that combines everything learned in that level.
- Use the search bar (top of the page) to jump straight to a topic.

Start here → [Level 1 · Entry](level-1/index.md)

## Related tracks

PySpark sits next to data engineering, SQL, and distributed systems more
broadly. Sister sites cover the neighboring ground:

- [Data Engineering Mastery Path](https://sigilipelli.github.io/data-engineering-mastery-path/) — ETL pipelines, orchestration, and data platforms
- [SQL Mastery Path](https://sigilipelli.github.io/sql-mastery-path/) — SQL from first principles
- [AI/ML Mastery Path](https://sigilipelli.github.io/ai-ml-mastery-path/) — machine-learning foundations
- [Python Mastery Path](https://sigilipelli.github.io/python-mastery-path/) — Python from first principles

🎥 **Prefer video?** Watch the [Mastery Path video series](https://youtube.com/@sigilipelli) on YouTube — Shorts and full walkthroughs of these lessons.

## More from the Mastery Path series

<!-- cross-link grid: added separately -->
