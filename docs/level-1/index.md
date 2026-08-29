# Level 1 · Entry <span class="level-badge">Foundations</span>

Goal: understand every moving part of a Spark job — architecture, the
SparkSession, RDDs vs. DataFrames, reading and writing data, DataFrame basics,
schemas, and aggregations — and ship a working CSV-to-Parquet ETL script built
from those parts.

## Modules

1. [What Is Spark & Why Distributed Processing](01-what-is-spark-distributed-processing.md)
2. [Spark Architecture (Driver, Executors, Cluster Manager)](02-spark-architecture.md)
3. [SparkSession & Your First PySpark Script](03-sparksession-first-script.md)
4. [RDDs vs. DataFrames](04-rdds-vs-dataframes.md)
5. [Reading Data (CSV, JSON, Parquet)](05-reading-data.md)
6. [DataFrame Basics (select, filter, withColumn)](06-dataframe-basics.md)
7. [Schemas & Data Types](07-schemas-data-types.md)
8. [Basic Aggregations (groupBy, agg)](08-basic-aggregations.md)
9. [Writing Data Out](09-writing-data-out.md)
10. [Capstone — CSV-to-Parquet ETL Script](10-capstone-csv-to-parquet-etl.md)

By the end of this level you'll be able to explain what happens between
`spark-submit` and a finished job, load data from CSV/JSON/Parquet into a
DataFrame, shape it with `select`/`filter`/`withColumn`, aggregate it with
`groupBy`, and write the result back out as partitioned Parquet.

!!! info "Setup for this level"
    ```bash
    pip install pyspark
    ```
    You also need a JDK (11 or 17) on your `PATH` — Spark's engine runs on the
    JVM. Everything in this level runs locally on a single machine in
    "local mode" (`local[*]`) — no cluster, no cloud account, no API key.
