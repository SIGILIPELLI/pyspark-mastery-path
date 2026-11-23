---
title: "PySpark Interview Questions and Answers (179 Questions)"
description: "179 PySpark interview questions with answers and explanations, from beginner to advanced, organised by level. Free, from the PySpark Mastery Path."
---

# PySpark Interview Questions and Answers

**179 questions** with the correct answer and a short explanation, organised from beginner (Level 1) to advanced (Level 4). Every question comes from a lesson in the free PySpark Mastery Path, so you can open the lesson to go deeper.

!!! tip "Practise, don't just read"
    Each lesson also ends with an interactive quiz, and the [final quiz](quiz.md) tests all four levels. Try to answer each question below before reading the answer.

## Level 1 · Entry

### [01 · What Is Spark &amp; Why Distributed Processing](level-1/01-what-is-spark-distributed-processing.md)

**Q1.** What problem does Spark solve?

**Answer:** Processing data too large or slow for one machine by distributing work across a cluster

Distributed processing.

**Q2.** What is lazy evaluation in Spark?

**Answer:** Transformations only build a plan; work runs when an action is called

Enables optimization.

**Q3.** Which of these is an action?

**Answer:** count()

Actions trigger execution.

**Q4.** What makes Spark faster than classic MapReduce for iterative work?

**Answer:** It keeps data in memory between stages instead of writing to disk each time

In-memory processing.

### [02 · Spark Architecture (Driver, Executors, Cluster Manager)](level-1/02-spark-architecture.md)

**Q5.** What does the driver do?

**Answer:** Runs your program, builds the plan and schedules tasks on executors

Coordinator.

**Q6.** What is an executor?

**Answer:** A process on a worker that runs tasks and holds cached data

Parallel workers.

**Q7.** What are jobs, stages and tasks?

**Answer:** An action creates a job; shuffles split it into stages; each stage runs tasks per partition

Hierarchy.

**Q8.** What causes a new stage?

**Answer:** A shuffle (wide dependency)

Narrow vs wide.

### [03 · SparkSession &amp; Your First PySpark Script](level-1/03-sparksession-first-script.md)

**Q9.** What does this print?

```python
df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "v"])
print(df.count(), df.columns, df.filter(F.col("id") >= 2).count())
```

**Answer:** 3 ['id', 'v'] 2

Basic DataFrame operations. Verified by running it on PySpark 3.5 (local mode).

**Q10.** What does this print?

```python
print(spark.range(5).select((F.col("id") * 2).alias("d")).agg(F.sum("d")).collect()[0][0])
```

**Answer:** 20

spark.range creates ids 0..4. Verified by running it on PySpark 3.5 (local mode).

**Q11.** What is a SparkSession?

**Answer:** The entry point for DataFrame and SQL functionality

Use SparkSession.builder.

**Q12.** What does master('local[2]') mean?

**Answer:** Run Spark locally with 2 worker threads

Development mode.

### [04 · RDDs vs. DataFrames](level-1/04-rdds-vs-dataframes.md)

**Q13.** What does this print?

```python
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6], 3)
print(rdd.getNumPartitions(), rdd.map(lambda x: x * x).filter(lambda x: x > 4).collect(), rdd.glom().map(len).collect())
```

**Answer:** 3 [9, 16, 25, 36] [2, 2, 2]

RDD transformations and partitions. Verified by running it on PySpark 3.5 (local mode).

**Q14.** Why prefer DataFrames over RDDs?

**Answer:** They have schemas and the Catalyst optimizer, so queries are faster and shorter

Structured APIs.

**Q15.** When use RDDs?

**Answer:** For low-level control or unstructured data where DataFrames don't fit

Escape hatch.

**Q16.** What is an immutable DataFrame?

**Answer:** Transformations return new DataFrames; the original never changes

Functional style.

### [05 · Reading Data (CSV, JSON, Parquet)](level-1/05-reading-data.md)

**Q17.** What does this print?

```python
import tempfile, os
d = tempfile.mkdtemp()
open(d + "/a.csv", "w").write("id,name,score\n1,Ann,3.5\n2,Bob,\n3,Cy,4.0\n")
df = spark.read.csv(d + "/a.csv", header=True, inferSchema=True)
print(df.dtypes)
print(df.filter(F.col("score").isNull()).count())
```

**Answer:** [('id', 'int'), ('name', 'string'), ('score', 'double')]
1

inferSchema reads types; empty values are null. Verified by running it on PySpark 3.5 (local mode).

**Q18.** Why define a schema explicitly in production?

**Answer:** Avoids an extra pass and prevents wrong inferred types

Faster and safer.

**Q19.** Which read option handles malformed CSV rows?

**Answer:** mode (PERMISSIVE, DROPMALFORMED, FAILFAST)

Corrupt record handling.

**Q20.** Which format is columnar and good for analytics?

**Answer:** Parquet

Compression and predicate pushdown.

### [06 · DataFrame Basics (select, filter, withColumn)](level-1/06-dataframe-basics.md)

**Q21.** What does this print?

```python
df = spark.createDataFrame([("a", 10), ("b", 20), ("c", 30)], ["k", "v"])
r = df.withColumn("v2", F.col("v") * 2).filter(F.col("v2") > 20).select("k", "v2").orderBy("k")
print(r.collect())
print(df.drop("v").columns, df.withColumnRenamed("v", "value").columns)
```

**Answer:** [Row(k='b', v2=40), Row(k='c', v2=60)]
['k'] ['k', 'value']

select/filter/withColumn. Verified by running it on PySpark 3.5 (local mode).

**Q22.** What does this print?

```python
df = spark.createDataFrame([(1, "x"), (1, "x"), (2, "y")], ["id", "v"])
print(df.distinct().count(), df.dropDuplicates(["id"]).count(), df.limit(2).count())
```

**Answer:** 2 2 2

distinct and dropDuplicates. Verified by running it on PySpark 3.5 (local mode).

**Q23.** What does df.select('a') return?

**Answer:** A new DataFrame with only column a

Lazy.

**Q24.** What does df.show() do?

**Answer:** Prints the first rows (an action)

Triggers execution.

### [07 · Schemas &amp; Data Types](level-1/07-schemas-data-types.md)

**Q25.** What does this print?

```python
schema = StructType([StructField("id", IntegerType(), False), StructField("name", StringType(), True)])
df = spark.createDataFrame([(1, "a"), (2, None)], schema)
print(df.schema.simpleString(), df.schema["id"].nullable, df.filter(F.col("name").isNull()).count())
```

**Answer:** struct&lt;id:int,name:string&gt; False 1

Explicit schemas. Verified by running it on PySpark 3.5 (local mode).

**Q26.** What does this print?

```python
df = spark.createDataFrame([("1", "2.5"), ("x", "3")], ["a", "b"])
r = df.select(F.col("a").cast("int").alias("a"), F.col("b").cast("double").alias("b"))
print(r.collect())
```

**Answer:** [Row(a=1, b=2.5), Row(a=None, b=3.0)]

Failed casts become null (without ANSI mode). Verified by running it on PySpark 3.5 (local mode).

**Q27.** Which type should hold money?

**Answer:** DecimalType with fixed precision

Avoid binary float.

**Q28.** What is nullable in a schema?

**Answer:** Whether the column may contain nulls

Metadata.

### [08 · Basic Aggregations (groupBy, agg)](level-1/08-basic-aggregations.md)

**Q29.** What does this print?

```python
df = spark.createDataFrame([("a", 10), ("a", 20), ("b", 5), ("b", None)], ["k", "v"])
r = df.groupBy("k").agg(F.count("*").alias("rows"), F.count("v").alias("nn"), F.sum("v").alias("s"), F.avg("v").alias("a")).orderBy("k")
print(r.collect())
```

**Answer:** [Row(k='a', rows=2, nn=2, s=30, a=15.0), Row(k='b', rows=2, nn=1, s=5, a=5.0)]

count(col) ignores nulls; count(*) counts rows. Verified by running it on PySpark 3.5 (local mode).

**Q30.** What does this print?

```python
df = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["x"])
print(df.agg(F.min("x"), F.max("x"), F.round(F.stddev("x"), 3)).collect()[0])
```

**Answer:** Row(min(x)=1, max(x)=4, round(stddev(x), 3)=1.291)

stddev is the sample standard deviation. Verified by running it on PySpark 3.5 (local mode).

**Q31.** What is the difference between groupBy().count() and count()?

**Answer:** groupBy counts per group; df.count() is an action returning one number

Transformation vs action.

**Q32.** Why use approx_count_distinct on huge data?

**Answer:** It is much cheaper than exact distinct counts with small error

HyperLogLog.

### [09 · Writing Data Out](level-1/09-writing-data-out.md)

**Q33.** What does this print?

```python
import tempfile, os
d = tempfile.mkdtemp() + "/out"
df = spark.createDataFrame([("a", 1), ("b", 2), ("a", 3)], ["k", "v"])
df.write.partitionBy("k").mode("overwrite").parquet(d)
print(sorted(x for x in os.listdir(d) if x.startswith("k=")))
print(spark.read.parquet(d).filter("k = 'a'").count())
```

**Answer:** ['k=a', 'k=b']
2

partitionBy creates directories. Verified by running it on PySpark 3.5 (local mode).

**Q34.** What does mode('append') do?

**Answer:** Adds to existing data instead of overwriting

Also overwrite, ignore, error.

**Q35.** Why avoid many tiny output files?

**Answer:** They slow reads and overload the metadata/file system; coalesce or repartition before writing

Small file problem.

**Q36.** What is dynamic partition overwrite for?

**Answer:** Replace only the partitions present in the new data

Safe incremental writes.

### [10 · Capstone — CSV-to-Parquet ETL Script](level-1/10-capstone-csv-to-parquet-etl.md)

**Q37.** What does the CSV-to-Parquet ETL project combine?

**Answer:** Reading with a schema, cleaning, transforming and writing partitioned Parquet

Level 1 capstone.

**Q38.** How validate an ETL output?

**Answer:** Compare row counts and key aggregates between input and output

Reconciliation.

**Q39.** Where should bad rows go?

**Answer:** To a quarantine location with the reason, not silently dropped

Data quality.

**Q40.** Why make the job idempotent?

**Answer:** Re-running it produces the same result without duplicates

Reliable pipelines.

## Level 2 · Intermediate

### [01 · Joins Broadcast Vs Shuffle](level-2/01-joins-broadcast-vs-shuffle.md)

**Q41.** What does this print?

```python
orders = spark.createDataFrame([(1,101,250.0),(2,102,75.5),(3,101,40.0),(4,103,500.0),(5,104,12.25)], ["order_id","customer_id","amount"])
customers = spark.createDataFrame([(101,"Alice","US"),(102,"Bob","IN"),(103,"Carla","DE")], ["customer_id","name","country"])
inner = orders.join(customers, "customer_id", "inner")
left = orders.join(customers, "customer_id", "left")
anti = orders.join(customers, "customer_id", "left_anti")
print(inner.count(), left.count(), anti.collect())
```

**Answer:** 4 5 [Row(customer_id=104, order_id=5, amount=12.25)]

Join types. Verified by running it on PySpark 3.5 (local mode).

**Q42.** What does this print?

```python
orders = spark.createDataFrame([(1,101,250.0),(2,102,75.5),(3,101,40.0),(4,103,500.0),(5,104,12.25)], ["order_id","customer_id","amount"])
customers = spark.createDataFrame([(101,"Alice","US"),(102,"Bob","IN"),(103,"Carla","DE")], ["customer_id","name","country"])
j = orders.join(F.broadcast(customers), "customer_id")
plan = j._jdf.queryExecution().executedPlan().toString()
print("BroadcastHashJoin" in plan, "SortMergeJoin" in plan)
```

**Answer:** True False

A hint forces a broadcast of the small side. Verified by running it on PySpark 3.5 (local mode).

**Q43.** When is a broadcast join best?

**Answer:** When one side is small enough to fit in memory on every executor

Avoids a shuffle.

**Q44.** What is the default join strategy for large tables?

**Answer:** Sort-merge join (shuffle both sides)

Expensive.

### [02 · Window Functions](level-2/02-window-functions.md)

**Q45.** What does this print?

```python
df = spark.createDataFrame([("a", 1, 10), ("a", 2, 30), ("a", 3, 20), ("b", 1, 5), ("b", 2, 5)], ["k", "t", "v"])
w = Window.partitionBy("k").orderBy("t")
r = df.withColumn("rn", F.row_number().over(w)).withColumn("prev", F.lag("v").over(w)).withColumn("run", F.sum("v").over(w)).orderBy("k", "t")
print([tuple(x) for x in r.select("k", "rn", "prev", "run").collect()])
```

**Answer:** [('a', 1, None, 10), ('a', 2, 10, 40), ('a', 3, 30, 60), ('b', 1, None, 5), ('b', 2, 5, 10)]

Running totals with ordered windows. Verified by running it on PySpark 3.5 (local mode).

**Q46.** What does this print?

```python
df = spark.createDataFrame([("a", 90), ("a", 90), ("a", 70), ("b", 50)], ["k", "s"])
w = Window.partitionBy("k").orderBy(F.desc("s"))
r = df.withColumn("rank", F.rank().over(w)).withColumn("dense", F.dense_rank().over(w)).orderBy("k", F.desc("s"))
print([(x.k, x.s, x["rank"], x.dense) for x in r.collect()])
```

**Answer:** [('a', 90, 1, 1), ('a', 90, 1, 1), ('a', 70, 3, 2), ('b', 50, 1, 1)]

rank leaves gaps; dense_rank does not. Verified by running it on PySpark 3.5 (local mode).

**Q47.** Why do windows without partitionBy cause trouble?

**Answer:** All data moves to a single partition

Always partition.

**Q48.** What does a frame like rowsBetween(-2, 0) define?

**Answer:** The current row and the two before it for a moving average

Moving windows.

### [03 · Udfs](level-2/03-udfs.md)

**Q49.** What does this print?

```python
from pyspark.sql.functions import udf
@udf(returnType=StringType())
def tier(n):
    return "high" if n is not None and n >= 100 else "low"
df = spark.createDataFrame([(50,), (150,), (None,)], ["v"])
a = df.withColumn("t", tier("v")).collect()
b = df.withColumn("t", F.when(F.col("v") >= 100, "high").otherwise("low")).collect()
print([r.t for r in a], [r.t for r in b])
```

**Answer:** ['low', 'high', 'low'] ['low', 'high', 'low']

Same result; built-ins are faster. Verified by running it on PySpark 3.5 (local mode).

**Q50.** Why prefer built-in functions over Python UDFs?

**Answer:** They run in the JVM and the optimizer can see them; UDFs serialize rows to Python

Performance.

**Q51.** What are pandas UDFs?

**Answer:** Vectorized UDFs operating on Arrow batches (much faster than row UDFs)

When logic can't be expressed in built-ins.

**Q52.** What happens to nulls in a UDF?

**Answer:** You must handle None yourself

Defensive code.

### [04 · Partitioning Strategy](level-2/04-partitioning-strategy.md)

**Q53.** What does this print?

```python
df = spark.range(1000)
print(df.rdd.getNumPartitions() >= 1, df.repartition(8).rdd.getNumPartitions(), df.repartition(8).coalesce(2).rdd.getNumPartitions())
```

**Answer:** True 8 2

repartition shuffles; coalesce merges without a full shuffle. Verified by running it on PySpark 3.5 (local mode).

**Q54.** What does this print?

```python
df = spark.createDataFrame([(i % 3, i) for i in range(12)], ["k", "v"])
p = df.repartition(3, "k")
places = {}
for idx, part in enumerate(p.rdd.glom().collect()):
    for r in part: places.setdefault(r.k, set()).add(idx)
print(sorted((k, len(v)) for k, v in places.items()))
print(p.rdd.getNumPartitions())
```

**Answer:** [(0, 1), (1, 1), (2, 1)]
3

Hash partitioning keeps every row of a key in a single partition (two keys may share one). Verified by running it on PySpark 3.5 (local mode).

**Q55.** What is a good target partition size?

**Answer:** Roughly 100–200 MB per partition

Balance parallelism and overhead.

**Q56.** When use coalesce instead of repartition?

**Answer:** To reduce partitions cheaply without a full shuffle

Cheaper.

### [05 · Caching Persistence](level-2/05-caching-persistence.md)

**Q57.** What does this print?

```python
df = spark.range(100000).withColumn("m", F.col("id") % 7)
c = df.cache()
print(c.count(), c.is_cached, c.storageLevel.useMemory)
c.unpersist()
print(c.is_cached)
```

**Answer:** 100000 True True
False

cache() is lazy until an action. Verified by running it on PySpark 3.5 (local mode).

**Q58.** When is caching worthwhile?

**Answer:** When a DataFrame is reused several times and is expensive to recompute

Memory is limited.

**Q59.** What does persist(MEMORY_AND_DISK) do?

**Answer:** Spills partitions that don't fit in memory to disk

Safer than memory-only.

**Q60.** What should you do when you no longer need a cached DataFrame?

**Answer:** unpersist() to free memory

Resource hygiene.

### [06 · Spark SQL Basics](level-2/06-spark-sql-basics.md)

**Q61.** What does this print?

```python
df = spark.createDataFrame([("a", 3), ("b", 8), ("c", 5)], ["name", "score"])
df.createOrReplaceTempView("t")
print(spark.sql("SELECT name FROM t WHERE score > 4 ORDER BY score DESC").collect())
print(spark.sql("SELECT count(*) AS n, round(avg(score), 2) AS a FROM t").collect())
```

**Answer:** [Row(name='b'), Row(name='c')]
[Row(n=3, a=5.33)]

Temp views let you mix SQL and DataFrames. Verified by running it on PySpark 3.5 (local mode).

**Q62.** What does a temp view live for?

**Answer:** The SparkSession

Session-scoped.

**Q63.** What is a global temp view?

**Answer:** A view shared across sessions in the same application, under global_temp

Cross-session.

**Q64.** Why is Spark SQL good for analysts?

**Answer:** Familiar SQL runs on the same optimizer and engine as DataFrames

Unified.

### [07 · SQL And Dataframe API](level-2/07-sql-and-dataframe-api.md)

**Q65.** What does this print?

```python
df = spark.createDataFrame([("a", 1), ("a", 2), ("b", 3)], ["k", "v"])
api = df.groupBy("k").agg(F.sum("v").alias("s")).orderBy("k").collect()
df.createOrReplaceTempView("t")
sql = spark.sql("SELECT k, SUM(v) AS s FROM t GROUP BY k ORDER BY k").collect()
print(api == sql, api)
```

**Answer:** True [Row(k='a', s=3), Row(k='b', s=3)]

Both APIs compile to the same plan. Verified by running it on PySpark 3.5 (local mode).

**Q66.** Which should you choose?

**Answer:** Whichever expresses the logic most clearly; they perform the same

Catalyst.

**Q67.** What does explain() show?

**Answer:** The logical and physical plans Spark will run

Compare both.

**Q68.** How mix SQL and API in one pipeline?

**Answer:** Register DataFrames as views and chain spark.sql and DataFrame calls

Flexible.

### [08 · Nulls Data Cleaning](level-2/08-nulls-data-cleaning.md)

**Q69.** What does this print?

```python
df = spark.createDataFrame([("a", 1.0), ("b", None), (None, 3.0), ("a", None)], ["k", "v"])
print(df.na.fill({"v": 0.0}).collect())
print(df.dropna(subset=["k"]).count(), df.dropna(how="all", subset=["k", "v"]).count(), df.filter(F.col("v").isNull()).count())
print(df.select(F.coalesce("k", F.lit("?")).alias("k")).collect())
```

**Answer:** [Row(k='a', v=1.0), Row(k='b', v=0.0), Row(k=None, v=3.0), Row(k='a', v=0.0)]
3 4 2
[Row(k='a'), Row(k='b'), Row(k='?'), Row(k='a')]

Null handling helpers. Verified by running it on PySpark 3.5 (local mode).

**Q70.** What does null == null return in Spark SQL?

**Answer:** null (unknown); use eqNullSafe / &lt;=&gt; for null-safe equality

Three-valued logic.

**Q71.** Why not blindly drop rows with nulls?

**Answer:** It can bias results; decide per column what's valid

Understand data.

**Q72.** What does na.drop(thresh=2) do?

**Answer:** Keeps rows with at least 2 non-null values

Threshold.

### [09 · Nested Complex Types](level-2/09-nested-complex-types.md)

**Q73.** What does this print?

```python
from pyspark.sql.types import ArrayType
df = spark.createDataFrame([(1, [10, 20], {"city": "Pune", "zip": "411"}), (2, [], {"city": "Oslo"})], "id int, nums array<int>, addr map<string,string>")
r = df.select("id", F.size("nums").alias("n"), F.col("addr")["city"].alias("city"), F.explode_outer("nums").alias("x"))
print([tuple(x) for x in r.orderBy("id", "x").collect()])
```

**Answer:** [(1, 2, 'Pune', 10), (1, 2, 'Pune', 20), (2, 0, 'Oslo', None)]

explode_outer keeps rows with empty arrays. Verified by running it on PySpark 3.5 (local mode).

**Q74.** What does this print?

```python
df = spark.createDataFrame([('{"a": 1, "b": {"c": "x"}}',)], ["j"])
r = df.select(F.get_json_object("j", "$.b.c").alias("c"), F.from_json("j", "a int, b struct<c:string>").alias("s"))
print(r.collect()[0].c, r.select("s.a", "s.b.c").collect())
```

**Answer:** x [Row(a=1, c='x')]

JSON helpers and struct access. Verified by running it on PySpark 3.5 (local mode).

**Q75.** What does explode do to an empty array?

**Answer:** Drops the row (use explode_outer to keep it)

Careful with counts.

**Q76.** How access a struct field?

**Answer:** col('s.field') or col('s')['field']

Dot notation.

### [10 · Capstone Multi Source Join](level-2/10-capstone-multi-source-join.md)

**Q77.** What does the multi-source join project combine?

**Answer:** Reading multiple formats, cleaning, joining and validating the result

Level 2 capstone.

**Q78.** How handle mismatched keys across sources?

**Answer:** Normalize (trim/case/type) before joining and measure unmatched rows

Key hygiene.

**Q79.** How detect join explosions?

**Answer:** Compare row counts before and after the join and check key uniqueness

Duplicate keys multiply rows.

**Q80.** What belongs in a join audit?

**Answer:** Matched, unmatched and duplicated key counts

Trust.

## Level 3 · Advanced

### [01 · Execution Plan Explain](level-3/01-execution-plan-explain.md)

**Q81.** What does this print?

```python
orders = spark.createDataFrame([(1,101,250.0),(2,102,75.5),(3,101,40.0),(4,103,500.0),(5,104,12.25)], ["order_id","customer_id","amount"])
customers = spark.createDataFrame([(101,"Alice","US"),(102,"Bob","IN"),(103,"Carla","DE")], ["customer_id","name","country"])
j = orders.groupBy("customer_id").agg(F.sum("amount").alias("t"))
text = j._jdf.queryExecution().executedPlan().toString()
print("HashAggregate" in text, "Exchange" in text)
```

**Answer:** True True

A groupBy needs a shuffle (Exchange). Verified by running it on PySpark 3.5 (local mode).

**Q82.** What do you read first in explain()?

**Answer:** The Exchange nodes (shuffles) and the join strategy

Cost drivers.

**Q83.** What is predicate pushdown?

**Answer:** Applying filters at the data source to read less data

Parquet/ORC.

**Q84.** What does 'explain("formatted")' offer?

**Answer:** A readable plan with operator details

Spark 3+.

### [02 · Shuffle Optimization](level-3/02-shuffle-optimization.md)

**Q85.** What is the biggest shuffle-reduction tactic?

**Answer:** Filter and project early, and broadcast small tables

Less data moved.

**Q86.** What does spark.sql.shuffle.partitions control?

**Answer:** The number of partitions after a shuffle (default 200)

Tune to data size.

**Q87.** What is a map-side combine?

**Answer:** Partial aggregation before the shuffle to reduce data moved

reduceByKey vs groupByKey.

**Q88.** Why avoid groupByKey on RDDs?

**Answer:** It shuffles all values without pre-aggregating

Prefer reduceByKey/aggregateByKey.

### [03 · Handling Skewed Data](level-3/03-handling-skewed-data.md)

**Q89.** What does this print?

```python
df = spark.createDataFrame([("hot", i) for i in range(8)] + [("cold", 1), ("warm", 2)], ["k", "v"])
counts = df.groupBy("k").count().orderBy(F.desc("count"), "k").collect()
print([(r.k, r["count"]) for r in counts])
salted = df.withColumn("salt", (F.rand(seed=1) * 3).cast("int"))
two = salted.groupBy("k", "salt").agg(F.count("*").alias("c")).groupBy("k").agg(F.sum("c").alias("n")).orderBy(F.desc("n"), "k").collect()
print([(r.k, r.n) for r in two])
```

**Answer:** [('hot', 8), ('cold', 1), ('warm', 1)]
[('hot', 8), ('cold', 1), ('warm', 1)]

Salting spreads a hot key over several partitions and then re-aggregates; the result is identical. Verified by running it on PySpark 3.5 (local mode).

**Q90.** How spot skew in the Spark UI?

**Answer:** A few tasks in a stage take much longer or process far more data

Task time distribution.

**Q91.** What does AQE skew-join handling do?

**Answer:** Splits oversized partitions automatically at runtime

Spark 3+.

**Q92.** What is salting?

**Answer:** Adding a random suffix to hot keys to distribute them, then aggregating again

Manual remedy.

### [04 · Structured Streaming Basics](level-3/04-structured-streaming-basics.md)

**Q93.** What does Structured Streaming treat a stream as?

**Answer:** An unbounded table that grows as new data arrives

Same DataFrame API.

**Q94.** What is a trigger?

**Answer:** When the engine processes new data (e.g. every 10 seconds or once)

Micro-batches.

**Q95.** What are output modes?

**Answer:** append, update and complete

What gets written each batch.

**Q96.** What is a watermark?

**Answer:** A bound on how late event-time data may arrive, so state can be dropped

State management.

### [05 · Performance Tuning](level-3/05-performance-tuning.md)

**Q97.** What is the first step in tuning?

**Answer:** Find the bottleneck (shuffle, skew, spill, GC) with the Spark UI

Measure.

**Q98.** What does spill mean?

**Answer:** Data didn't fit in memory and was written to disk, slowing tasks

Increase partitions.

**Q99.** What reduces serialization costs?

**Answer:** Using built-in functions and Kryo/Arrow where relevant

Less overhead.

**Q100.** What is a rule of thumb for executor cores?

**Answer:** About 4–5 cores per executor to avoid I/O contention

Balanced.

### [06 · Adaptive Query Execution](level-3/06-adaptive-query-execution.md)

**Q101.** What does this print?

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
print(spark.conf.get("spark.sql.adaptive.enabled"), spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))
df = spark.range(1000).groupBy((F.col("id") % 10).alias("k")).count()
print(df.count())
```

**Answer:** true true
10

AQE is on by default in Spark 3.2+; here it's enabled explicitly. Verified by running it on PySpark 3.5 (local mode).

**Q102.** What does AQE do at runtime?

**Answer:** Re-optimizes using real statistics: coalesces partitions, switches join strategy and handles skew

Adaptive.

**Q103.** Why is AQE valuable?

**Answer:** Static estimates are often wrong; runtime stats fix plans

Less manual tuning.

**Q104.** Does AQE replace understanding of your data?

**Answer:** No; it helps but you still design partitioning and joins

Tool, not magic.

### [07 · Broadcast Joins At Scale](level-3/07-broadcast-joins-at-scale.md)

**Q105.** Why might Spark not auto-broadcast a small DataFrame built with createDataFrame?

**Answer:** Automatic broadcast relies on size statistics, which in-memory Python data often lacks; use F.broadcast() to force it

Verified locally: even a 2-row DataFrame planned a sort-merge join without the hint.

**Q106.** What is the risk of broadcasting too much?

**Answer:** Executors run out of memory

Mind table sizes.

**Q107.** What does autoBroadcastJoinThreshold default to?

**Answer:** 10 MB

Tune carefully.

**Q108.** When avoid broadcast?

**Answer:** When the 'small' side grows or has skew in size

Monitor.

### [08 · Monitoring Spark UI](level-3/08-monitoring-spark-ui.md)

**Q109.** What does the Spark UI Stages tab show?

**Answer:** Tasks, shuffle read/write, spill and duration per stage

Diagnose.

**Q110.** What does the SQL tab show?

**Answer:** The query plan with metrics per operator

Find slow nodes.

**Q111.** What's a sign of GC trouble?

**Answer:** High GC time relative to task time in the Executors tab

Memory pressure.

**Q112.** Where are job-level event logs stored?

**Answer:** The history server reads event logs for finished applications

Post-mortems.

### [09 · Checkpointing Fault Tolerance](level-3/09-checkpointing-fault-tolerance.md)

**Q113.** What does checkpointing do in streaming?

**Answer:** Saves progress and state so the query resumes after failure

checkpointLocation.

**Q114.** What is an RDD lineage?

**Answer:** The recorded chain of transformations used to recompute lost partitions

Fault tolerance.

**Q115.** When use RDD checkpoint?

**Answer:** To truncate long lineages in iterative jobs

Avoid stack overflows.

**Q116.** What do exactly-once sinks need?

**Answer:** Idempotent writes or transactional sinks plus checkpoints

Delivery guarantees.

### [10 · Capstone Tuned Batch Pipeline](level-3/10-capstone-tuned-batch-pipeline.md)

**Q117.** What does the tuned batch pipeline project combine?

**Answer:** Profiling, partitioning, joins, skew handling and AQE tuning with before/after measurements

Level 3 capstone.

**Q118.** How prove a tuning worked?

**Answer:** Compare runtime, shuffle and spill metrics before and after on the same data

Evidence.

**Q119.** What do you change first?

**Answer:** The biggest bottleneck identified by the UI

Pareto.

**Q120.** What do you document?

**Answer:** Findings, settings and the reasoning

Repeatable.

## Level 4 · Master

### [01 · Delta Lake Lakehouse Patterns](level-4/01-delta-lake-lakehouse-patterns.md)

**Q121.** What does Delta Lake add on top of Parquet?

**Answer:** ACID transactions, time travel, schema enforcement and MERGE

Lakehouse.

**Q122.** What does MERGE do?

**Answer:** Upserts: update matches and insert new rows atomically

Idempotent loads.

**Q123.** What does time travel enable?

**Answer:** Querying or restoring earlier versions of a table

Audit and rollback.

**Q124.** Why use Delta over plain Parquet files for a table that is updated?

**Answer:** Atomic commits and a transaction log prevent partial writes and enable MERGE and rollbacks

Reliability.

### [02 · Orchestration Airflow Integration](level-4/02-orchestration-airflow-integration.md)

**Q125.** What does Airflow do for Spark jobs?

**Answer:** Schedules and orchestrates them with dependencies, retries and alerts

Workflow engine.

**Q126.** Which operator submits Spark jobs?

**Answer:** SparkSubmitOperator (or provider-specific operators)

Orchestration.

**Q127.** What should tasks be?

**Answer:** Idempotent and parameterized by run date

Safe retries.

**Q128.** Where do business logic and orchestration live?

**Answer:** Business logic in Spark code; scheduling in the orchestrator

Separation.

### [03 · Cost Performance Tradeoffs](level-4/03-cost-performance-tradeoffs.md)

**Q129.** How balance cost and performance?

**Answer:** Right-size clusters, use spot capacity and autoscaling, and optimize hot jobs

Cost awareness.

**Q130.** What is a cheap big win?

**Answer:** Columnar formats, partition pruning and filtering early

Read less.

**Q131.** When does adding nodes not help?

**Answer:** When the bottleneck is skew or a single partition

Fix design first.

**Q132.** What should you track?

**Answer:** Cost per job/run and per business output

FinOps.

### [04 · Debugging Failed Spark Jobs](level-4/04-debugging-failed-spark-jobs.md)

**Q133.** Where start debugging a failed job?

**Answer:** The failed stage's task error and executor logs

Root cause.

**Q134.** What does an OOM error usually mean?

**Answer:** A partition or broadcast is too big for executor memory

Repartition or increase memory.

**Q135.** What does 'FetchFailed' suggest?

**Answer:** Executor/node loss or shuffle data unavailable

Check cluster health.

**Q136.** How reproduce a failure?

**Answer:** Run on a smaller sample with the same code path and settings

Isolate.

### [05 · Schema Evolution Production](level-4/05-schema-evolution-production.md)

**Q137.** What does this print?

```python
df1 = spark.createDataFrame([(1, "a")], ["id", "v"])
df2 = spark.createDataFrame([(2, "b", 1.5)], ["id", "v", "w"])
u = df1.unionByName(df2, allowMissingColumns=True)
print(u.columns, [tuple(r) for r in u.orderBy("id").collect()])
```

**Answer:** ['id', 'v', 'w'] [(1, 'a', None), (2, 'b', 1.5)]

Schema evolution by name. Verified by running it on PySpark 3.5 (local mode).

**Q138.** What is a safe way to add a column in production?

**Answer:** Add it as nullable with a default and update downstream consumers

Backward compatibility.

**Q139.** What breaks consumers?

**Answer:** Removing or retyping columns without notice

Contracts.

**Q140.** What does mergeSchema do on Parquet/Delta writes?

**Answer:** Allows new columns to be added to the table schema

Controlled evolution.

### [06 · Multi Tenant Cluster Strategy](level-4/06-multi-tenant-cluster-strategy.md)

**Q141.** What isolates tenants on a shared cluster?

**Answer:** Queues/pools, quotas and per-team permissions

Fair scheduling.

**Q142.** What do dynamic allocation settings do?

**Answer:** Scale executors up and down with load

Efficiency.

**Q143.** How prevent noisy neighbors?

**Answer:** Resource limits and cost attribution

Governance.

**Q144.** When use separate clusters?

**Answer:** For strict isolation or very different workloads

Trade-offs.

### [07 · Data Quality Gates](level-4/07-data-quality-gates.md)

**Q145.** What does this print?

```python
df = spark.createDataFrame([(1, "a@x.com", 10.0), (2, None, -5.0), (2, "bad", 7.0), (3, "c@x.com", None)], ["id", "email", "amt"])
checks = {
    "id_unique": df.select("id").distinct().count() == df.count(),
    "email_not_null": df.filter(F.col("email").isNull()).count() == 0,
    "email_format": df.filter(~F.col("email").rlike("^[^@]+@[^@]+$")).count() == 0,
    "amt_non_negative": df.filter(F.col("amt") < 0).count() == 0,
}
print({k: v for k, v in checks.items()})
print("FAIL" if not all(checks.values()) else "PASS")
```

**Answer:** {'id_unique': False, 'email_not_null': False, 'email_format': False, 'amt_non_negative': False}
FAIL

Rows with null email are excluded from the rlike filter (null predicate), so only the 'bad' value is caught; every check fails here. Verified by running it on PySpark 3.5 (local mode).

**Q146.** Where should quality gates sit?

**Answer:** Between pipeline stages, failing or quarantining before bad data propagates

Fail fast.

**Q147.** What are common checks?

**Answer:** Uniqueness, nulls, ranges, referential integrity and freshness

Data tests.

**Q148.** What do you do with failing rows?

**Answer:** Quarantine with reasons and alert owners

Traceability.

### [08 · CI/CD for Spark Jobs](level-4/08-cicd-for-spark-jobs.md)

**Q149.** What should CI run for Spark jobs?

**Answer:** Unit tests on small local DataFrames, linting and integration tests

Local Spark.

**Q150.** How test transformation functions?

**Answer:** Write them as pure DataFrame-to-DataFrame functions and assert on small inputs

Testable design.

**Q151.** How deploy?

**Answer:** Package the code, version it and promote through environments

CI/CD.

**Q152.** What config differs by environment?

**Answer:** Paths, cluster size and credentials via parameters, not code

12-factor.

### [09 · Career Growth Big Data](level-4/09-career-growth-big-data.md)

**Q153.** What skills grow a big data career?

**Answer:** Strong fundamentals, SQL, distributed systems, performance and communication

Durable skills.

**Q154.** What does a senior data engineer do?

**Answer:** Designs reliable, cost-aware pipelines and mentors others

Impact.

**Q155.** How learn faster?

**Answer:** Build projects, read plans and the Spark UI, and review others' code

Practice.

**Q156.** What differentiates you?

**Answer:** Owning outcomes: quality, cost and reliability

Value.

### [10 · Capstone Production Lakehouse](level-4/10-capstone-production-lakehouse.md)

**Q157.** What does the lakehouse capstone combine?

**Answer:** Ingestion, Delta tables, quality gates, orchestration and CI

Level 4 capstone.

**Q158.** How structure layers?

**Answer:** Bronze (raw), silver (clean), gold (business-ready)

Medallion.

**Q159.** How ensure reliability?

**Answer:** Idempotent jobs, checkpoints, alerts and runbooks

Production.

**Q160.** How prove value?

**Answer:** Freshness, quality and cost metrics tied to business outputs

Outcomes.

## Mixed questions across all levels

**Q161.** What does this print?

```python
df = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["id", "v"])
print(df.count(), df.distinct().count(), df.dropDuplicates(["id"]).count())
```

**Answer:** 3 2 2

distinct vs dropDuplicates. Verified by running it on PySpark 3.5 (local mode).

**Q162.** What does this print?

```python
df = spark.createDataFrame([("a", 1), ("a", None), ("b", 2)], ["k", "v"])
print(df.agg(F.count("*"), F.count("v"), F.sum("v")).collect()[0])
```

**Answer:** Row(count(1)=3, count(v)=2, sum(v)=3)

count(col) skips nulls. Verified by running it on PySpark 3.5 (local mode).

**Q163.** What does this print?

```python
df = spark.createDataFrame([("x", 5), ("x", 7), ("y", 1)], ["k", "v"])
w = Window.partitionBy("k").orderBy(F.desc("v"))
print([tuple(r) for r in df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").orderBy("k").collect()])
```

**Answer:** [('x', 7, 1), ('y', 1, 1)]

Top-1 per group with row_number. Verified by running it on PySpark 3.5 (local mode).

**Q164.** What does this print?

```python
a = spark.createDataFrame([(1, "x"), (2, "y"), (3, "z")], ["id", "v"])
b = spark.createDataFrame([(1,), (3,)], ["id"])
print(a.join(b, "id", "left_semi").count(), a.join(b, "id", "left_anti").collect())
```

**Answer:** 2 [Row(id=2, v='y')]

Semi and anti joins filter without adding columns. Verified by running it on PySpark 3.5 (local mode).

**Q165.** What does this print?

```python
df = spark.createDataFrame([("2026-03-05",), ("bad",)], ["d"])
print([r.m for r in df.select(F.month(F.to_date("d")).alias("m")).collect()])
```

**Answer:** [3, None]

Unparseable dates become null. Verified by running it on PySpark 3.5 (local mode).

**Q166.** What does this print?

```python
df = spark.range(10)
print(df.repartition(5).rdd.getNumPartitions(), df.coalesce(1).rdd.getNumPartitions())
```

**Answer:** 5 1

repartition vs coalesce. Verified by running it on PySpark 3.5 (local mode).

**Q167.** A groupBy job has one task taking 20 minutes while the rest finish in seconds. What's the likely cause?

**Answer:** Data skew on a hot key

Check the stage's task distribution.

**Q168.** When should you broadcast a table?

**Answer:** When it is small enough to fit in executor memory, avoiding a shuffle of the large side

Tune the threshold.

**Q169.** Why define a schema instead of inferSchema in production?

**Answer:** It avoids an extra pass and prevents surprise types

Stable contracts.

**Q170.** What makes a write idempotent?

**Answer:** Overwriting a partition or using MERGE so a retry doesn't duplicate rows

Safe retries.

**Q171.** What causes many tiny output files?

**Answer:** Too many partitions at write time; coalesce/repartition first

Small file problem.

**Q172.** A job OOMs on a join. First checks?

**Answer:** Broadcast size, partition counts and skew in the Spark UI

Diagnose before scaling.

**Q173.** What does AQE do?

**Answer:** Uses runtime statistics to coalesce partitions, change join strategy and split skewed partitions

Adaptive.

**Q174.** What does a watermark do in streaming?

**Answer:** Bounds how late data may arrive so old state can be dropped

Event time.

**Q175.** Why make transformations pure functions of DataFrames?

**Answer:** They're easy to unit test on small local data

Testability.

**Q176.** Where do failing records go in a robust pipeline?

**Answer:** A quarantine table with the reason, plus an alert

Data quality.

**Q177.** What does Delta Lake add to a data lake?

**Answer:** ACID transactions, schema enforcement and time travel

Lakehouse.

**Q178.** What is the right way to compare two tuning attempts?

**Answer:** Run both on the same data and compare runtime, shuffle and spill metrics

Evidence.

**Q179.** What does dynamic partition overwrite protect?

**Answer:** Only the partitions in the new data are replaced

Incremental loads.

## Practical coding questions

### CSV to Parquet ETL with quarantine and reconciliation

Read the raw CSV with an explicit all-string schema, cast amount and date, quarantine rows that fail with a reason, de-duplicate by order_id, reconcile counts, report monthly revenue and the largest order per country, then write country-partitioned Parquet and read it back.

Starter code:

```
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
import os, tempfile

# 1. Raw CSV with a few bad rows.
raw = """order_id,customer,country,amount,order_date
1,alice,US,120.50,2026-01-05
2,bob,IN,75.00,2026-01-06
2,bob,IN,75.00,2026-01-06
3,carla,DE,abc,2026-01-07
4,dan,US,300.00,2026-02-01
5,erin,IN,,2026-02-02
6,fay,US,45.25,2026-02-03
7,gus,DE,210.00,not-a-date
8,hana,IN,99.99,2026-02-10
"""
work = tempfile.mkdtemp()
open(work + "/orders.csv", "w").write(raw)

schema = StructType([StructField(n, StringType()) for n in ["order_id", "customer", "country", "amount", "order_date"]])
df = spark.read.csv(work + "/orders.csv", header=True, schema=schema)

# 2. TODO: cast amount to decimal(10,2) and order_date to a date (yyyy-MM-dd); add a `reason` column
#    ("bad_amount" when the amount does not parse, else "bad_date" when the date does not parse, else null)
#    good = rows without a reason, de-duplicated by order_id; bad = rows with a reason.

# 3. TODO: print  input=<n> good=<n> quarantined=<n> duplicates_removed=<n>  and whether the counts reconcile,
#    then the sorted list of (order_id, reason) for the quarantined rows.

# 4. TODO: monthly orders and revenue per country (print "<country> <yyyy-MM> orders=<n> revenue=<amount>"),
#    then the largest order per country using a window (row_number) -> list of (country, customer, amount).

# 5. TODO: write partitioned Parquet (by country), print the partition folders and whether a read-back row count matches.
```

Expected output:

```text
input=9 good=5 quarantined=3 duplicates_removed=1
reconciled: True
quarantine: [('3', 'bad_amount'), ('5', 'bad_amount'), ('7', 'bad_date')]
IN 2026-01 orders=1 revenue=75.00
IN 2026-02 orders=1 revenue=99.99
US 2026-01 orders=1 revenue=120.50
US 2026-02 orders=2 revenue=345.25
top order per country: [('IN', 'hana', '99.99'), ('US', 'dan', '300.00')]
partitions: ['country=IN', 'country=US']
round trip: True
```

??? success "Show a solution"

    ```
    from pyspark.sql import SparkSession, functions as F, Window
    from pyspark.sql.types import *
    spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import os, tempfile

    # 1. Raw CSV with a few bad rows.
    raw = """order_id,customer,country,amount,order_date
    1,alice,US,120.50,2026-01-05
    2,bob,IN,75.00,2026-01-06
    2,bob,IN,75.00,2026-01-06
    3,carla,DE,abc,2026-01-07
    4,dan,US,300.00,2026-02-01
    5,erin,IN,,2026-02-02
    6,fay,US,45.25,2026-02-03
    7,gus,DE,210.00,not-a-date
    8,hana,IN,99.99,2026-02-10
    """
    work = tempfile.mkdtemp()
    open(work + "/orders.csv", "w").write(raw)

    schema = StructType([StructField(n, StringType()) for n in ["order_id", "customer", "country", "amount", "order_date"]])
    df = spark.read.csv(work + "/orders.csv", header=True, schema=schema)

    # 2. Parse, then split valid and quarantined rows (with a reason).
    parsed = (df.withColumn("amount_d", F.col("amount").cast("decimal(10,2)"))
                .withColumn("date_d", F.to_date("order_date", "yyyy-MM-dd")))
    reason = (F.when(F.col("amount_d").isNull(), "bad_amount").when(F.col("date_d").isNull(), "bad_date"))
    checked = parsed.withColumn("reason", reason)
    good = checked.filter(F.col("reason").isNull()).dropDuplicates(["order_id"])
    bad = checked.filter(F.col("reason").isNotNull())

    # 3. Reconcile counts.
    total_in, n_good, n_bad = df.count(), good.count(), bad.count()
    dupes = checked.filter(F.col("reason").isNull()).count() - n_good
    print(f"input={total_in} good={n_good} quarantined={n_bad} duplicates_removed={dupes}")
    print("reconciled:", total_in == n_good + n_bad + dupes)
    print("quarantine:", sorted((r.order_id, r.reason) for r in bad.collect()))

    # 4. Monthly revenue by country with the top customer per country (window function).
    clean = good.select("order_id", "customer", "country", F.col("amount_d").alias("amount"), F.date_format("date_d", "yyyy-MM").alias("month"))
    rev = clean.groupBy("country", "month").agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue")).orderBy("country", "month")
    for r in rev.collect(): print(f"{r.country} {r.month} orders={r.orders} revenue={r.revenue}")
    w = Window.partitionBy("country").orderBy(F.desc("amount"))
    top = clean.withColumn("rn", F.row_number().over(w)).filter("rn = 1").orderBy("country")
    print("top order per country:", [(r.country, r.customer, str(r.amount)) for r in top.collect()])

    # 5. Write partitioned Parquet and read it back.
    out = work + "/out"
    clean.write.partitionBy("country").mode("overwrite").parquet(out)
    print("partitions:", sorted(p for p in os.listdir(out) if p.startswith("country=")))
    print("round trip:", spark.read.parquet(out).count() == n_good)
    ```

    Parsing into typed columns turns bad values into nulls so rows can be quarantined with a reason, and the counts must add up (input = good + quarantined + duplicates). Verified by running the solution on PySpark 3.5 in local mode.

### Join strategy, skew salting and a quality gate

Show that a broadcast hint changes the physical join without changing the result, find the hottest key, prove that two-stage salted aggregation equals the direct aggregation, and write a small quality gate that stops the pipeline when checks fail.

Starter code:

```
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
fact = spark.createDataFrame([("hot", i) for i in range(60)] + [("k%d" % i, i) for i in range(10)], ["key", "v"])
dim = spark.createDataFrame([("hot", "H"), ("k0", "A"), ("k1", "B"), ("k2", "C")], ["key", "label"])

def plan_has(df, text):
    return text in df._jdf.queryExecution().executedPlan().toString()

# TODO 1: disable automatic broadcast (spark.sql.autoBroadcastJoinThreshold = -1), left-join fact with dim, and print
#         "auto broadcast off -> SortMergeJoin" (check the plan); then repeat with F.broadcast(dim) and print
#         "with broadcast hint -> BroadcastHashJoin"; print whether both give the same rows and the row count.
# TODO 2: find the hottest key and its share of the rows ("hottest key: <key> with <pct>% of rows").
# TODO 3: salt (v % 4), aggregate in two stages (sum and count per key+salt, then per key), compare with the direct
#         aggregation, and print the number of partial groups for the hot key and its total.
# TODO 4: a gate(df, name, cond) helper that counts violating rows; run four checks (v not null, v >= 0,
#         dimension matched, dim key unique), print PASS / FAIL(n) for each and finally STOP or CONTINUE.
```

Expected output:

```text
auto broadcast off -> SortMergeJoin
with broadcast hint -> BroadcastHashJoin
same result: True | rows: 70
hottest key: hot with 86% of rows
salted == direct: True
hot key partial groups: 4
hot total: 1770
v not null           PASS
v >= 0               PASS
dimension matched    FAIL(7)
dim key unique       PASS
pipeline: STOP
```

??? success "Show a solution"

    ```
    from pyspark.sql import SparkSession, functions as F, Window
    from pyspark.sql.types import *
    spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # Join strategy, data skew with salting, and a data-quality gate.
    fact = spark.createDataFrame([("hot", i) for i in range(60)] + [("k%d" % i, i) for i in range(10)], ["key", "v"])
    dim = spark.createDataFrame([("hot", "H"), ("k0", "A"), ("k1", "B"), ("k2", "C")], ["key", "label"])

    def plan_has(df, text):
        return text in df._jdf.queryExecution().executedPlan().toString()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    smj = fact.join(dim, "key", "left")
    print("auto broadcast off ->", "SortMergeJoin" if plan_has(smj, "SortMergeJoin") else "other")
    bhj = fact.join(F.broadcast(dim), "key", "left")
    print("with broadcast hint ->", "BroadcastHashJoin" if plan_has(bhj, "BroadcastHashJoin") else "other")
    print("same result:", sorted(map(tuple, smj.collect())) == sorted(map(tuple, bhj.collect())), "| rows:", smj.count())

    # Skew: one key holds most of the rows.
    counts = fact.groupBy("key").count().orderBy(F.desc("count")).collect()
    share = counts[0]["count"] / fact.count()
    print(f"hottest key: {counts[0]['key']} with {share:.0%} of rows")

    # Two-stage aggregation with salting gives the same answer while spreading the hot key.
    N = 4
    salted = fact.withColumn("salt", (F.col("v") % N))
    partial = salted.groupBy("key", "salt").agg(F.sum("v").alias("s"), F.count("*").alias("c"))
    final = partial.groupBy("key").agg(F.sum("s").alias("total"), F.sum("c").alias("n")).orderBy("key")
    direct = fact.groupBy("key").agg(F.sum("v").alias("total"), F.count("*").alias("n")).orderBy("key")
    print("salted == direct:", final.collect() == direct.collect())
    print("hot key partial groups:", partial.filter("key = 'hot'").count())
    print("hot total:", final.filter("key = 'hot'").collect()[0]["total"])

    # Data-quality gate.
    def gate(df, name, cond):
        bad = df.filter(~cond).count()
        return (name, "PASS" if bad == 0 else f"FAIL({bad})")
    joined = smj
    checks = [
        gate(fact, "v not null", F.col("v").isNotNull()),
        gate(fact, "v >= 0", F.col("v") >= 0),
        gate(joined, "dimension matched", F.col("label").isNotNull()),
        gate(dim, "dim key unique", F.lit(dim.select("key").distinct().count() == dim.count())),
    ]
    for name, res in checks: print(f"{name:20s} {res}")
    print("pipeline:", "STOP" if any(r.startswith("FAIL") for _, r in checks) else "CONTINUE")
    ```

    Plans show whether Spark shuffles or broadcasts; salting spreads a hot key across partitions and a second aggregation restores the exact answer; gates stop bad data early. Verified by running the solution on PySpark 3.5 in local mode (small data, so timings would not show the real benefit).

---

More free practice: the [PySpark Mastery Path](index.md) curriculum, the [Interview Prep Pack](https://learnmasterypath.com/interview-pack/) and the [free resume ATS check](https://learnmasterypath.com/resume/).
