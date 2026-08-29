# 04 · RDDs vs. DataFrames

!!! note "Not executed against a live cluster in this environment"
    As in Module 3, code here is carefully hand-traced against documented
    PySpark behavior, not run against a live Spark cluster in this authoring
    environment.

## RDDs: Spark's original abstraction

An **RDD** (Resilient Distributed Dataset) is Spark's lowest-level data
abstraction: a distributed collection of arbitrary Python (or Java/Scala)
objects, split into partitions across the cluster, with no built-in notion of
columns, schema, or types beyond "whatever object is in each element."

```python
sc = spark.sparkContext   # the lower-level context, reachable from SparkSession

rdd = sc.parallelize([
    ("Alice", "Sales", 72000),
    ("Bob", "Engineering", 95000),
    ("Carla", "Engineering", 105000),
])

# RDD transformations look like functional-programming primitives:
high_earners = rdd.filter(lambda row: row[2] > 80000)
names = high_earners.map(lambda row: row[0])

print(names.collect())   # ACTION — ['Bob', 'Carla']
```

Notice: you access fields by *position* (`row[2]`), there's no schema, and
the "transformation" logic is an arbitrary Python lambda that Spark has no
way to inspect or optimize — it can only run it as a black box, once per row,
per partition.

## DataFrames: structured, optimizable, faster

A **DataFrame** is a distributed collection of rows with a **named, typed
schema** — conceptually a distributed spreadsheet or SQL table. The same
logic as above, expressed as a DataFrame:

```python
df = spark.createDataFrame(
    [("Alice", "Sales", 72000), ("Bob", "Engineering", 95000), ("Carla", "Engineering", 105000)],
    ["name", "department", "salary"],
)

result = df.filter(df.salary > 80000).select("name")
print([row.name for row in result.collect()])   # ['Bob', 'Carla']
```

The DataFrame version accesses fields by *name* (`df.salary`, more readable
and less error-prone), and — critically — `filter(df.salary > 80000)` is
built from Spark's own expression objects, not an opaque Python lambda. That
means Spark's **Catalyst optimizer** can inspect the expression, rewrite it,
push it down into the data source, and generate efficient JVM bytecode for
it via **Tungsten** (Spark's off-heap, binary memory format). RDDs, by
running arbitrary Python callables, forfeit almost all of that — every row
has to be deserialized from Spark's internal format into a Python object,
passed to your lambda, and the result reserialized, which is comparatively
slow.

## Side-by-side comparison

| | RDD | DataFrame |
|---|---|---|
| Structure | Arbitrary objects, no schema | Named columns, typed schema |
| API style | Functional (`map`, `filter`, `reduce`) with raw lambdas | Declarative, SQL-like (`select`, `filter`, `groupBy`) |
| Optimization | None — Spark runs your code as a black box | Catalyst optimizer rewrites and optimizes the whole plan |
| Performance | Slower, especially in Python (serialization overhead per row) | Fast — most work happens inside the JVM via Tungsten, regardless of Python/Scala |
| When to use | Rare: unstructured data, custom partitioning logic, or algorithms that don't fit the relational model | The default choice for the overwhelming majority of PySpark work, including all of this course |

## Why DataFrames dominate in practice

For nearly all real workloads — ETL, analytics, joins, aggregations — the
DataFrame API is both easier to write and faster to run than the equivalent
RDD code, *especially* in PySpark specifically. When you write RDD
transformations with Python lambdas, every element has to cross the
Python/JVM boundary: Spark's JVM process serializes each row, launches a
Python process to run your lambda, then deserializes the result back. This
round-trip is a real, measurable cost. DataFrame operations, in contrast,
compile down to JVM bytecode that runs natively — your Python code just
*describes* the operation once, at plan-building time, not once per row.

This is why the official Spark guidance — and this entire course — treats
DataFrames as the default, and RDDs as an escape hatch for the rare cases
that genuinely need unstructured, arbitrary per-record logic that can't be
expressed relationally.

## Converting between them

You can move between the two when you truly need to:

```python
# DataFrame -> RDD (of Row objects)
rdd_from_df = df.rdd
print(rdd_from_df.first())   # Row(name='Alice', department='Sales', salary=72000)

# RDD -> DataFrame (needs column names or a schema)
df_from_rdd = rdd.toDF(["name", "department", "salary"])
df_from_rdd.show()
```

## Worked example: same logic, two ways

Task: given employee data, find the average salary per department.

**RDD approach** (functional, manual aggregation):

```python
rdd = sc.parallelize([
    ("Alice", "Sales", 72000),
    ("Bob", "Engineering", 95000),
    ("Carla", "Engineering", 105000),
    ("Deepak", "Sales", 68000),
])

pairs = rdd.map(lambda row: (row[1], (row[2], 1)))   # (dept, (salary, count))
summed = pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
averages = summed.mapValues(lambda v: v[0] / v[1])

print(averages.collect())
# [('Sales', 70000.0), ('Engineering', 100000.0)]
```

**DataFrame approach** (declarative, one line of intent):

```python
from pyspark.sql.functions import avg

df.groupBy("department").agg(avg("salary").alias("avg_salary")).show()

# +-----------+----------+
# | department|avg_salary|
# +-----------+----------+
# |      Sales|   70000.0|
# |Engineering|  100000.0|
# +-----------+----------+
```

Both produce the same answer. The DataFrame version is shorter, reads closer
to the intent ("average salary, grouped by department"), and lets Spark
choose the actual execution strategy — the RDD version hard-codes one
specific strategy (`reduceByKey`) that you had to design yourself.

## Exercise

Given this RDD-style data:

```python
rdd = sc.parallelize([
    ("p1", "electronics", 250),
    ("p2", "books", 15),
    ("p3", "electronics", 800),
    ("p4", "books", 22),
])
```

1. Rewrite this as a DataFrame with columns `product_id`, `category`, `price`.
2. Using the DataFrame API, find the total (`sum`) price per category.
3. Explain, in your own words, why the DataFrame version of step 2 lets
   Spark's optimizer do more than the equivalent RDD `reduceByKey` would.

Expected DataFrame code for step 2:

```python
from pyspark.sql.functions import sum as spark_sum

df.groupBy("category").agg(spark_sum("price").alias("total_price")).show()
# electronics -> 1050, books -> 37
```
