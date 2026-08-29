# 03 · SparkSession & Your First PySpark Script

## Installing PySpark

```bash
pip install pyspark
```

This installs the Python bindings *and* bundles the Spark JVM libraries. You
also need a JDK on your machine (Java 11 or 17 are the safest choices for
recent Spark versions) — check with:

```bash
java -version
```

If that fails, install a JDK (e.g. via `brew install openjdk@17` on macOS, or
your OS's package manager) before continuing.

!!! note "Not executed against a live cluster in this environment"
    The code in this module was written and carefully traced through by hand
    for correctness, but this authoring environment does not have a JVM/Spark
    runtime available to actually execute it. Everything shown uses
    documented, standard PySpark APIs and should run as-is once you have
    `pyspark` and a JDK installed locally.

## SparkSession: the entry point

Everything in modern PySpark starts from a `SparkSession` — it's your handle
to the driver, and every DataFrame, every read, every SQL query goes through
it.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FirstPySparkScript")
    .master("local[*]")   # run locally, using all available CPU cores
    .getOrCreate()
)

print(spark.version)
```

Key parts of that builder call:

- `.appName(...)` — a human-readable name that shows up in logs and the
  Spark UI. Purely cosmetic, but useful once you're debugging multiple jobs.
- `.master("local[*]")` — tells Spark where to run. `local[*]` means "run
  in this one process, using all available CPU cores as if they were
  executors." Use `local[4]` to cap it at 4 cores, or a cluster URL like
  `spark://host:7077` / `yarn` / `k8s://...` when you have a real cluster.
  Every example in this level uses `local[*]`.
- `.getOrCreate()` — reuses an existing SparkSession in the same process if
  one already exists (useful in notebooks), otherwise creates a new one.

`SparkSession` is actually a unified entry point that wraps what used to be
several separate objects in older Spark versions (`SparkContext`,
`SQLContext`, `HiveContext`) — you'll see `spark.sparkContext` referenced
occasionally for lower-level RDD operations, covered in Module 4.

## Your first script, end to end

Let's create a tiny in-memory DataFrame (no file needed yet — that's Module
5) and run a couple of operations on it, to see the lazy/eager split from
Module 2 in practice.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FirstPySparkScript")
    .master("local[*]")
    .getOrCreate()
)

# Create a small DataFrame from plain Python data — useful for
# experimenting without a file.
data = [
    ("Alice", "Sales", 72000),
    ("Bob", "Engineering", 95000),
    ("Carla", "Engineering", 105000),
    ("Deepak", "Sales", 68000),
]
columns = ["name", "department", "salary"]

df = spark.createDataFrame(data, columns)   # transformation — nothing runs yet

df.printSchema()   # this is an action-like call: it inspects the schema,
                    # which for createDataFrame is already known, so it's cheap

# root
#  |-- name: string (nullable = true)
#  |-- department: string (nullable = true)
#  |-- salary: long (nullable = true)

df.show()           # ACTION — triggers execution and prints the result

# +------+-----------+------+
# |  name| department|salary|
# +------+-----------+------+
# | Alice|      Sales| 72000|
# |   Bob|Engineering| 95000|
# | Carla|Engineering|105000|
# |Deepak|      Sales| 68000|
# +------+-----------+------+

high_earners = df.filter(df.salary > 70000)   # transformation
print(high_earners.count())                    # ACTION — prints 3

spark.stop()   # always stop the session when your script is done,
                # to release cluster resources cleanly
```

A few details worth calling out:

- `createDataFrame` infers a schema from the Python data you pass — `"long"`
  for the salary column because the values are Python `int`s. Module 7 covers
  specifying schemas explicitly, which you'll want for real files.
- `.show()` defaults to printing 20 rows, truncating long strings. Pass
  `df.show(n=50, truncate=False)` to see more, untruncated.
- `spark.stop()` is good hygiene, especially in local mode where forgetting
  it can leave lingering processes; it's essential in cluster mode where it
  releases real, shared resources back to the cluster manager.

## Running it as a script

Save the code above as `first_script.py` and run it with:

```bash
python first_script.py
```

or, using Spark's own launcher (recommended for anything beyond local
experimentation, since `spark-submit` handles packaging and cluster
deployment options `python` alone doesn't):

```bash
spark-submit first_script.py
```

For everything in this level, plain `python first_script.py` is enough since
we're in local mode.

## Worked example: configuring the session

You can pass Spark configuration properties through the builder too — a
common one for local development is capping how much memory the single
local-mode JVM process is allowed to use:

```python
spark = (
    SparkSession.builder
    .appName("ConfiguredExample")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")   # default is 200; too many
                                                     # for small local datasets
    .getOrCreate()
)
```

`spark.sql.shuffle.partitions` controls how many partitions Spark uses after
a shuffle (e.g. after a `groupBy`). The default of 200 is tuned for cluster
workloads; on a laptop with a small dataset, 200 tiny partitions creates more
scheduling overhead than benefit, so it's common to lower it for local
development and testing. You'll tune this for real in Level 3.

## Exercise

Write a script (don't need to run it if you don't have Spark installed yet,
but reason through what it should print) that:

1. Creates a SparkSession named `"ExerciseSession"` running in local mode
   with 2 cores (`local[2]`).
2. Builds a DataFrame from this data: three rows of `(product, price)` —
   `("Laptop", 1200)`, `("Mouse", 25)`, `("Keyboard", 75)`.
3. Prints the schema, then filters to rows where `price > 50`, then prints
   the count of the filtered result.
4. Stops the session.

Expected reasoning: the schema should show `product: string` and
`price: long`; the filter keeps `"Laptop"` (1200) and `"Keyboard"` (75), so
the final count should print `2`.
